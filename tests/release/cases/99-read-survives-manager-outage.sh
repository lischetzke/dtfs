#!/usr/bin/env bash
# WI-09: a FUSE read must survive a manager outage without stalling. Pre-fix,
# read()'s cache-miss branch awaited the migration-trigger RPC
# (MigrationTrigger::on_remote_read -> DtfsClient::allocate_chunks) INLINE,
# AFTER the chunk bytes were already fetched, while holding a
# known_locations DashMap Ref guard for the whole call. That RPC sets no
# per-call deadline and retries Unavailable by walking the manager pool for
# up to MANAGER_WALK_DEADLINE_MS (15s) against a 5s connect / 60s request
# timeout — so a read whose bytes were already in hand stalled ~15s PER
# CHUNK during a manager outage/election. The fix
# (crates/dtfs-fuse/src/migration.rs MigrationTrigger::spawn_on_remote_read)
# fires that RPC in the background instead of awaiting it inline, and no
# longer clears the migration cooldown entry on failure.
#
# Live repro: write a multi-chunk fixture from another DC so the reader is
# cold, warm the reader's cache with a 1 MB partial read (a real open()+read(),
# not just `stat` — the read path needs GetFileLocations from the manager,
# which is what actually populates known_locations, so this happens BEFORE
# the outage), then DROP the reader's own outbound traffic to the manager
# ports (9401 gRPC, 9402 REST) ONLY — data-plane port 9500 stays open
# throughout — and cold-read the file. The fixture is 12 MB at the 4 MB
# default chunk size, so the warm read touches only the first chunk and the
# rest stay cold, exercising the migration-hint path (not a metadata miss)
# on the post-outage read. It must complete at roughly transfer speed, not
# ~15s per chunk.
source "$(dirname "$0")/../lib/common.sh"
source "$(dirname "$0")/../lib/dc.sh"

SIZE_MB=12    # multiple chunks at the 4 MB default volume chunk size
PREFIX=/release/99-mgr-outage-$$/
SVC='dtfs-fuse@-mnt-dtfs.service'
WRITER=$(client_for_dc dc-b); [ -n "$WRITER" ] || WRITER=""   # write from the OTHER dc so the reader is cold
READER=$(client_for_dc dc-a); [ -n "$READER" ] || READER="$CLIENT"
[ -n "$READER" ] || skip "no dc-a reader client"
[ -n "$WRITER" ] && [ "$WRITER" != "$READER" ] || skip "need a second client in another DC to write cold fixtures"

DROPPED=0
case_cleanup() {
    if [ "$DROPPED" = 1 ]; then
        ssh_run "$READER" "sudo iptables -D OUTPUT -p tcp --dport 9401 -j DROP; sudo iptables -D OUTPUT -p tcp --dport 9402 -j DROP" >/dev/null 2>&1
    fi
    client_run "sudo rm -rf ${MOUNT}${PREFIX}" >/dev/null 2>&1
    ssh_run "$WRITER" "sudo rm -rf ${MOUNT}${PREFIX}" >/dev/null 2>&1
}
trap case_cleanup EXIT

echo "[1/5] prereqs: iptables on $READER"
ssh_run "$READER" "command -v iptables >/dev/null" \
    || known_issue WI-09 "iptables unavailable on $READER; cannot simulate a manager outage"

echo "[2/5] write a ${SIZE_MB}MB fixture via $WRITER so $READER is cold"
ssh_run "$WRITER" "sudo mkdir -p ${MOUNT}${PREFIX} && head -c $((SIZE_MB*1024*1024)) /dev/urandom | sudo tee ${MOUNT}${PREFIX}F.bin >/dev/null && sudo sync" \
    || fail "write failed"
ORIG=$(ssh_run "$WRITER" "sudo sha256sum ${MOUNT}${PREFIX}F.bin | cut -d' ' -f1")

echo "[3/5] warm $READER's metadata + location cache with a real partial read"
ssh_run "$READER" "sudo stat ${MOUNT}${PREFIX}F.bin >/dev/null" || fail "stat warm failed"
ssh_run "$READER" "sudo dd if=${MOUNT}${PREFIX}F.bin of=/dev/null bs=1M count=1 status=none" || fail "warm read failed"

echo "[4/5] DROP $READER's outbound manager ports 9401/9402 (data 9500 stays open)"
ssh_run "$READER" "sudo iptables -A OUTPUT -p tcp --dport 9401 -j DROP && sudo iptables -A OUTPUT -p tcp --dport 9402 -j DROP" \
    || skip "could not apply iptables DROP"
DROPPED=1

echo "[5/5] cold-read from $READER — must complete at transfer speed, not ~15s/chunk"
START=$(date +%s)
GOT=$(ssh_run "$READER" "timeout 60 sha256sum ${MOUNT}${PREFIX}F.bin 2>/dev/null | cut -d' ' -f1")
ELAPSED=$(( $(date +%s) - START ))
echo "  read elapsed=${ELAPSED}s sha=$GOT"

# Best-effort corroboration: the fire-and-forget migration RPC should still
# have attempted and failed against the blackholed manager. Informational
# only — journalctl verbosity/permissions vary, so this never fails the case.
WARNCOUNT=$(ssh_run "$READER" "sudo journalctl -u '$SVC' --since '2 min ago' 2>/dev/null | grep -c 'migration allocation request failed'" 2>/dev/null || echo "?")
echo "  'migration allocation request failed' warnings in log: $WARNCOUNT"

[ -n "$GOT" ] || fail "WI-09: read did not complete within 60s — manager RPC still blocking the read"
assert_eq "$ORIG" "$GOT" "cold-read-during-manager-outage checksum"
[ "$ELAPSED" -le 20 ] || fail "read took ${ELAPSED}s (> 20s bound) — looks like the migration-trigger RPC is still stalling the read inline (pre-fix: ~15s PER CHUNK against a blackholed manager)"
echo "  cold read during manager outage completed in ${ELAPSED}s with correct bytes"
pass

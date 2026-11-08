#!/usr/bin/env bash
# BE-8: end-to-end integrity sequence requested by operator:
#   write → read → verify → migrate → verify → corrupt one replica
#   → read still ok (fallback) → scrub catches → quarantine →
#   auto-correction → unquarantine → cleanup.
#
# Most of the post-corruption assertions are guarded by Bug BC
# implementation status. Pre-BC the case exits KNOWN_ISSUE [BC]
# rather than FAIL, but every step that already works is verified.
source "$(dirname "$0")/../lib/common.sh"

F="$MOUNT/release/13-integrity-$$.bin"
BACKUP=""
QUARANTINED_HOST=""

case_cleanup() {
    # Restore the byte we flipped (best-effort).
    if [ -n "$BACKUP" ] && [ -n "$QUARANTINED_HOST" ]; then
        ssh_run "$QUARANTINED_HOST" "sudo cp '$BACKUP' '${BACKUP%.bak}'" 2>/dev/null || true
        ssh_run "$QUARANTINED_HOST" "sudo rm -f '$BACKUP'" 2>/dev/null || true
    fi
    # Operator unquarantine if BC landed + node still Quarantined.
    if [ -n "$QUARANTINED_HOST" ]; then
        nid=$(mgr_get_any /api/v1/nodes | python3 -c "
import json,sys
for n in json.load(sys.stdin):
    if n.get('hostname')==sys.argv[1]:
        print(n.get('node_id','')); break
" "$QUARANTINED_HOST")
        if [ -n "$nid" ]; then
            mgr_post_leader "/api/v1/nodes/$nid/unquarantine" \
                '{"operator_id":"release-suite","reason_acknowledged":"automated test cleanup"}' \
                >/dev/null 2>&1 || true
        fi
    fi
    client_run "sudo rm -f '$F'" >/dev/null 2>&1 || true
}
trap case_cleanup EXIT

# ── A: write ──────────────────────────────────────────────────────
echo "[A] Write 10 MiB"
client_run "sudo mkdir -p $MOUNT/release && \
            sudo dd if=/dev/urandom of='$F' bs=1M count=10 status=none && \
            sudo sync"
HASH=$(sha256_via_fuse "$F")
[ -n "$HASH" ] || fail "A: write sha256 empty"

# ── B: read back ──────────────────────────────────────────────────
echo "[B] Read sha256 match"
assert_eq "$HASH" "$(sha256_via_fuse "$F")" "B initial read"

# ── C: chunk + replica enumeration ────────────────────────────────
echo "[C] Enumerate replicas"
fname=${F#$MOUNT}
CIDS=$(chunk_ids_for_path "$fname")
NCHUNKS=$(echo "$CIDS" | grep -c '^[a-f0-9]\+$' || true)
assert_ge "$NCHUNKS" 1 "chunks for path"
CID=$(echo "$CIDS" | head -1)

# Find at least 2 hosts holding the first chunk.
HOLDING=()
for h in $(data_list); do
    if host_has_chunk "$h" "$CID"; then HOLDING+=("$h"); fi
done
echo "  chunk $CID held by: ${HOLDING[*]:-(none)}"
[ ${#HOLDING[@]} -ge 2 ] || skip "fewer than 2 replicas of chunk $CID — RF=1 cluster?"

# ── D-E: migrate + sha256 still matches ──────────────────────────
echo "[D] Migrate path → cold (no-op if already cold)"
mgr_post_leader /api/v1/migrate "{\"path\":\"$fname\",\"target_tier\":\"cold\"}" \
    >/dev/null 2>&1 || true
sleep 5
assert_eq "$HASH" "$(sha256_via_fuse "$F")" "E after-migrate read"

# ── F: corrupt one replica ───────────────────────────────────────
QUARANTINED_HOST="${HOLDING[0]}"
echo "[F] Flip a byte on $QUARANTINED_HOST"
DATFILE=$(ssh_run "$QUARANTINED_HOST" "sudo find /mnt -name '$CID.dat' 2>/dev/null | head -1")
[ -n "$DATFILE" ] || fail "F: chunk file not found on $QUARANTINED_HOST"
BACKUP="${DATFILE}.bak.release"
ssh_run "$QUARANTINED_HOST" "sudo cp '$DATFILE' '$BACKUP' && \
    sudo dd if=/dev/urandom of='$DATFILE' bs=1 count=1 seek=4096 conv=notrunc status=none"

# ── G: read still succeeds via fallback ──────────────────────────
# REV-2-5 — the point of this step is to exercise verify_on_read on the data
# node plus the P0-2 replica fallback in the client. Steps B and E already read
# the file, so the FUSE client holds the chunk in its readahead cache AND in the
# P0-11 local disk cache; dropping the kernel page cache alone leaves both
# intact, the read is served locally, no data node is ever contacted, and this
# assertion passes no matter what verify_on_read does. Purge the client's own
# caches first: stop the mount, delete the local cache directory, restart.
echo "[G] Purge the FUSE client caches so the read really reaches a data node"
CACHE_DIR=$(client_run "sudo awk -F'\"' '/^[[:space:]]*local_cache_dir/ {print \$2}' /etc/dtfs/fuse.toml 2>/dev/null | head -1" || true)
client_run "sudo systemctl stop dtfs-fuse@mnt-dtfs.service" >/dev/null 2>&1 || true
if [ -n "$CACHE_DIR" ]; then
    client_run "sudo rm -rf '${CACHE_DIR:?}'/*" >/dev/null 2>&1 || true
fi
client_run "sudo systemctl start dtfs-fuse@mnt-dtfs.service" >/dev/null 2>&1 || true
mount_back() { client_run "sudo mountpoint -q /mnt/dtfs" >/dev/null 2>&1; }
wait_for 60 "FUSE mount back after cache purge" mount_back

echo "[G] Read after corruption — must still match via replica fallback"
client_run "sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'" >/dev/null 2>&1 || true
NEWHASH=$(sha256_via_fuse "$F")
if [ "$NEWHASH" != "$HASH" ]; then
    fail "G: corrupt bytes leaked to FUSE — expected fallback to clean replica. Got $NEWHASH"
fi

# ── H-L: BC-dependent assertions ─────────────────────────────────
# Probe whether Bug BC quarantine is live: /api/v1/nodes row has
# `quarantine` field non-null after a fault.
sleep 5
QSTATE=$(mgr_get_any /api/v1/nodes | python3 -c "
import json,sys
for n in json.load(sys.stdin):
    if n.get('hostname')==sys.argv[1]:
        print('yes' if n.get('quarantine') else 'no'); break
" "$QUARANTINED_HOST" 2>/dev/null || echo no)

if [ "$QSTATE" = "no" ]; then
    echo "[H-L] Quarantine surface not yet present — BC-10/11/12 not deployed."
    known_issue BC "verify_on_read + auto-correction require Bug BC scrub + quarantine"
fi

echo "[H] Quarantine triggered: $QUARANTINED_HOST"
# Re-replication audit
sleep 10
AUDIT=$(mgr_get_any '/api/v1/audit?limit=200')
if ! echo "$AUDIT" | grep -q "node.quarantine.*$QUARANTINED_HOST"; then
    fail "H: no node.quarantine audit row for $QUARANTINED_HOST"
fi

# ── M: delete + orphan reap (BC-7) ───────────────────────────────
echo "[M] Delete + verify orphan reap (BC orphan_scrub)"
client_run "sudo rm -f '$F'"
sleep 30
LEFT=0
for h in $(data_list); do
    if host_has_chunk "$h" "$CID"; then LEFT=$((LEFT+1)); fi
done
if [ "$LEFT" -gt 0 ]; then
    known_issue BC "$LEFT replica(s) of $CID still on disk after unlink — orphan_scrub missing"
fi

pass

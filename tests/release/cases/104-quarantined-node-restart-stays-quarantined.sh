#!/usr/bin/env bash
# CR-2-1 (RAFT-1) — RegisterNode must carry forward sticky Quarantined/
# Maintenance/Draining node state instead of wiping it back to Healthy.
#
# Pre-fix, RegisterNode's sole producer (metadata.rs) always builds
# `health: Healthy, quarantine: None`, so ANY re-register (every process
# restart) silently cleared an integrity quarantine and re-admitted the
# node into read/allocate/replication pools with its corrupt disk intact.
# The unit-level regression is
# state_machine::posix_path_regression::raft_1_reregister_keeps_quarantined.
#
# Shape of the probe: corrupt one on-disk replica of a real chunk, read
# the file through FUSE so the data node's verify_on_read reports the
# mismatch (heartbeat IntegrityFault -> manager auto-quarantines,
# metadata.rs QuarantineNode). Confirm the node shows "quarantined".
# Restart the data node process (a plain RegisterNode re-registration) and
# confirm it comes back still "quarantined" rather than reverting to
# "healthy".
source "$(dirname "$0")/../lib/common.sh"

F="$MOUNT/release/104-quar-restart-$$.bin"
DATFILE=""
HOST=""
NID=""

case_cleanup() {
    if [ -n "$DATFILE" ] && [ -n "$HOST" ]; then
        ssh_run "$HOST" "sudo cp '$DATFILE.bak' '$DATFILE' && sudo rm -f '$DATFILE.bak'" 2>/dev/null || true
    fi
    if [ -n "$NID" ]; then
        mgr_post_leader "/api/v1/nodes/$NID/unquarantine" \
            '{"operator_id":"release-suite","reason_acknowledged":"automated test cleanup pass"}' \
            >/dev/null 2>&1 || true
    fi
    client_run "sudo rm -f '$F'" >/dev/null 2>&1 || true
}
trap case_cleanup EXIT

node_health() {
    mgr_get_any /api/v1/nodes | python3 -c "
import json,sys
for n in json.load(sys.stdin):
    if n.get('hostname')==sys.argv[1]:
        print(str(n.get('health','')).lower()); break
" "$1"
}

echo "[1/7] write 10 MiB and find a chunk with >=2 replicas"
client_run "sudo mkdir -p $MOUNT/release && sudo dd if=/dev/urandom of='$F' bs=1M count=10 status=none && sudo sync"
sleep 3
fname=${F#$MOUNT}
CID=$(chunk_ids_for_path "$fname" | head -1)
[ -n "$CID" ] || fail "no chunk_id"
HOLDING=()
for h in $(data_list); do
    if host_has_chunk "$h" "$CID"; then HOLDING+=("$h"); fi
done
[ ${#HOLDING[@]} -ge 2 ] || skip "fewer than 2 replicas — RF=1 cluster, can't corrupt one and still read"
HOST="${HOLDING[0]}"

echo "[2/7] resolve node_id for $HOST"
NID=$(mgr_get_any /api/v1/nodes | python3 -c "
import json,sys
for n in json.load(sys.stdin):
    if n.get('hostname')==sys.argv[1]:
        print(n.get('node_id','')); break
" "$HOST")
[ -n "$NID" ] || fail "node_id lookup failed for $HOST"
BEFORE_HEALTH=$(node_health "$HOST")
echo "  host=$HOST node_id=$NID health-before=$BEFORE_HEALTH"

echo "[3/7] corrupt that replica's .dat on disk"
DATFILE=$(ssh_run "$HOST" "sudo find /mnt -name '$CID.dat' 2>/dev/null | head -1")
[ -n "$DATFILE" ] || fail "chunk file not found on $HOST"
ssh_run "$HOST" "sudo cp '$DATFILE' '$DATFILE.bak' && sudo dd if=/dev/urandom of='$DATFILE' bs=1 count=64 seek=128 conv=notrunc status=none"

echo "[4/7] read through FUSE — verify_on_read must detect the mismatch and report it"
client_run "sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'" >/dev/null 2>&1 || true
client_run "sudo cat '$F' > /dev/null" >/dev/null 2>&1 || true

echo "[5/7] wait for auto-quarantine (heartbeat IntegrityFault -> QuarantineNode)"
is_quarantined() { [ "$(node_health "$HOST")" = "quarantined" ]; }
if ! wait_for_soft 90 is_quarantined; then
    skip "node never reached 'quarantined' within 90s (heartbeat propagation cadence, or detector variance) — cannot exercise the restart path"
fi
echo "  health after corruption+read: quarantined"

echo "[6/7] restart the data node (RegisterNode re-registration)"
ssh_run "$HOST" "sudo systemctl restart dtfs-data"
# Give it time to come back up and re-register at least once.
node_reregistered() { [ -n "$(node_health "$HOST")" ]; }
wait_for 60 "$HOST heartbeat resumes" node_reregistered
sleep 5

echo "[7/7] health after restart MUST still be quarantined (pre-CR-2-1: reverted to healthy)"
AFTER_HEALTH=$(node_health "$HOST")
echo "  health-after-restart=$AFTER_HEALTH"
if [ "$AFTER_HEALTH" = "quarantined" ]; then
    pass
else
    fail "RegisterNode wiped sticky Quarantined state on restart (got '$AFTER_HEALTH', RAFT-1 regression)"
fi

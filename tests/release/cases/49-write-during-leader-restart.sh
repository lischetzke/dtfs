#!/usr/bin/env bash
# Bug CF: a FUSE write in flight while the Raft leader restarts must
# complete (client re-walks the manager pool with backoff instead of
# surfacing EIO after one walk). Incident regression: dtfs-lm 2026-06-06,
# a WinSCP transfer died when the leader was restarted last in a rolling
# manager restart and the followers kept answering "forward to <dead
# leader>" for the ~2.5 s election window.
source "$(dirname "$0")/../lib/common.sh"

F="$MOUNT/release/49-leader-restart-$$.bin"
RCF="/tmp/49-write-rc-$$"
SIZE_MB=1024   # single sustained stream, ~10s+ on the lab clusters
LEADER_HOST=""

case_cleanup() {
    [ -n "$LEADER_HOST" ] && ssh_run "$LEADER_HOST" "sudo systemctl start dtfs-manager" >/dev/null 2>&1 || true
    client_run "sudo rm -f '$F' '$RCF'" >/dev/null 2>&1 || true
}
trap case_cleanup EXIT

N=$(echo "$(mgrs_list)" | wc -w)
[ "$N" -ge 3 ] || skip "need 3+ managers for leader restart under write; got $N"

LEADER_HOST=$(mgr_get_any /api/v1/nodes | python3 -c "
import json,sys
for n in json.load(sys.stdin):
    if n.get('is_leader'): print(n.get('hostname','')); break
")
[ -n "$LEADER_HOST" ] || skip "no leader reported yet"

echo "[1/4] start a sustained ${SIZE_MB} MiB write on the client (single stream, one fh)"
client_run "sudo mkdir -p $MOUNT/release"
client_run "nohup sudo sh -c 'dd if=/dev/urandom of=$F bs=4M count=$((SIZE_MB / 4)) \
    iflag=fullblock conv=fsync status=none; echo \$? > $RCF' >/dev/null 2>&1 &" \
    || fail "could not start writer"

sleep 3   # let the write get going before the restart

echo "[2/4] restart dtfs-manager on the leader ($LEADER_HOST) mid-write"
ssh_run "$LEADER_HOST" "sudo systemctl restart dtfs-manager" || fail "leader restart failed"

echo "[3/4] wait for the write to finish"
writer_done() { client_run "test -f $RCF"; }
wait_for 180 "writer finished" writer_done
RC=$(client_run "cat $RCF")
assert_eq "0" "$RC" "write exit code (EIO during election = Bug CF regression)"

echo "[4/4] verify the file is complete and readable"
SIZE=$(client_run "stat -c %s '$F'" || echo 0)
assert_eq "$((SIZE_MB * 1024 * 1024))" "$SIZE" "file size after write through leader restart"
client_run "sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'" >/dev/null 2>&1 || true
READ_HASH=$(sha256_via_fuse "$F")
[ -n "$READ_HASH" ] || fail "file unreadable after leader restart"

pass

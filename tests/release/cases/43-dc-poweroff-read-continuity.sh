#!/usr/bin/env bash
# REL-2-43: full-DC power-off, quorum holds, reads continue. Power dc-c off
# (1 of 3 Raft voters) and confirm a leader remains and the file still reads
# intact from a surviving DC. Restores dc-c in cleanup.
source "$(dirname "$0")/../lib/common.sh"
require_pve

DOWN=dc-c
CR=$(client_for_dc dc-b)   # reader in a surviving DC
[ -n "$CR" ] || skip "no dc-b client in inventory"

PATHF="$MOUNT/release/43-poweroff-$$.bin"; LOCAL=/tmp/43-src-$$.bin
RESTORED=0
case_cleanup() {
    [ "$RESTORED" = "1" ] || power_dc_on "$DOWN" >/dev/null 2>&1 || true
    rm_fuse_file "$PATHF"; rm -f "$LOCAL"
}
trap case_cleanup EXIT

echo "[1/4] write + hash from $CR"
head -c $((8 * 1024 * 1024)) /dev/urandom > "$LOCAL"
SRC=$(sha256sum "$LOCAL" | awk '{print $1}')
scp "${SSH_OPTS[@]}" "$LOCAL" "debian@$CR:/tmp/43-src.bin" >/dev/null 2>&1 || skip "cannot stage to $CR"
ssh_run "$CR" "sudo mkdir -p \"\$(dirname '$PATHF')\" && sudo cp /tmp/43-src.bin '$PATHF' && sudo sync" || fail "write failed"
ssh_run "$CR" "sudo rm -f /tmp/43-src.bin"
sleep 5

echo "[2/4] power OFF $DOWN"
power_dc_off "$DOWN"

echo "[3/4] quorum must hold (leader elected, >=2 managers answer)"
wait_for 60 "quorum after $DOWN down" quorum_ok

echo "[4/4] read still intact from $CR"
ssh_run "$CR" "sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'" >/dev/null 2>&1 || true
GOT=$(ssh_run "$CR" "sudo sha256sum '$PATHF' | awk '{print \$1}'")
assert_eq "$SRC" "$GOT" "read after $DOWN power-off"

echo "[*] restore $DOWN"
power_dc_on "$DOWN" && RESTORED=1
pass

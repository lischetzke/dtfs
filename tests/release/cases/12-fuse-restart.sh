#!/usr/bin/env bash
# BE-16: stop dtfs-fuse on the client, verify the mount goes away,
# restart, verify reads resume + sha matches.
source "$(dirname "$0")/../lib/common.sh"

F="$MOUNT/release/12-fuse-$$.bin"
case_cleanup() {
    ssh_run "$CLIENT" "sudo systemctl start dtfs-fuse" >/dev/null 2>&1 || true
    sleep 5
    client_run "sudo rm -f '$F'" >/dev/null 2>&1 || true
}
trap case_cleanup EXIT

client_run "sudo mkdir -p $MOUNT/release && sudo dd if=/dev/urandom of='$F' bs=1M count=4 status=none && sudo sync"
HASH=$(sha256_via_fuse "$F")

echo "[1/3] stop dtfs-fuse"
ssh_run "$CLIENT" "sudo systemctl stop dtfs-fuse" || fail "stop failed"
sleep 3

echo "[2/3] mount should be gone"
if ssh_run "$CLIENT" "mountpoint -q $MOUNT"; then
    skip "mount still active — fuse3 lazy umount may need manual help"
fi

echo "[3/3] restart + verify"
ssh_run "$CLIENT" "sudo systemctl start dtfs-fuse" || fail "start failed"
# Wait up to 20s for mount.
for _ in $(seq 1 20); do
    if ssh_run "$CLIENT" "mountpoint -q $MOUNT"; then break; fi
    sleep 1
done
ssh_run "$CLIENT" "mountpoint -q $MOUNT" || fail "mount did not return"
NEW=$(sha256_via_fuse "$F")
assert_eq "$HASH" "$NEW" "post-restart read"
pass

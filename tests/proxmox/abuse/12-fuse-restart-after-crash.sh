#!/usr/bin/env bash
# Bug H regression test — SOAK-RECOVERY-2026-05-18.md.
#
# When `dtfs-fuse` exits unexpectedly (OOM, panic, signal), the
# mountpoint is left in the "Transport endpoint is not connected"
# state. systemd's Restart=on-failure relaunches the binary, but the
# subsequent mount(2) used to fail because the kernel still held the
# stale session inode. The fix is `ExecStartPre=-/bin/fusermount3 -u /%I`
# in the systemd unit, which clears any stale mount before remounting.
#
# This script verifies the cycle end-to-end on one client:
#   1. SIGKILL dtfs-fuse (simulates a crash).
#   2. Wait for systemd to restart the unit.
#   3. Assert the mount is back AND a basic ls works.
#
# Run via: ssh debian@<client-ip> 'bash -s' < this-file.

set -uo pipefail

UNIT="dtfs-fuse@mnt-dtfs.service"
MOUNT=/mnt/dtfs
MAX_WAIT_SECS=30

step() { echo "[bug-h-test] $*"; }

step "ensure $UNIT is initially active"
if ! systemctl is-active "$UNIT" >/dev/null; then
    sudo systemctl start "$UNIT"
    sleep 3
fi
mountpoint -q "$MOUNT" || { echo "FAIL: $MOUNT not mounted before test"; exit 1; }

step "verify the unit file has the Bug H ExecStartPre guard"
if ! sudo systemctl cat "$UNIT" | grep -q '^ExecStartPre=-/bin/fusermount3'; then
    echo "FAIL: systemd unit is missing the Bug H ExecStartPre line"
    exit 1
fi

step "SIGKILL the running dtfs-fuse process to simulate a crash"
pid=$(pgrep -f "/usr/local/bin/dtfs-fuse.*$MOUNT" | head -1)
if [ -z "$pid" ]; then
    echo "FAIL: no dtfs-fuse process to kill"
    exit 1
fi
sudo kill -KILL "$pid"

step "wait up to ${MAX_WAIT_SECS}s for systemd to restart + remount"
ok=0
for i in $(seq 1 $MAX_WAIT_SECS); do
    if systemctl is-active "$UNIT" >/dev/null && mountpoint -q "$MOUNT"; then
        ok=1
        echo "[bug-h-test] mount back after ${i}s"
        break
    fi
    sleep 1
done

if [ $ok -eq 0 ]; then
    echo "FAIL: mount did not come back within ${MAX_WAIT_SECS}s"
    sudo systemctl status "$UNIT" --no-pager | head
    sudo journalctl -u "$UNIT" -n 20 --no-pager
    exit 1
fi

step "verify ls works (no 'Transport endpoint is not connected')"
if ! ls "$MOUNT" >/dev/null 2>&1; then
    echo "FAIL: ls $MOUNT failed after auto-restart"
    exit 1
fi

step "PASS — dtfs-fuse auto-recovered from SIGKILL + remounted cleanly"

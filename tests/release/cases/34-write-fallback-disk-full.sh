#!/usr/bin/env bash
# Bug BU — a write must route around a full disk instead of hard-failing.
# Live repro: data01-warm at 100% returned `os error 28` -> `0/1 replicas`
# -> ghost-unlink -> EIO, while data02-warm sat idle with free space.
#
# Balloon one data node's warm disk below the placement headroom, write a
# file via the FUSE client, and assert: (1) the write SUCCEEDS and reads
# back intact (capacity-aware placement routed it to a disk with room),
# and (2) no orphan *.dat.tmp is left on the ballooned disk (BU-6).
source "$(dirname "$0")/../lib/common.sh"

TARGET=$(data_list | awk '{print $1}')
WARM_MOUNT=${WARM_MOUNT:-/mnt/data01-warm}
BALLOON="$WARM_MOUNT/.dtfs-bu4-balloon.bin"
TESTFILE="$MOUNT/bu4-write-fallback.bin"
LOCAL_SRC="/tmp/bu4-src-$$.bin"

[ -n "$TARGET" ] || skip "no data node in DATA list"

# Confirm the warm mount + tmp dir exist on the target.
if ! ssh_run "$TARGET" "test -d '$WARM_MOUNT/tmp'"; then
    known_issue BU "warm mount $WARM_MOUNT/tmp not present on $TARGET — topology mismatch"
fi

case_cleanup() {
    ssh_run "$TARGET" "sudo rm -f '$BALLOON'" >/dev/null 2>&1 || true
    client_run "sudo rm -f '$TESTFILE'" >/dev/null 2>&1 || true
    rm -f "$LOCAL_SRC" 2>/dev/null || true
}
trap case_cleanup EXIT

echo "[1/5] balloon $WARM_MOUNT below headroom on $TARGET"
# Leave ~256 MB free (< 1 GiB default headroom) so placement treats it full.
AVAIL_KB=$(ssh_run "$TARGET" "df --output=avail -k '$WARM_MOUNT' | tail -1 | tr -d ' '")
[ -n "$AVAIL_KB" ] || known_issue BU "could not read free space on $TARGET:$WARM_MOUNT"
FILL_KB=$((AVAIL_KB - 262144))
if [ "$FILL_KB" -le 0 ]; then
    echo "  already under headroom (avail=${AVAIL_KB}KB)"
else
    ssh_run "$TARGET" "sudo fallocate -l ${FILL_KB}K '$BALLOON'" \
        || known_issue BU "fallocate balloon failed on $TARGET"
fi
echo "  free now: $(ssh_run "$TARGET" "df -h '$WARM_MOUNT' | tail -1 | awk '{print \$4}'")"

echo "[2/5] wait for the manager to see the disk as (near-)full"
sleep 8   # one+ heartbeat so used_bytes propagates into the placement ring

echo "[3/5] write a 40 MB file via the FUSE client"
head -c 41943040 /dev/urandom > "$LOCAL_SRC"
SRC_SHA=$(sha256sum "$LOCAL_SRC" | awk '{print $1}')
# Stage to the client then copy onto the mount (avoids piping over ssh).
scp "${SSH_OPTS[@]}" "$LOCAL_SRC" "debian@$CLIENT:/tmp/bu4-src.bin" >/dev/null 2>&1 \
    || known_issue BU "could not stage source to client"
if ! client_run "sudo cp /tmp/bu4-src.bin '$TESTFILE' && sync"; then
    fail "write to $TESTFILE failed (EIO) — capacity-aware placement did not route around the full disk"
fi

echo "[4/5] read back + verify integrity"
GOT_SHA=$(sha256_via_fuse "$TESTFILE")
client_run "sudo rm -f /tmp/bu4-src.bin" >/dev/null 2>&1 || true
assert_eq "$SRC_SHA" "$GOT_SHA" "round-trip sha256 of the file written under disk-full"

echo "[5/5] assert no orphan streaming tmp left on the ballooned disk"
LEAKED=$(ssh_run "$TARGET" "sudo find '$WARM_MOUNT/tmp' -name '*.dat.tmp' 2>/dev/null | wc -l" | tr -d ' ')
echo "  leaked .dat.tmp on $TARGET:$WARM_MOUNT = ${LEAKED:-0}"
assert_eq "0" "${LEAKED:-0}" "orphan streaming tmp count after write"

pass

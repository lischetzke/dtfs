#!/usr/bin/env bash
# BE-4: basic write + read + sha256 roundtrip via FUSE.
source "$(dirname "$0")/../lib/common.sh"

F="$MOUNT/release/01-basic-$$.bin"

case_cleanup() {
    client_run "sudo rm -f '$F'" >/dev/null 2>&1 || true
}
trap case_cleanup EXIT

echo "[1/3] Write 4 MiB random file"
client_run "sudo mkdir -p $MOUNT/release && \
            sudo dd if=/dev/urandom of='$F' bs=1M count=4 status=none && \
            sudo sync"

echo "[2/3] sha256 immediate read"
WRITE_HASH=$(client_run "sudo sha256sum '$F' | awk '{print \$1}'")
[ -n "$WRITE_HASH" ] || fail "sha256 returned empty after write"

echo "[3/3] sha256 after metadata round-trip (drop_caches)"
client_run "sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'" >/dev/null 2>&1 || true
READ_HASH=$(sha256_via_fuse "$F")
assert_eq "$WRITE_HASH" "$READ_HASH" "sha256 after re-read"

pass

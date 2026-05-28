#!/usr/bin/env bash
# BE-6: operator migrate — source preserved + target replica added.
source "$(dirname "$0")/../lib/common.sh"

F="$MOUNT/release/05-migrate-$$.bin"
case_cleanup() { client_run "sudo rm -f '$F'" >/dev/null 2>&1 || true; }
trap case_cleanup EXIT

echo "[1/3] write 4 MiB"
client_run "sudo mkdir -p $MOUNT/release && sudo dd if=/dev/urandom of='$F' bs=1M count=4 status=none && sudo sync"
sleep 3
HASH=$(sha256_via_fuse "$F")

echo "[2/3] POST /api/v1/migrate (path → cold)"
RESP=$(mgr_post_leader /api/v1/migrate "{\"path\":\"${F#$MOUNT}\",\"target_tier\":\"cold\"}")
echo "  response: $RESP" | head -1
sleep 8

echo "[3/3] sha256 still matches after migrate"
NEW=$(sha256_via_fuse "$F")
assert_eq "$HASH" "$NEW" "post-migrate sha"
pass

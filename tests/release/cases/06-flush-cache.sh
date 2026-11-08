#!/usr/bin/env bash
# BE-7: flush_cache — mandatory preserved, audit emitted.
source "$(dirname "$0")/../lib/common.sh"

PREFIX=/release/06-flush-$$/
client_run "sudo mkdir -p $MOUNT$PREFIX"
case_cleanup() { client_run "sudo rm -rf $MOUNT$PREFIX" >/dev/null 2>&1 || true; }
trap case_cleanup EXIT

echo "[1/3] write 3 files + 2 reads each (triggers cache promote)"
for i in 1 2 3; do
    client_run "sudo dd if=/dev/urandom of=$MOUNT${PREFIX}f$i.bin bs=1M count=4 status=none && sudo sync"
    client_run "sudo cat $MOUNT${PREFIX}f$i.bin > /dev/null; sleep 1; sudo cat $MOUNT${PREFIX}f$i.bin > /dev/null"
done
sleep 5

echo "[2/3] flush_cache"
RESP=$(mgr_post_leader /api/v1/files/flush_cache "{\"path_prefix\":\"$PREFIX\",\"cache_tiers\":[\"warm\"]}")
echo "  response: $RESP" | head -1

echo "[3/3] audit shows cache.flush_complete and no AnchorViolated"
sleep 3
AUDIT=$(mgr_get_any /api/v1/audit?limit=200)
if ! echo "$AUDIT" | grep -q "cache.flush"; then
    fail "no cache.flush audit emitted"
fi
if echo "$AUDIT" | grep -q "AnchorViolated"; then
    fail "AnchorViolated alert during flush — mandatory anchor was stranded"
fi
pass

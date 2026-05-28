#!/usr/bin/env bash
# BE-13: set-mandatory endpoint shape.
source "$(dirname "$0")/../lib/common.sh"

RESP=$(mgr_post_leader /api/v1/volumes/$VOL/set-mandatory \
    '{"mandatory_replicas":{"warm":0,"cold":1}}')
if echo "$RESP" | grep -q '"error"' && ! echo "$RESP" | grep -q "ok\|true"; then
    fail "set-mandatory rejected unexpectedly: $RESP"
fi
pass

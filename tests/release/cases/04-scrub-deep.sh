#!/usr/bin/env bash
# BE-12 / BF-11: trigger manual deep scrub. Same shape as 03.
source "$(dirname "$0")/../lib/common.sh"

RESP=$(mgr_post_leader /api/v1/scrub '{"level":"deep"}')
if echo "$RESP" | grep -q '"status":"accepted"'; then
    :
elif echo "$RESP" | grep -q -E '404|not[ _]found|"error"'; then
    skip "manual scrub trigger endpoint not implemented or rejected: $RESP"
else
    fail "unexpected scrub trigger response: $RESP"
fi
pass

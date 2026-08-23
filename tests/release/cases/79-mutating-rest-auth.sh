#!/usr/bin/env bash
# 79-mutating-rest-auth.sh — LIS-18 / Sec R1: mutating manager REST
# requires cluster shared_secret when configured.
#
# Asserts:
#   1) GET stays open without secret
#   2) POST without secret → 401
#   3) POST with wrong secret → 403
#   4) POST with correct X-Dtfs-Shared-Secret proceeds past the auth gate
. "$(dirname "$0")/../lib/common.sh"

case_cleanup() { :; }
trap case_cleanup EXIT

SECRET=$(mgr_shared_secret)
[ -n "$SECRET" ] || skip "cluster has empty shared_secret (legacy open path); nothing to gate"

leader_or_any() {
    local h
    for h in $(mgrs_list); do
        local st
        st=$(curl "${CURL_OPTS[@]}" "http://$h:$MGR_PORT/api/v1/raft/state" 2>/dev/null || true)
        if echo "$st" | grep -q '"is_leader":true'; then
            echo "$h"
            return 0
        fi
    done
    mgrs_list | awk '{print $1}'
}

HOST=$(leader_or_any)
BASE="http://$HOST:$MGR_PORT"

echo "[1/4] GET /api/v1/stats without secret stays open"
CODE=$(curl "${CURL_OPTS[@]}" -o /tmp/lis18-stats.json -w '%{http_code}' \
    "$BASE/api/v1/stats" 2>/dev/null || echo 000)
[ "$CODE" = "200" ] || fail "GET /stats expected 200, got $CODE"

echo "[2/4] POST /api/v1/scrub without secret → 401"
CODE=$(curl "${CURL_OPTS[@]}" -o /tmp/lis18-nosec.json -w '%{http_code}' \
    -X POST -H 'content-type: application/json' \
    -d '{"level":"quick"}' "$BASE/api/v1/scrub" 2>/dev/null || echo 000)
[ "$CODE" = "401" ] || fail "missing secret expected 401, got $CODE body=$(cat /tmp/lis18-nosec.json 2>/dev/null)"
grep -q 'shared_secret required' /tmp/lis18-nosec.json \
    || fail "missing-secret body should mention shared_secret required"

echo "[3/4] POST /api/v1/scrub with wrong secret → 403"
CODE=$(curl "${CURL_OPTS[@]}" -o /tmp/lis18-bad.json -w '%{http_code}' \
    -X POST -H 'content-type: application/json' \
    -H 'X-Dtfs-Shared-Secret: wrong-secret-lis18' \
    -d '{"level":"quick"}' "$BASE/api/v1/scrub" 2>/dev/null || echo 000)
[ "$CODE" = "403" ] || fail "wrong secret expected 403, got $CODE body=$(cat /tmp/lis18-bad.json 2>/dev/null)"
grep -q 'shared_secret mismatch' /tmp/lis18-bad.json \
    || fail "mismatch body should say shared_secret mismatch"

echo "[4/4] POST /api/v1/scrub with correct secret proceeds past auth"
RESP=$(mgr_post_leader /api/v1/scrub '{"level":"quick"}')
echo "  response: $(echo "$RESP" | head -c 200)"
if echo "$RESP" | grep -qE 'shared_secret (required|mismatch)'; then
    fail "authed scrub still rejected by auth gate: $RESP"
fi
if echo "$RESP" | grep -q '"status":"accepted"'; then
    :
elif echo "$RESP" | grep -q '"error"'; then
    echo "  note: scrub returned non-auth error after gate (ok for this case): $RESP"
else
    fail "unexpected scrub response: $RESP"
fi

pass

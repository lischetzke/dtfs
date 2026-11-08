#!/usr/bin/env bash
# CF-4a — GET /api/v1/audit?limit=N must return the NEWEST N entries
# (newest-first), not the oldest. Pre-CF-4a the default local ring
# (replicate_via_raft = false) truncated to the OLDEST N in insertion
# order, so recent operator actions were silently hidden behind the
# limit while a replicated cluster showed them. The fix makes the local
# ring iterate back-to-front so both audit branches agree.
#
# Strategy: emit two distinct, identifiable audit events in a known
# order (set-spill noop, then set-cache noop — both idempotent empty-body
# posts that log a `noop` without mutating cluster state), then assert a
# small limited query returns the most-recent one first.
source "$(dirname "$0")/../lib/common.sh"

case_cleanup() { :; }  # no cluster state is mutated (both posts are no-ops)
trap case_cleanup EXIT

OLDER_ACTION="volume.set_spill"
NEWER_ACTION="volume.set_cache"

echo "[1/3] emit an OLDER audit marker (set-spill noop on $VOL)"
R1=$(mgr_post_leader "/api/v1/volumes/$VOL/set-spill" '{}') \
    || fail "set-spill post failed"
assert_match '"changed":[[:space:]]*false' "$R1" "set-spill should be a no-op"

sleep 2  # ensure a distinct second-resolution timestamp + commit ordering

echo "[2/3] emit a NEWER audit marker (set-cache noop on $VOL)"
R2=$(mgr_post_leader "/api/v1/volumes/$VOL/set-cache" '{}') \
    || fail "set-cache post failed"
assert_match '"changed":[[:space:]]*false' "$R2" "set-cache should be a no-op"

# Let the (best-effort, spawn-and-forget) replicated fan-out settle so
# the replicate_via_raft branch has both entries committed if enabled.
sleep 3

echo "[3/3] limit=1 over our two markers must return the NEWEST first"
# Scope to our two markers via the action prefix filter so unrelated
# background audit traffic can't shift the result. With limit=1 the
# single entry must be the newer of the two (set-cache).
newest_is_newer() {
    local body
    body=$(mgr_get_any "/api/v1/audit?limit=1&action=volume.set_") || return 1
    echo "$body" | python3 -c "
import json,sys
rows=json.load(sys.stdin)
if not rows:
    sys.exit(1)
# Newest-first: row[0] is the most recent matching entry.
sys.exit(0 if rows[0].get('action')=='$NEWER_ACTION' else 1)
"
}
wait_for 20 "audit limit returns newest-first (CF-4a)" newest_is_newer

# Stronger check: limit=2 must list newest-first, i.e. [set-cache, set-spill].
BODY2=$(mgr_get_any "/api/v1/audit?limit=2&action=volume.set_") \
    || fail "audit query (limit=2) failed"
ORDER=$(echo "$BODY2" | python3 -c "
import json,sys
rows=json.load(sys.stdin)
print(','.join(r.get('action','') for r in rows))
")
echo "  order=$ORDER"
assert_eq "$NEWER_ACTION,$OLDER_ACTION" "$ORDER" \
    "limit=2 must return both markers newest-first"

pass

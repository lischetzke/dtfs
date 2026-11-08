#!/usr/bin/env bash
# BE-12 / BF-11 / CF-8: manual quick scrub via /api/v1/scrub.
#
# Pre-CF-8 the endpoint answered "accepted" and dispatched NOTHING (the
# 2026-06-06 production sweep proved zero scrub starts after a trigger),
# and this case only asserted the accepted contract, so the gap was
# never caught. It now asserts EXECUTION: after the trigger, every data
# node must produce a fresh quick scrub result (relayed via heartbeat,
# Bug BY) within the wait window. This case FAILS against a pre-CF-8
# manager/data build — that is the point.
source "$(dirname "$0")/../lib/common.sh"

# Baseline: newest quick-scrub completion across all nodes (0 if none).
BEFORE=$(mgr_get_any /api/v1/stats | python3 -c '
import json,sys
d=json.load(sys.stdin)
ts=[r["completed_at_ms"] for r in d.get("scrub_results_by_node",[]) if r.get("level")=="quick"]
print(max(ts) if ts else 0)')

EXPECTED_NODES=$(mgr_get_any /api/v1/cluster/status | python3 -c '
import json,sys; print(json.load(sys.stdin)["data_nodes"])')
[ -n "$EXPECTED_NODES" ] && [ "$EXPECTED_NODES" -ge 1 ] || skip "no data nodes registered"

RESP=$(mgr_post_leader /api/v1/scrub '{"level":"quick"}')
if echo "$RESP" | grep -q '"status":"accepted"'; then
    :
elif echo "$RESP" | grep -q -E '404|not[ _]found|"error"'; then
    skip "manual scrub trigger endpoint not implemented or rejected: $RESP"
else
    fail "unexpected scrub trigger response: $RESP"
fi

# CF-8 — the trigger must actually run: each data node executes the
# quick scrub and ships its result on the next heartbeat
# (stats.scrub_results_by_node gains a fresh quick entry per node).
scrub_executed_everywhere() {
    mgr_get_any /api/v1/stats | python3 -c "
import json,sys
d=json.load(sys.stdin)
fresh={r['node_id'] for r in d.get('scrub_results_by_node',[])
       if r.get('level')=='quick' and r.get('completed_at_ms',0) > $BEFORE}
sys.exit(0 if len(fresh) >= $EXPECTED_NODES else 1)"
}
wait_for 120 "manual quick scrub executes on all $EXPECTED_NODES data nodes" \
    scrub_executed_everywhere

# Targets are not implemented — the API must say so, not silently
# accept-and-ignore (the pre-CF-8 behaviour).
T=$(mgr_post_leader /api/v1/scrub '{"level":"quick","target_type":"disk","target":"/mnt/x"}')
assert_match 'not implemented' "$T" "per-target scrub is rejected honestly"

pass

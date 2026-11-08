#!/usr/bin/env bash
# Bug BV — (BV-3) cluster_status.data_nodes must count only role==Data and
# agree with stats.data_node_count and the live /api/v1/nodes roster; and
# (BV-1) stats/cluster_status must expose an `anchor_pending` count so an
# unanchored-but-present file isn't hidden by files_missing_chunks:0.
source "$(dirname "$0")/../lib/common.sh"

NODES=$(mgr_get_any /api/v1/nodes) || known_issue BV "no manager reachable"
STATUS=$(mgr_get_any /api/v1/cluster/status) || known_issue BV "cluster/status unreachable"
STATS=$(mgr_get_any /api/v1/stats) || known_issue BV "stats unreachable"

# Actual role==Data count from the node roster.
ACTUAL_DATA=$(echo "$NODES" | python3 -c "
import json,sys
print(sum(1 for n in json.load(sys.stdin) if str(n.get('role','')).lower()=='data'))
")
CS_DATA=$(echo "$STATUS" | python3 -c "import json,sys; print(json.load(sys.stdin).get('data_nodes','MISSING'))")
ST_DATA=$(echo "$STATS"  | python3 -c "import json,sys; print(json.load(sys.stdin).get('data_node_count','MISSING'))")

echo "[1/3] data-node counts: roster=$ACTUAL_DATA cluster_status=$CS_DATA stats=$ST_DATA"
assert_eq "$ACTUAL_DATA" "$CS_DATA" "cluster_status.data_nodes vs role==Data roster"
assert_eq "$ACTUAL_DATA" "$ST_DATA" "stats.data_node_count vs role==Data roster"

echo "[2/3] cluster_status exposes anchor_pending"
CS_AP=$(echo "$STATUS" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['anchor_pending'] if 'anchor_pending' in d else 'MISSING')")
[ "$CS_AP" = "MISSING" ] && fail "cluster_status missing anchor_pending field"
echo "  cluster_status.anchor_pending = $CS_AP"

echo "[3/3] stats exposes anchor_pending"
ST_AP=$(echo "$STATS" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['anchor_pending'] if 'anchor_pending' in d else 'MISSING')")
[ "$ST_AP" = "MISSING" ] && fail "stats missing anchor_pending field"
echo "  stats.anchor_pending = $ST_AP"

# anchor_pending must be a non-negative integer.
case "$ST_AP" in
    ''|*[!0-9]*) fail "anchor_pending not a non-negative integer: '$ST_AP'" ;;
esac

pass

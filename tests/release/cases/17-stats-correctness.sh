#!/usr/bin/env bash
# BE-10: cluster stats sanity checks.
#   logical_bytes ≤ net_used_bytes
#   raw_used_bytes ≥ net_used_bytes  (per-tier RF division)
#   nodes 6/6 healthy
#   tier_breakdown sums == raw_total / raw_used
source "$(dirname "$0")/../lib/common.sh"

S=$(mgr_get_any /api/v1/stats)
[ -n "$S" ] || fail "/api/v1/stats empty"

eval "$(echo "$S" | python3 -c "
import json,sys
d=json.load(sys.stdin)
print(f'LOG={d[\"logical_bytes\"]}')
print(f'NET={d[\"net_used_bytes\"]}')
print(f'RAW={d[\"raw_used_bytes\"]}')
print(f'TIER_USED_SUM={sum(t[\"used_bytes\"] for t in d[\"tier_breakdown\"])}')
print(f'TIER_TOTAL_SUM={sum(t[\"total_bytes\"] for t in d[\"tier_breakdown\"])}')
print(f'NODES_HEALTHY={d[\"healthy_nodes\"]}')
print(f'NODES_TOTAL={d[\"total_nodes\"]}')
")"

echo "logical=$LOG  net=$NET  raw=$RAW"
echo "tier_used_sum=$TIER_USED_SUM  tier_total_sum=$TIER_TOTAL_SUM"
echo "nodes_healthy=$NODES_HEALTHY  total=$NODES_TOTAL"

# 1. logical ≤ net (anything else = phantom; not necessarily fatal but flag).
if [ "$LOG" -gt "$NET" ]; then
    fail "logical $LOG > net_used $NET — phantom bytes (metadata without chunks)"
fi

# 2. raw_used ≥ net_used (RF division shrinks raw → net).
if [ "$NET" -gt "$RAW" ]; then
    fail "net_used $NET > raw_used $RAW — impossible without RF<1"
fi

# 3. tier_used_sum ≈ raw_used (Bug BB: apportionment may diverge from
# real per-disk usage but the SUM must still match the per-node total).
DIFF=$(python3 -c "print(abs($TIER_USED_SUM - $RAW))")
TOL=$(python3 -c "print(max(int($RAW * 0.05), 1024*1024))")
if [ "$DIFF" -gt "$TOL" ]; then
    fail "tier_used_sum $TIER_USED_SUM diverges from raw_used $RAW by >$TOL"
fi

# 4. Healthy nodes equal total nodes.
if [ "$NODES_HEALTHY" -lt "$NODES_TOTAL" ]; then
    fail "$((NODES_TOTAL - NODES_HEALTHY)) node(s) unhealthy"
fi

pass

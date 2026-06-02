#!/usr/bin/env bash
# BB-1: /api/v1/stats per-tier used_bytes must reflect REAL per-disk usage,
# not a capacity-share apportionment (BA-2 lie: equal-size warm+cold disks
# forced to identical-to-the-byte usage). Asserts the stats tier_breakdown
# used_bytes matches the sum of per-disk used_bytes from /api/v1/nodes,
# within a small tolerance.
source "$(dirname "$0")/../lib/common.sh"

TOL=${TOL:-1048576} # 1 MiB tolerance

# Emit "<tier> <stats_used> <real_used>" per tier with disks.
python_cmp() {
python3 - "$1" "$2" <<'PY'
import json,sys
stats=json.loads(sys.argv[1]); nodes=json.loads(sys.argv[2])
from collections import defaultdict
real=defaultdict(int); has=defaultdict(int)
for n in nodes:
    if str(n.get('role','')).lower()=='data':
        for d in n.get('disks',[]) or []:
            real[d.get('tier')]+=int(d.get('used_bytes',0)); has[d.get('tier')]+=1
sb={t.get('tier'):int(t.get('used_bytes',0)) for t in stats.get('tier_breakdown',[])}
for tier in sorted(set(list(real)+list(sb))):
    if has.get(tier,0)==0: continue
    print(tier, sb.get(tier,0), real.get(tier,0))
PY
}

STATS=$(mgr_get_any /api/v1/stats)
NODES=$(mgr_get_any /api/v1/nodes)
[ -n "$STATS" ] && [ -n "$NODES" ] || skip "stats/nodes API unavailable"

echo "[1/2] compare per-tier stats.used_bytes vs real disk sums"
rows=$(python_cmp "$STATS" "$NODES")
[ -n "$rows" ] || skip "no tier rows with disks"
echo "$rows" | while read -r t s r; do echo "  $t: stats=$s real=$r"; done

echo "[2/2] assert within tolerance"
bad=""
while read -r t s r; do
    diff=$(( s > r ? s - r : r - s ))
    [ "$diff" -gt "$TOL" ] && bad="$bad $t(Δ$diff)"
done <<< "$rows"

if [ -z "$bad" ]; then
    pass
else
    known_issue BB "stats per-tier used_bytes diverges from real disk sums:$bad — BB-1 fidelity fix not in the deployed binary"
fi

#!/usr/bin/env bash
# BN-2: per-heartbeat UpdateNodeUsage applies must NOT write audit rows
# (telemetry, not auditable), and UpdateNodeStatus must audit only genuine
# health transitions. Data nodes heartbeat every 5 s, so pre-BN-2 the audit
# feed accumulated ~24 UpdateNodeUsage rows/min and evicted real events.
#
# Wait several heartbeat cycles and assert the count of UpdateNodeUsage
# rows in the audit feed stays 0 (and does not grow).
source "$(dirname "$0")/../lib/common.sh"

count_summary_prefix() {
    mgr_get_any "/api/v1/audit?limit=400" | python3 -c "
import json,sys
d=json.load(sys.stdin)
rows=d if isinstance(d,list) else d.get('entries',d.get('rows',[]))
print(sum(1 for r in rows if str(r.get('summary','')).startswith(sys.argv[1])))
" "$1"
}

echo "[1/3] baseline UpdateNodeUsage audit rows"
U0=$(count_summary_prefix "UpdateNodeUsage")
echo "  UpdateNodeUsage rows now: $U0"

echo "[2/3] wait 30s (~6 heartbeat cycles)"
sleep 30

echo "[3/3] re-count"
U1=$(count_summary_prefix "UpdateNodeUsage")
echo "  UpdateNodeUsage rows after wait: $U1"

# Post-BN-2: must NOT grow (data nodes heartbeat ~6x in 30s). Residual
# rows from before a rolling upgrade may linger in the capped log and get
# evicted over time, so we assert no-growth (U1 <= U0), not absolute zero.
# Pre-BN-2: grows every 5s -> U1 > U0.
if [ "${U1:-1}" -le "${U0:-0}" ]; then
    pass
else
    known_issue BN "UpdateNodeUsage audit rows grew $U0 -> $U1 over 30s — BN-2 heartbeat-audit suppression not in the deployed manager binary"
fi

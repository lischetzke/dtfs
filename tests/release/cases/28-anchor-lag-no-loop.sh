#!/usr/bin/env bash
# BO-1 + BO-2: the anchor-lag repair scheduler must not spin a tight loop.
# Pre-BO, a same-node warm->cold anchor copy failed with "no healthy source
# replica" (BO-1: dispatcher excluded the only holder), the task went Failed,
# and check_anchor_lag re-queued it on the very next 30s tick (BO-2: Failed
# excluded from the dedup set) — producing audit feeds that were ~100%
# anchor.lag_repair + migration.failed.
#
# This case samples the audit feed and asserts it is NOT dominated by
# anchor.lag_repair / migration.failed rows. Pre-BO -> ~100% spam ->
# KNOWN_ISSUE [BO]; post-BO -> low fraction -> PASS.
source "$(dirname "$0")/../lib/common.sh"

# Fraction (percent, integer) of the last N audit rows whose action/summary
# is anchor.lag_repair or migration.failed.
spam_pct() {
    mgr_get_any "/api/v1/audit?limit=100" | python3 -c "
import json,sys
d=json.load(sys.stdin)
rows=d if isinstance(d,list) else d.get('entries',d.get('rows',[]))
if not rows:
    print(0); sys.exit()
def is_spam(r):
    s=(str(r.get('action',''))+' '+str(r.get('summary','')))
    return ('anchor.lag_repair' in s) or ('migration.failed' in s)
n=sum(1 for r in rows if is_spam(r))
print(int(100*n/len(rows)))
"
}

echo "[1/2] sample audit feed spam fraction"
P0=$(spam_pct)
echo "  anchor.lag_repair/migration.failed = ${P0}% of last 100 rows"

echo "[2/2] wait 35s (>1 scheduler tick) and re-sample"
sleep 35
P1=$(spam_pct)
echo "  after wait = ${P1}%"

# A converged cluster shows a low, non-growing spam fraction. The tight
# loop pegs this near 100% and keeps it there.
if [ "${P1:-100}" -lt 80 ]; then
    pass
else
    known_issue BO "audit feed is ${P1}% anchor.lag_repair/migration.failed — anchor-lag tight loop (BO-1/BO-2) not in the deployed manager binary"
fi

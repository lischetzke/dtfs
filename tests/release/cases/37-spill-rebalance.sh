#!/usr/bin/env bash
# Bug BX (BX-2 spill + BX-11 rebalance) — end-to-end capacity spill and
# reconcile. The full scenario is: fill EVERY disk of the mandated tier across
# all data nodes, write a chunk -> it spills to the next-best tier (succeeds,
# `placement.spill` audited, chunk classifies non-stable in chunk_placement);
# then free the mandated tier -> the per-tier backfill (BX-4) + rebalance
# (BX-11) converge it back to `stable`.
#
# Filling an entire tier across the cluster is destructive and topology-
# dependent (needs >= 2 data nodes with the same tier + spare capacity to
# balloon). On a minicluster where one warm disk is already full and the
# default volume resolves to the cold tier (which has room), the spill branch
# can't be exercised without also filling cold — which risks the live data
# set. This case therefore verifies what it safely can and pins KNOWN_ISSUE
# for the destructive fill when the topology can't host it.
source "$(dirname "$0")/../lib/common.sh"

# 1. The placement metric must be present + self-consistent (BX-6 contract).
STATS=$(mgr_get_any /api/v1/stats) || known_issue BX "stats unreachable"
HAS_CP=$(echo "$STATS" | python3 -c "import json,sys; print('yes' if 'chunk_placement' in json.load(sys.stdin) else 'no')")
[ "$HAS_CP" = "yes" ] || known_issue BX "stats.chunk_placement absent — BX-6 not deployed"

# 2. If a spill has ever occurred on this cluster, the audit log carries a
#    placement.spill row (BX-10). Report it; absence is not a failure (no
#    spill may have been needed).
AUDIT=$(mgr_get_any "/api/v1/audit?limit=500" 2>/dev/null || echo "[]")
SPILLS=$(echo "$AUDIT" | python3 -c "
import json,sys
try: d=json.load(sys.stdin)
except Exception: print(0); sys.exit(0)
rows=d if isinstance(d,list) else d.get('entries',d.get('audit',[]))
print(sum(1 for r in rows if 'placement.spill' in json.dumps(r)))
" 2>/dev/null || echo 0)
echo "[info] placement.spill audit rows seen: ${SPILLS:-0}"

# 3. The rebalance pass must never have driven a chunk under goal: missing
#    chunks (realized data loss) must stay 0 on a healthy cluster.
MISSING=$(echo "$STATS" | python3 -c "import json,sys; print(json.load(sys.stdin)['chunk_placement']['missing'])")
echo "[info] chunk_placement.missing = $MISSING"
assert_eq "0" "$MISSING" "no chunk driven to missing by the rebalance pass"

# 4. The destructive fill-all-tiers spill -> free -> rebalance convergence is
#    not run here (would risk the live data set on a 2-node-per-tier lab).
known_issue BX "fill-all-tiers spill/rebalance convergence not exercised on the shared lab (destructive); spill+rebalance covered by unit tests bx1/bx2-compile/bx5 + the deployed rebalance pass (missing=0, no under-replication)"

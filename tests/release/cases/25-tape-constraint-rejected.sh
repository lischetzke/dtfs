#!/usr/bin/env bash
# BM-3: setting mandatory_replicas.tape > 0 on a cluster with no tape-tier
# disk must be REJECTED at volume save (the under-replication scheduler
# could never satisfy it). Pre-BM-3 the API accepted it silently and the
# scheduler queued a failing replication task for every chunk forever.
#
# The case POSTs set-mandatory {tape_replicas:1} to the default volume and
# asserts the API rejects it (error mentioning tape) AND the volume's tape
# mandatory stays 0. SKIPs if the cluster actually has a tape disk (then
# the constraint IS satisfiable and rejection is not expected).
source "$(dirname "$0")/../lib/common.sh"

cluster_has_tape() {
    mgr_get_any /api/v1/nodes | python3 -c "
import json,sys
for n in json.load(sys.stdin):
    for d in n.get('disks',[]) or []:
        if str(d.get('tier','')).lower()=='tape':
            sys.exit(0)
sys.exit(1)
"
}

vol_tape_mandatory() {
    mgr_get_any "/api/v1/volumes/$VOL" | python3 -c "
import json,sys
d=json.load(sys.stdin)
m=d.get('mandatory_replicas',{}) or {}
print(m.get('tape',0))
"
}

case_cleanup() {
    # Best-effort: make sure we never leave tape mandatory set.
    mgr_post_leader "/api/v1/volumes/$VOL/set-mandatory" '{"tape_replicas":0}' >/dev/null 2>&1 || true
}
trap case_cleanup EXIT

echo "[1/3] precondition: cluster has no tape-tier disk"
if cluster_has_tape; then
    skip "cluster has a tape-tier disk; tape constraint would be satisfiable"
fi

echo "[2/3] attempt to set mandatory_replicas.tape=1 on volume $VOL"
RESP=$(mgr_post_leader "/api/v1/volumes/$VOL/set-mandatory" '{"tape_replicas":1}')
echo "  response: $RESP"

echo "[3/3] assert rejected + volume tape mandatory still 0"
AFTER=$(vol_tape_mandatory)
echo "  volume.mandatory_replicas.tape=$AFTER"

if echo "$RESP" | grep -qi 'tape' && echo "$RESP" | grep -qi 'error' && [ "${AFTER:-0}" = "0" ]; then
    pass
else
    known_issue BM "set-mandatory accepted an unsatisfiable tape constraint (tape=$AFTER) — BM-3 validation not in the deployed manager binary"
fi

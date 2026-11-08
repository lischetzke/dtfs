#!/usr/bin/env bash
# REL-2-75: the operator node-decommission endpoint
# (POST /api/v1/nodes/{id}/decommission, Raft DeregisterNode) must REFUSE to
# remove a node that still holds chunk replicas. Removing it would dangle the
# node's location entries and silently lower the replication factor with no
# re-replication (integrity prime directive, CLAUDE.md A.6 #5). A drained node
# removes cleanly (covered by the CO-2-3 join-test cleanup + the dtfs-raft unit
# test deregister_node_refuses_while_holding_replicas_then_succeeds_when_drained).
# This case gates the LIVE guard: a replica-holding node is not removable and
# the file survives the refused attempt.
source "$(dirname "$0")/../lib/common.sh"
source "$(dirname "$0")/../lib/dc.sh"

PREFIX=/release/75-decom-$$/
PATHF="${MOUNT}${PREFIX}f.bin"
APIF="${PATHF#$MOUNT}"  # mount-relative path for manager API calls
LOCAL=/tmp/75-src-$$.bin

CW=$(client_for_dc dc-a); [ -n "$CW" ] || CW="$CLIENT"
[ -n "$CW" ] || skip "no client available"

case_cleanup() { client_run "sudo rm -rf ${MOUNT}${PREFIX}" >/dev/null 2>&1 || true; rm -f "$LOCAL"; }
trap case_cleanup EXIT

echo "[1/4] write a file so a data node holds a replica"
head -c $((4 * 1024 * 1024)) /dev/urandom > "$LOCAL"
scp "${SSH_OPTS[@]}" "$LOCAL" "debian@$CW:/tmp/75-src.bin" >/dev/null 2>&1 || skip "cannot stage src"
ssh_run "$CW" "sudo mkdir -p ${MOUNT}${PREFIX} && sudo cp /tmp/75-src.bin '$PATHF' && sudo sync" || fail "write failed"
ssh_run "$CW" "sudo rm -f /tmp/75-src.bin"
wait_for_soft 45 inspect_has_replicas "$APIF" || known_issue REL-1-2 "inspect lacks per-replica array (REL-1-2)"

echo "[2/4] resolve a node_id that holds a replica of this file"
HOLDER=$(mgr_get_any "/api/v1/files/inspect?path=$APIF" | python3 -c "
import json,sys
d=json.load(sys.stdin)
for c in d.get('chunks',[]):
    for r in c.get('replicas',[]):
        nid=r.get('node_id','')
        if nid:
            print(nid); sys.exit(0)
")
[ -n "$HOLDER" ] || skip "could not resolve a replica holder node_id"
echo "  holder node_id=$HOLDER"

echo "[3/4] decommission MUST be refused while the node holds the replica"
RESP=$(mgr_post_leader "/api/v1/nodes/$HOLDER/decommission" \
    '{"operator_id":"rel-75","reason":"integrity guard live test: replica-holding node must not be removable"}')
echo "  resp: $(echo "$RESP" | head -c 200)"
echo "$RESP" | grep -q '"ok":true' && fail "decommission of a replica-holding node was NOT refused"

echo "[4/4] node still registered + file still readable after the refused attempt"
mgr_get_any /api/v1/nodes | python3 -c "
import json,sys
for n in json.load(sys.stdin):
    if str(n.get('node_id'))=='$HOLDER':
        sys.exit(0)
sys.exit(1)" || fail "holder node disappeared after a REFUSED decommission (guard breached)"
ssh_run "$CW" "sudo cat '$PATHF' > /dev/null" || fail "file unreadable after refused decommission"
echo "  integrity guard held: replica-holding node not removable, file intact"
pass

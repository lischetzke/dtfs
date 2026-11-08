#!/usr/bin/env bash
# CO-3-7 [HIGH]: a corrupt/missing chunk report (ReportChunkStatus) that lands
# on a FOLLOWER manager must still de-locate the bad replica (and let the
# scheduler re-replicate). Pre-fix the handler did `let _ = raft_write(...)`,
# which on a follower returns ForwardToLeader and was silently swallowed: the
# corrupt replica stayed advertised as healthy and re-replication never queued.
# Post-fix `report_chunk_status` routes through `raft_write_or_forward` (forwards
# to the leader), so the de-location commits regardless of which manager the
# report hit.
#
# Live forge: write a file with >=2 warm replicas, send a forged corrupt
# ReportChunkStatus for ONE warm replica to a FOLLOWER via grpcurl, and assert
# that replica is de-located. The manager gRPC has no reflection, so grpcurl is
# pointed at proto/. KNOWN_ISSUE only when grpcurl/proto is unavailable.
#
# Safety: RemoveCorruptChunkReplicas drops ALL of the reported node's replicas
# of the chunk (node-wide), so we target a warm replica on a node that does NOT
# also hold the chunk's cold anchor -- never risk stranding the anchor.
source "$(dirname "$0")/../lib/common.sh"
source "$(dirname "$0")/../lib/dc.sh"

PROTO_DIR="$(cd "$(dirname "$0")/../../../proto" 2>/dev/null && pwd)"
PREFIX=/release/71-corrupt-$$/
PATHF="${MOUNT}${PREFIX}f.bin"
APIF="${PATHF#$MOUNT}"
SAVED=0
restore_policy() {
    [ "$SAVED" = "1" ] || return 0
    mgr_post_leader /api/v1/volumes/$VOL/set-mandatory '{"warm_replicas":2,"cold_replicas":1}' >/dev/null 2>&1 || true
}
case_cleanup() { restore_policy; client_run "sudo rm -rf ${MOUNT}${PREFIX}" >/dev/null 2>&1 || true; }
trap case_cleanup EXIT

CW=$(client_for_dc dc-a); [ -n "$CW" ] || CW="$CLIENT"
[ -n "$CW" ] || skip "no client available"

echo "[1/5] preconditions: grpcurl + proto"
command -v grpcurl >/dev/null 2>&1 \
    || known_issue CO-3-7 "grpcurl unavailable: forged ReportChunkStatus not live-wirable; routing fix (raft_write_or_forward) unit-covered"
[ -n "$PROTO_DIR" ] && [ -f "$PROTO_DIR/manager.proto" ] \
    || known_issue CO-3-7 "manager.proto not found at $PROTO_DIR; cannot resolve the RPC without server reflection"

echo "[2/5] mandatory warm=2 cold=1 + write a file so a chunk has 2 warm replicas"
mgr_post_leader /api/v1/volumes/$VOL/set-mandatory '{"warm_replicas":2,"cold_replicas":1}' >/dev/null; SAVED=1
ssh_run "$CW" "sudo mkdir -p ${MOUNT}${PREFIX} && head -c $((4*1024*1024)) /dev/urandom | sudo tee '$PATHF' >/dev/null && sudo sync" || fail "write failed"
wait_for_soft 45 inspect_has_replicas "$APIF" || known_issue REL-1-2 "inspect lacks per-replica array (REL-1-2)"

echo "[3/5] pick a warm replica whose node does NOT hold the chunk's cold anchor"
# Wait for a chunk that actually has a warm replica on a non-anchor node.
pick_victim() {
    mgr_get_any "/api/v1/files/inspect?path=$APIF" | python3 -c "
import json,sys
d=json.load(sys.stdin)
for c in d.get('chunks',[]):
    reps=c.get('replicas',[])
    cold_nodes={r['node_id'] for r in reps if str(r.get('tier','')).lower()=='cold'}
    for r in reps:
        if str(r.get('tier','')).lower()=='warm' and r['node_id'] not in cold_nodes:
            print(c['chunk_id'], r['node_id']); sys.exit(0)
sys.exit(1)
"
}
VICTIM_LINE=""
for _ in $(seq 1 20); do
    VICTIM_LINE=$(pick_victim) && [ -n "$VICTIM_LINE" ] && break
    sleep 5
done
[ -n "$VICTIM_LINE" ] || known_issue CO-3-7 "no warm replica on a non-anchor node within 100s (placement not converged)"
CID=$(echo "$VICTIM_LINE" | awk '{print $1}')
VICTIM=$(echo "$VICTIM_LINE" | awk '{print $2}')
echo "  chunk=$CID victim_node=$VICTIM"

_victim_present() {
    mgr_get_any "/api/v1/files/inspect?path=$APIF" | python3 -c "
import json,sys
d=json.load(sys.stdin)
for c in d.get('chunks',[]):
    if c.get('chunk_id')=='$CID':
        sys.exit(0 if any(r['node_id']=='$VICTIM' for r in c.get('replicas',[])) else 1)
sys.exit(1)"
}

echo "[4/5] forge a corrupt ReportChunkStatus for (chunk,$VICTIM) to a FOLLOWER"
TARGET=""
for h in $(mgrs_list); do
    st=$(curl "${CURL_OPTS[@]}" "http://$h:$MGR_PORT/api/v1/raft/state" 2>/dev/null \
         | python3 -c "import json,sys;print(json.load(sys.stdin).get('state',''))" 2>/dev/null)
    [ "$st" = "Follower" ] && { TARGET="$h"; break; }
done
[ -n "$TARGET" ] || skip "no follower manager found (single-manager / unhealthy cluster)"
echo "  follower=$TARGET"
OUT=$(grpcurl -plaintext -max-time 8 -import-path "$PROTO_DIR" -proto manager.proto \
      -d "{\"node_id\":\"$VICTIM\",\"chunks\":[{\"chunk_id\":\"$CID\",\"size\":4194304,\"status\":\"corrupt\"}]}" \
      "$TARGET:9401" dtfs.manager.ManagerService/ReportChunkStatus 2>&1 || true)
echo "  ack: $(echo "$OUT" | tr -d '\n' | head -c 160)"

echo "[5/5] the reported replica must be de-located (follower forwarded the de-location)"
# De-location commits within a tick; the under-replication scheduler may later
# re-replicate the warm copy back, so assert the drop promptly.
_victim_gone() { ! _victim_present; }
wait_for 30 "corrupt replica de-located on $VICTIM" _victim_gone
echo "  corrupt replica de-located via the follower (CO-3-7)"
pass

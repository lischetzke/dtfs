#!/usr/bin/env bash
# Bug BL — heartbeat Raft writes silently dropped on a non-leader manager.
# When a data node heartbeats a follower, the four Raft writes inside the
# manager heartbeat handler (UpdateNodeUsage / UpdateNodeDiskUsage /
# QuarantineNode / RemoveCorruptChunkReplicas) returned ForwardToLeader and
# were `let _`-discarded, so node liveness (last_seen_ms), quarantine state,
# and integrity-fault records never committed.
#
# Reproduction: restart the current Raft leader. A new leader is elected and
# the restarted manager rejoins as a follower; data nodes that re-bind to it
# would freeze last_seen_ms pre-BL. Post-BL the follower forwards the writes
# via the ForwardClientWrite RPC, so last_seen advances.
#
# Runs from the dtfs-builder host. PASS (exit 0) once every healthy data
# node's last_seen advanced + is fresh; FAIL on the pre-BL binary.
set -uo pipefail

MGRS=${MGRS:-mini-mgr01 mini-mgr02 mini-mgr03}
MGR_PORT=${MGR_PORT:-9402}
FRESH_S=${FRESH_S:-45}
SSH_OPTS=(-o StrictHostKeyChecking=no -i ~/.ssh/dtfs-lab/id_ed25519 -o ConnectTimeout=5)
CURL_OPTS=(-s --max-time 6)

step() { echo "[BL] $*"; }
fail() { echo "  FAIL: $*" >&2; cleanup; exit 1; }

now_ms() { date -u +%s%3N; }
mgr_get_any() {
    for h in $MGRS; do
        local r; r=$(curl "${CURL_OPTS[@]}" "http://$h:$MGR_PORT$1" 2>/dev/null)
        [ -n "$r" ] && { echo "$r"; return 0; }
    done
    return 1
}
data_rows() {
    mgr_get_any /api/v1/nodes | python3 -c "
import json,sys
for n in json.load(sys.stdin):
    if str(n.get('role','')).lower()=='data':
        print(n.get('hostname'), n.get('last_seen_ms',0), str(n.get('health','')).lower())
"
}
leader_host() {
    for h in $MGRS; do
        local st; st=$(curl "${CURL_OPTS[@]}" "http://$h:$MGR_PORT/api/v1/raft/state" 2>/dev/null \
            | python3 -c "import json,sys;print(json.load(sys.stdin).get('state',''))" 2>/dev/null)
        [ "$st" = "Leader" ] && { echo "$h"; return 0; }
    done
    return 1
}

cleanup() {
    for h in $MGRS; do
        ssh "${SSH_OPTS[@]}" "debian@$h" "sudo systemctl is-active dtfs-manager >/dev/null 2>&1 || sudo systemctl restart dtfs-manager" >/dev/null 2>&1 || true
    done
}
trap cleanup EXIT

step "locate leader"
LEADER=$(leader_host) || fail "no Raft leader"
echo "  leader=$LEADER"

step "baseline"
BASE=$(data_rows)
[ -n "$BASE" ] || fail "no data nodes"
echo "$BASE" | sed 's/^/  /'

step "restart leader $LEADER (forces re-election; it rejoins as follower)"
ssh "${SSH_OPTS[@]}" "debian@$LEADER" "sudo systemctl restart dtfs-manager"

step "wait for new leader"
for i in $(seq 1 30); do leader_host >/dev/null && break; sleep 2; done
leader_host >/dev/null || fail "no leader re-elected within 60s"
sleep 12  # ~2 heartbeat cycles

step "assert last_seen advanced + fresh"
NOW=$(now_ms); STALE=""
while read -r host ls health; do
    base=$(echo "$BASE" | awk -v h="$host" '$1==h{print $2}')
    age=$(( (NOW - ls) / 1000 ))
    echo "  $host last_seen=$ls age=${age}s health=$health baseline=$base"
    [ "$health" = "healthy" ] || continue
    { [ "$ls" -le "${base:-0}" ] || [ "$age" -gt "$FRESH_S" ]; } && STALE="$STALE $host"
done < <(data_rows)

[ -z "$STALE" ] || fail "stale/frozen last_seen after follower heartbeat:$STALE (pre-BL binary)"
echo "[BL] PASS — non-leader heartbeat writes are forwarded; last_seen current on all data nodes"

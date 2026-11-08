#!/usr/bin/env bash
# BL: a data node that heartbeats a NON-leader manager must still have its
# Raft-committed liveness (last_seen_ms) advance, because the non-leader
# now forwards the heartbeat's Raft writes to the leader (ForwardClientWrite)
# instead of silently dropping them.
#
# Reproduction: restart the current Raft leader. A new leader is elected
# among the remaining managers; the restarted manager rejoins as a
# FOLLOWER. Data nodes that re-bind their heartbeat to that follower will,
# pre-BL, freeze last_seen_ms (the UpdateNodeUsage write is ForwardToLeader-
# dropped). Post-BL the follower forwards the write, so last_seen advances.
#
# We assert every healthy data node's last_seen_ms advances and is fresh
# within FRESH_S of wall clock after the dust settles. Pre-BL binary leaves
# at least one node stale -> KNOWN_ISSUE [BL] (auto-promotes once BL ships).
source "$(dirname "$0")/../lib/common.sh"

FRESH_S=${FRESH_S:-45}

now_ms() { date -u +%s%3N; }

# Print "<hostname> <last_seen_ms> <health>" per data node.
data_node_rows() {
    mgr_get_any /api/v1/nodes | python3 -c "
import json,sys
for n in json.load(sys.stdin):
    if str(n.get('role','')).lower()=='data':
        print(n.get('hostname'), n.get('last_seen_ms',0), str(n.get('health','')).lower())
"
}

# Which manager is the current Raft leader? Echo its hostname or empty.
current_leader_host() {
    for h in $(mgrs_list); do
        local st
        st=$(curl "${CURL_OPTS[@]}" "http://$h:$MGR_PORT/api/v1/raft/state" 2>/dev/null \
             | python3 -c "import json,sys;print(json.load(sys.stdin).get('state',''))" 2>/dev/null)
        if [ "$st" = "Leader" ]; then echo "$h"; return 0; fi
    done
    return 1
}

a_leader_exists() { [ -n "$(current_leader_host)" ]; }

case_cleanup() {
    # Make sure every manager is back up.
    for h in $(mgrs_list); do
        ssh_run "$h" "sudo systemctl is-active dtfs-manager >/dev/null 2>&1 || sudo systemctl restart dtfs-manager" >/dev/null 2>&1 || true
    done
}
trap case_cleanup EXIT

echo "[1/5] locate current Raft leader"
LEADER=$(current_leader_host)
[ -n "$LEADER" ] || skip "no Raft leader found (cluster not healthy)"
echo "  leader=$LEADER"

MGR_COUNT=$(mgrs_list | wc -w)
[ "$MGR_COUNT" -ge 3 ] || skip "need >=3 managers to keep quorum across a leader restart (have $MGR_COUNT)"

echo "[2/5] baseline data-node last_seen"
data_node_rows | while read -r host ls health; do echo "  $host last_seen=$ls health=$health"; done
BASELINE=$(data_node_rows)
[ -n "$BASELINE" ] || skip "no data nodes registered"

echo "[3/5] restart the leader ($LEADER) to force re-election; it rejoins as a follower"
ssh_run "$LEADER" "sudo systemctl restart dtfs-manager"

echo "[4/5] wait for a new leader + the restarted manager to be back up"
wait_for 60 "a Raft leader exists again" a_leader_exists
wait_for 60 "$LEADER process back up" bash -c "ssh ${SSH_OPTS[*]} debian@$LEADER 'systemctl is-active dtfs-manager' | grep -q '^active'" 2>/dev/null || true
sleep 12  # ~2 heartbeat cycles so followers forward UpdateNodeUsage to the leader

echo "[5/5] assert every healthy data node's last_seen advanced + is fresh"
NOW=$(now_ms)
STALE=""
while read -r host ls health; do
    base_ls=$(echo "$BASELINE" | awk -v h="$host" '$1==h {print $2}')
    age_ms=$(( NOW - ls ))
    echo "  $host last_seen=$ls (age $(( age_ms/1000 ))s) health=$health baseline=$base_ls"
    [ "$health" = "healthy" ] || continue
    if [ "$ls" -le "${base_ls:-0}" ] || [ "$age_ms" -gt $(( FRESH_S * 1000 )) ]; then
        STALE="$STALE $host"
    fi
done < <(data_node_rows)

if [ -z "$STALE" ]; then
    pass
else
    known_issue BL "data node(s)$STALE have stale/frozen last_seen after heartbeating a follower — non-leader heartbeat Raft-write forwarding not in the deployed manager binary"
fi

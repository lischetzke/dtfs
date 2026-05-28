# tests/release/lib/health.sh — cluster readiness/health helpers for
# the E2E suite. Sourced by lib/common.sh. Depends on common.sh
# (mgr_get_any, mgrs_list, wait_for, CURL_OPTS, MGR_PORT).

# Predicate: the manager API answers the nodes endpoint on some peer.
cluster_api_up() { mgr_get_any /api/v1/nodes >/dev/null 2>&1; }

# Block until the manager API answers, up to N seconds (default 60).
wait_cluster_ready() { wait_for "${1:-60}" "cluster API ready" cluster_api_up; }

# Echo the current Raft leader's hostname (empty + non-zero if none).
mgr_leader() {
    local h r
    for h in $(mgrs_list); do
        r=$(curl "${CURL_OPTS[@]}" "http://$h:$MGR_PORT/api/v1/nodes" 2>/dev/null)
        [ -n "$r" ] || continue
        local leader
        leader=$(echo "$r" | python3 -c "
import json,sys
try: d=json.load(sys.stdin)
except Exception: sys.exit(0)
for n in d:
    if n.get('is_leader') or str(n.get('raft_role','')).lower()=='leader':
        print(n.get('hostname','')); break
" 2>/dev/null)
        if [ -n "$leader" ]; then echo "$leader"; return 0; fi
    done
    return 1
}

# Count of registered data nodes.
data_node_count() {
    mgr_get_any /api/v1/nodes | python3 -c "
import json,sys
d=json.load(sys.stdin)
print(sum(1 for n in d if str(n.get('role','')).lower()=='data'))
" 2>/dev/null
}

# Predicate: no node is quarantined and none reports an unhealthy status.
# An absent/None status is treated as healthy (the API omits it for
# nodes in the normal state).
all_nodes_healthy() {
    local r
    r=$(mgr_get_any /api/v1/nodes) || return 1
    echo "$r" | python3 -c "
import json,sys
d=json.load(sys.stdin)
ok={'healthy','ok','active','up','none',''}
bad=[n.get('hostname') for n in d
     if n.get('quarantine') or str(n.get('status') or '').lower() not in ok]
sys.exit(1 if bad else 0)
"
}

# Block until every node is healthy/un-quarantined, up to N seconds.
wait_all_healthy() { wait_for "${1:-60}" "all nodes healthy" all_nodes_healthy; }

#!/usr/bin/env bash
# BE-11: every documented endpoint returns the expected schema keys.
source "$(dirname "$0")/../lib/common.sh"

check_keys() {
    local path="$1"; shift
    local r=$(mgr_get_any "$path")
    [ -n "$r" ] || fail "$path empty response"
    for k in "$@"; do
        if ! echo "$r" | python3 -c "
import json,sys,re
d=json.load(sys.stdin)
def walk(d):
    if isinstance(d,dict): return d
    if isinstance(d,list) and d: return d[0]
    return {}
top=walk(d)
print('ok' if '$k' in top else 'missing')
" | grep -q '^ok$'; then
            fail "$path missing key '$k'"
        fi
    done
    echo "  $path ✓ ($*)"
}

check_keys /api/v1/cluster/status status cluster_name data_nodes
check_keys /api/v1/nodes node_id hostname role total_bytes_read
check_keys /api/v1/volumes volumes

# CF-4c — Manager rows must carry the raft_member flag, and on a healthy
# joined cluster every manager is a voter/learner so it must read true.
# (A false here is the production dc1 symptom: a manager configured but not
# in raft membership.) Data/client rows omit the field (None → skipped).
nodes_json=$(mgr_get_any /api/v1/nodes)
[ -n "$nodes_json" ] || fail "/api/v1/nodes empty response"
mgr_member=$(echo "$nodes_json" | python3 -c "
import json,sys
nodes=json.load(sys.stdin)
mgrs=[n for n in nodes if str(n.get('role','')).lower()=='manager']
if not mgrs:
    print('no-managers'); sys.exit(0)
missing=[n.get('hostname') for n in mgrs if 'raft_member' not in n]
if missing:
    print('missing:'+','.join(map(str,missing))); sys.exit(0)
unjoined=[n.get('hostname') for n in mgrs if n.get('raft_member') is not True]
print('unjoined:'+','.join(map(str,unjoined)) if unjoined else 'all-joined')
")
case "$mgr_member" in
    all-joined) echo "  /api/v1/nodes raft_member ✓ (all managers joined)";;
    no-managers) echo "  /api/v1/nodes raft_member ✓ (no manager rows to check)";;
    missing:*) fail "/api/v1/nodes manager rows missing raft_member: ${mgr_member#missing:}";;
    unjoined:*) fail "/api/v1/nodes managers not in raft membership: ${mgr_member#unjoined:}";;
    *) fail "/api/v1/nodes raft_member check unexpected output: $mgr_member";;
esac
check_keys /api/v1/stats raw_total_bytes raw_used_bytes net_used_bytes logical_bytes tier_breakdown
check_keys /api/v1/files files
check_keys "/api/v1/audit?limit=10" timestamp action

# Config effective (Bug BD): scope_kind not yet expected until BD-1.
r=$(mgr_get_any /api/v1/config/effective)
if [ -n "$r" ]; then
    n=$(echo "$r" | python3 -c "import json,sys; print(len(json.load(sys.stdin).get('entries',[])))" 2>/dev/null || echo 0)
    if [ "$n" -lt 40 ]; then
        fail "/api/v1/config/effective only $n entries (<40)"
    fi
    echo "  /api/v1/config/effective ✓ ($n entries)"
fi

pass

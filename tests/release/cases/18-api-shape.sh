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

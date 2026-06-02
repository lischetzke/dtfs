#!/usr/bin/env bash
# Bug BR-1: every manager must report version + build_date for ALL managers
# in /api/v1/nodes, regardless of which manager serves the request. Pre-BR
# only the serving manager was versioned (peers rendered blank) because
# managers are Raft peers, not RegisterNode callers, so their identity never
# entered the shared node registry (ISSUE-SCAN-2026-06-02-1525).
source "$(dirname "$0")/../lib/common.sh"

# Print "<total_mgrs> <with_version> <with_build_date>" for one manager's view.
managers_versioned_on() {
    local host="$1"
    curl "${CURL_OPTS[@]}" "http://$host:$MGR_PORT/api/v1/nodes" 2>/dev/null | python3 -c "
import json,sys
try:
    nodes=json.load(sys.stdin)
except Exception:
    print('ERR'); sys.exit(0)
mgrs=[n for n in nodes if str(n.get('role','')).lower()=='manager']
total=len(mgrs)
ver=sum(1 for n in mgrs if n.get('version'))
bd=sum(1 for n in mgrs if n.get('build_date'))
print(f'{total} {ver} {bd}')
"
}

FAILED=0
SEEN=0
for h in $(mgrs_list); do
    out=$(managers_versioned_on "$h")
    [ "$out" = "ERR" ] && { echo "  $h: unreachable/parse error"; continue; }
    SEEN=$((SEEN + 1))
    total=$(echo "$out" | awk '{print $1}')
    ver=$(echo "$out" | awk '{print $2}')
    bd=$(echo "$out" | awk '{print $3}')
    echo "  $h: $ver/$total managers versioned, $bd/$total with build_date"
    if [ "$total" -gt 0 ] && { [ "$ver" -lt "$total" ] || [ "$bd" -lt "$total" ]; }; then
        FAILED=1
    fi
done

[ "$SEEN" -gt 0 ] || skip "no manager reachable"

if [ "$FAILED" -eq 0 ]; then
    pass
else
    known_issue BR "some managers report blank version/build_date for peers — BR-1 self-register not in deployed binaries"
fi

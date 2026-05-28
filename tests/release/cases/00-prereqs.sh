#!/usr/bin/env bash
# BE-3: prereqs — ssh works, all mgrs reachable, FUSE mounted, healthy.
source "$(dirname "$0")/../lib/common.sh"

echo "[1/4] SSH reachability"
for h in $(mgrs_list) $(data_list) "$CLIENT"; do
    ssh_run "$h" "true" || fail "ssh $h failed"
done

echo "[2/4] Manager API"
for h in $(mgrs_list); do
    r=$(curl "${CURL_OPTS[@]}" "http://$h:$MGR_PORT/api/v1/cluster/status" 2>/dev/null)
    [ -n "$r" ] || fail "mgr $h /api/v1/cluster/status empty"
done

echo "[3/4] All nodes registered"
nodes=$(mgr_get_any /api/v1/nodes)
n=$(echo "$nodes" | python3 -c "import json,sys; print(len(json.load(sys.stdin)))" 2>/dev/null || echo 0)
assert_ge "$n" 6 "registered nodes"

echo "[4/4] FUSE mount"
client_run "mountpoint -q $MOUNT" || fail "FUSE not mounted at $MOUNT on $CLIENT"

pass

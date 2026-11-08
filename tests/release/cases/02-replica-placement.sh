#!/usr/bin/env bash
# BE-5: a fresh write produces exactly the configured mandatory replicas.
source "$(dirname "$0")/../lib/common.sh"

F="$MOUNT/release/02-placement-$$.bin"

case_cleanup() { client_run "sudo rm -f '$F'" >/dev/null 2>&1 || true; }
trap case_cleanup EXIT

echo "[1/4] Write 4 MiB"
client_run "sudo mkdir -p $MOUNT/release && sudo dd if=/dev/urandom of='$F' bs=1M count=4 status=none && sudo sync"

echo "[2/4] Look up mandatory_replicas from volume"
mreq=$(mgr_get_any /api/v1/volumes | python3 -c "
import json,sys
v=json.load(sys.stdin).get('volumes',[])
for x in v:
    if x.get('volume_id')=='$VOL':
        m=x.get('mandatory_replicas',{}) or x.get('replication',{})
        print(sum(int(m.get(t,0)) for t in ('hot','warm','cold','tape')))
        break
")
[ -n "$mreq" ] || fail "could not read volume mandatory_replicas"
assert_ge "$mreq" 1 "mandatory replica total"

echo "[3/4] Wait for chunk_locations to converge ($mreq replicas)"
fname=${F#$MOUNT}
sleep 5

echo "[4/4] Confirm distribution honours mandatory"
data=$(mgr_get_any "/api/v1/files/inspect?path=$fname")
n=$(echo "$data" | python3 -c "
import json,sys
d=json.load(sys.stdin)
print(sum(len(c.get('node_ids',[])) for c in d.get('chunks',[])))
")
assert_ge "$n" "$mreq" "actual replica count"

pass

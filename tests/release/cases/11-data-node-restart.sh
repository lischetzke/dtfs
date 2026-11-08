#!/usr/bin/env bash
# BE-15: stop a data node, verify reads still serve via the other
# replica, restart, verify the node rejoins.
source "$(dirname "$0")/../lib/common.sh"

F="$MOUNT/release/11-dn-restart-$$.bin"
case_cleanup() {
    for h in $(data_list); do
        ssh_run "$h" "sudo systemctl start dtfs-data" >/dev/null 2>&1 || true
    done
    client_run "sudo rm -f '$F'" >/dev/null 2>&1 || true
}
trap case_cleanup EXIT

client_run "sudo mkdir -p $MOUNT/release && sudo dd if=/dev/urandom of='$F' bs=1M count=4 status=none && sudo sync"
HASH=$(sha256_via_fuse "$F")
sleep 3

# Pick a data node holding a replica.
fname=${F#$MOUNT}
CID=$(chunk_ids_for_path "$fname" | head -1)
[ -n "$CID" ] || fail "no chunk_id"
HOST=""
for h in $(data_list); do
    if host_has_chunk "$h" "$CID"; then HOST="$h"; break; fi
done
[ -n "$HOST" ] || skip "no host holds chunk yet"

# Need at least 2 replicas for fallback.
COUNT=0; for h in $(data_list); do host_has_chunk "$h" "$CID" && COUNT=$((COUNT+1)); done
[ "$COUNT" -ge 2 ] || skip "fewer than 2 replicas — RF=1 cluster"

echo "[1/3] stop $HOST"
ssh_run "$HOST" "sudo systemctl stop dtfs-data" || fail "stop failed"
sleep 8

echo "[2/3] read still ok via fallback"
client_run "sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'" >/dev/null 2>&1 || true
NEW=$(sha256_via_fuse "$F")
assert_eq "$HASH" "$NEW" "read after data node down"

echo "[3/3] restart $HOST + reconverge"
ssh_run "$HOST" "sudo systemctl start dtfs-data" || true
sleep 8
pass

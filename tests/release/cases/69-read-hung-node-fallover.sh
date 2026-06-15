#!/usr/bin/env bash
# FUSE-HOL: a read must not hang forever on a hung/dead data node. read() fetches
# each chunk with read_chunk().await; the P0-2 fallback only triggers on a
# transport Err, NOT on a hang (a node that holds the connection but never
# returns the chunk). Pre-fix, with the data node's chunk-port responses dropped
# on an established channel, a cold read hung indefinitely (>35s, observed). The
# fix bounds each per-replica fetch with `client.chunk_read_timeout_secs` and
# fails over to the next replica on timeout. (This supersedes the CO-3-12
# concurrent-open angle: fuse3 dispatches handlers concurrently, so the mount is
# not globally serialized; CO-3-12's per-file guard is unit-covered.)
#
# Live repro: 2 replicas (dc-a + dc-b). Lower the reader's fetch timeout, warm
# its channel to the dc-a node, DROP that node's :9500 responses (established
# channel => GetChunk hangs mid-stream, no transport error), then cold-read a
# second file whose nearest replica is that node. It must fall over to the other
# replica within ~the timeout and return CORRECT bytes — not hang.
source "$(dirname "$0")/../lib/common.sh"
source "$(dirname "$0")/../lib/dc.sh"

TMO=12                 # test fetch timeout (s); read must recover within ~this
IFACE=eth0
PREFIX=/release/69-hol-$$/
WRITER=$(client_for_dc dc-b); [ -n "$WRITER" ] || WRITER=""   # write from the OTHER dc so the reader is cold
READER=$(client_for_dc dc-a); [ -n "$READER" ] || READER="$CLIENT"
DNODE=$(data_hosts_in_dc dc-a | head -1)   # the reader's nearest data node (tried first); we hang it
[ -n "$READER" ] || skip "no dc-a reader client"
[ -n "$WRITER" ] && [ "$WRITER" != "$READER" ] || skip "need a second client in another DC to write cold fixtures"
[ -n "$DNODE" ] || skip "no dc-a data node to hang"
[ -n "$(data_hosts_in_dc dc-b)" ] || skip "no dc-b data node to fail over to"

CFG=/etc/dtfs/fuse.toml
SVC='dtfs-fuse@-mnt-dtfs.service'
DROPPED=0; CFG_SAVED=0; SAVED=0
case_cleanup() {
    [ "$DROPPED" = 1 ] && ssh_run "$DNODE" "sudo iptables -D OUTPUT -p tcp --sport 9500 -j DROP" >/dev/null 2>&1
    if [ "$CFG_SAVED" = 1 ]; then
        ssh_run "$READER" "sudo sed -i '/^chunk_read_timeout_secs = $TMO\$/d' $CFG; sudo systemctl restart '$SVC'" >/dev/null 2>&1
    fi
    [ "$SAVED" = 1 ] && mgr_post_leader /api/v1/volumes/$VOL/set-mandatory '{"warm_replicas":2,"cold_replicas":1}' >/dev/null 2>&1
    client_run "sudo rm -rf ${MOUNT}${PREFIX}" >/dev/null 2>&1
    ssh_run "$WRITER" "sudo rm -rf ${MOUNT}${PREFIX}" >/dev/null 2>&1
}
trap case_cleanup EXIT

echo "[1/6] prereqs: tc/iptables on $DNODE, set reader $READER fetch timeout=${TMO}s"
ssh_run "$DNODE" "command -v iptables >/dev/null || sudo iptables -L >/dev/null 2>&1" \
    || known_issue FUSE-HOL "iptables unavailable on $DNODE; cannot simulate a hung node"
ssh_run "$READER" "grep -q '^chunk_read_timeout_secs' $CFG && sudo sed -i 's/^chunk_read_timeout_secs = .*/chunk_read_timeout_secs = $TMO/' $CFG || sudo sed -i '/^\[client\]/a chunk_read_timeout_secs = $TMO' $CFG; sudo systemctl restart '$SVC'" \
    || skip "could not set reader fetch timeout"
CFG_SAVED=1
sleep 4

echo "[2/6] mandatory warm=2 cold=1 + write 2 cold fixtures via $WRITER"
mgr_post_leader /api/v1/volumes/$VOL/set-mandatory '{"warm_replicas":2,"cold_replicas":1}' >/dev/null; SAVED=1
ssh_run "$WRITER" "sudo mkdir -p ${MOUNT}${PREFIX} && head -c $((4*1024*1024)) /dev/urandom | sudo tee ${MOUNT}${PREFIX}A.bin >/dev/null && head -c $((4*1024*1024)) /dev/urandom | sudo tee ${MOUNT}${PREFIX}B.bin >/dev/null && sudo sync" || fail "write failed"
ORIG=$(ssh_run "$WRITER" "sudo sha256sum ${MOUNT}${PREFIX}B.bin | cut -d' ' -f1")
_two_replicas() { [ "$(mgr_get_any "/api/v1/files/inspect?path=${PREFIX}B.bin" | python3 -c "import json,sys;cs=json.load(sys.stdin).get('chunks',[]);print(len({r['node_id'] for c in cs for r in c.get('replicas',[])}))" 2>/dev/null)" -ge 2 ]; }
wait_for 60 "B replicated to 2 nodes" _two_replicas

echo "[3/6] warm the reader's channel to $DNODE (read A)"
ssh_run "$READER" "sudo cat ${MOUNT}${PREFIX}A.bin >/dev/null" || fail "warm read failed"

echo "[4/6] DROP $DNODE :9500 responses (established channel -> GetChunk hangs mid-stream)"
ssh_run "$DNODE" "sudo iptables -A OUTPUT -p tcp --sport 9500 -j DROP" || skip "could not apply iptables DROP"
DROPPED=1

echo "[5/6] cold-read B from $READER — must fall over within ~${TMO}s, not hang"
START=$(date +%s)
GOT=$(ssh_run "$READER" "timeout $((TMO*4)) sha256sum ${MOUNT}${PREFIX}B.bin 2>/dev/null | cut -d' ' -f1")
ELAPSED=$(( $(date +%s) - START ))
echo "  read elapsed=${ELAPSED}s sha=$GOT"

echo "[6/6] assert: completed within the timeout bound AND bytes correct (no hang, fell over)"
[ -n "$GOT" ] || fail "FUSE-HOL: read did not complete (hung past $((TMO*4))s) — fetch not bounded"
assert_eq "$ORIG" "$GOT" "read-after-fallover checksum"
[ "$ELAPSED" -le $((TMO*3)) ] || fail "read took ${ELAPSED}s (> ${TMO}s bound x3) — fallover not bounded by the fetch timeout"
echo "  hung-node read fell over to a healthy replica in ${ELAPSED}s with correct bytes"
pass

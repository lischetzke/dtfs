#!/usr/bin/env bash
# Bug N (endpoint fix) — deleting a file MUST physically reclaim its chunks
# on the data nodes via the manager's delete fan-out, not leak them to the
# orphan reconciler. Pre-fix the fan-out built its tonic endpoint as
# format!("http://{}", grpc_addr) on a grpc_addr that already carries the
# scheme (Bug W), producing http://http://ip:port — every DeleteChunk RPC
# failed with a transport error and every deleted file's chunks stayed on
# disk until the (24h-grace) orphan reaper. Observed live on production
# 2026-06-06: 119 of 123 deletes leaked.
source "$(dirname "$0")/../lib/common.sh"

TESTFILE="$MOUNT/bugn-fanout.bin"
SIZE=$((5 * 1024 * 1024)) # 5 MB -> 2 chunks at 4 MB

case_cleanup() {
    client_run "sudo rm -f '$TESTFILE'" >/dev/null 2>&1 || true
}
trap case_cleanup EXIT

echo "[1/4] write a 2-chunk file via FUSE"
client_run "head -c $SIZE /dev/urandom | sudo tee '$TESTFILE' >/dev/null && sync" \
    || fail "write failed"
sleep 2

echo "[2/4] resolve its chunk ids + confirm they exist on a data node"
CHUNKS=$(chunk_ids_for_path "/bugn-fanout.bin") || fail "files/inspect failed"
[ -n "$CHUNKS" ] || fail "no chunk ids for the test file"
FOUND_BEFORE=0
for c in $CHUNKS; do
    for h in $(data_list); do
        if host_has_chunk "$h" "$c"; then FOUND_BEFORE=$((FOUND_BEFORE+1)); break; fi
    done
done
echo "  chunks=$(echo "$CHUNKS" | wc -w) on-disk-before=$FOUND_BEFORE"
assert_ge "$FOUND_BEFORE" 1 "at least one chunk physically present before delete"

echo "[3/4] delete the file via FUSE"
client_run "sudo rm '$TESTFILE'" || fail "unlink failed"

chunk_gone_everywhere() {
    local cid="$1" h
    for h in $(data_list); do
        if host_has_chunk "$h" "$cid"; then return 1; fi
    done
    return 0
}

echo "[4/4] fan-out must reclaim every chunk from every data node within 30s"
for c in $CHUNKS; do
    wait_for 30 "chunk $c reclaimed (fan-out leak = Bug N)" chunk_gone_everywhere "$c"
done
pass

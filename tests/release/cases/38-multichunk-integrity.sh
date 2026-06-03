#!/usr/bin/env bash
# Bug BW — large-file under-chunking corruption. A write larger than the
# volume chunk_size MUST be split into ceil(size/chunk_size) chunks, and read
# back byte-exactly. Pre-fix the FUSE client chunked at a hardcoded 128 MB
# while file metadata recorded the 4 MB volume chunk size, so a >4 MB file was
# stored as one oversized chunk and reads past the first 4 MB returned garbage
# (a 12 MB file -> chunk_count=1; reads via sha256sum vs cp disagreed).
source "$(dirname "$0")/../lib/common.sh"

TESTFILE="$MOUNT/bw-multichunk.bin"
SIZE=$((12 * 1024 * 1024)) # 12 MB -> 3 chunks at 4 MB
LOCAL=/tmp/bw-src-$$.bin

case_cleanup() {
    client_run "sudo rm -f '$TESTFILE' /tmp/bw-src.bin" >/dev/null 2>&1 || true
    rm -f "$LOCAL" 2>/dev/null || true
}
trap case_cleanup EXIT

echo "[1/4] write a 12 MB file via the FUSE client"
head -c "$SIZE" /dev/urandom > "$LOCAL"
SRC=$(sha256sum "$LOCAL" | awk '{print $1}')
scp "${SSH_OPTS[@]}" "$LOCAL" "debian@$CLIENT:/tmp/bw-src.bin" >/dev/null 2>&1 \
    || known_issue BW "could not stage source to client"
client_run "sudo cp /tmp/bw-src.bin '$TESTFILE' && sync" \
    || fail "write of a multi-chunk file failed"

echo "[2/4] read back two ways; both must equal the source"
sleep 1
GOT1=$(sha256_via_fuse "$TESTFILE")
GOT2=$(client_run "sudo dd if='$TESTFILE' bs=1M 2>/dev/null | sha256sum | awk '{print \$1}'")
echo "  src=$SRC fuse=$GOT1 dd=$GOT2"
assert_eq "$SRC" "$GOT1" "sha256 via FUSE read"
assert_eq "$SRC" "$GOT2" "sha256 via dd (different read pattern)"

echo "[3/4] chunk_count must equal ceil(size/chunk_size)"
INSPECT=$(mgr_get_any "/api/v1/files/inspect?path=/bw-multichunk.bin") \
    || known_issue BW "files/inspect unreachable"
read -r CC CS <<<"$(echo "$INSPECT" | python3 -c "
import json,sys
d=json.load(sys.stdin)
print(d.get('chunk_count','NA'), d.get('chunk_size','NA'))
")"
echo "  chunk_count=$CC chunk_size=$CS"
case "$CS" in ''|*[!0-9]*) fail "bad chunk_size: $CS" ;; esac
EXPECT=$(( (SIZE + CS - 1) / CS ))
assert_eq "$EXPECT" "$CC" "chunk_count == ceil(size/chunk_size)"

echo "[4/4] integrity + chunk count correct"
pass

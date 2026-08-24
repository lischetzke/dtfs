#!/usr/bin/env bash
# REV-2-1 — a file whose chunk index space has a GAP must never serve a
# neighbouring chunk's bytes.
#
# The manager builds GetFileLocations from a BTreeMap keyed by chunk_index and
# emits only the VALUES, so the repeated field is DENSE. The FUSE client indexed
# it POSITIONALLY (`chunks[offset / chunk_size]`). Any gap therefore shifted
# every later chunk by one and the reader was handed a DIFFERENT chunk's bytes
# at a valid offset - checksum-clean, with no error logged anywhere, which is
# the worst failure mode this system has.
#
# Gaps are producible from the mount: truncate a file out to N chunks and then
# write only a later one. The fix labels each entry with its chunk_index and has
# the client resolve by index, serving zeros for a genuine hole.
source "$(dirname "$0")/../lib/common.sh"

F=""
case_cleanup() { rm_fuse_file "$F"; }
trap case_cleanup EXIT

CHUNK_MB=${REV76_CHUNK_MB:-4}
CHUNK=$((CHUNK_MB * 1024 * 1024))

echo "[1/4] create a sparse file: truncate past several chunks, write only the tail"
F="$MOUNT/release/76-sparse-$$.bin"
client_run "sudo mkdir -p $MOUNT/release" >/dev/null 2>&1 || true
# 6 chunks of logical size, data written only into the LAST one.
SIZE=$((CHUNK * 6))
OFF=$((CHUNK * 5))
client_run "sudo truncate -s $SIZE '$F'" || skip "truncate not supported on this mount"
# A recognisable, non-zero payload so a mis-resolved read is obvious.
client_run "sudo dd if=/dev/zero bs=1M count=$CHUNK_MB status=none | tr '\\0' 'Z' | sudo dd of='$F' bs=1M seek=$((OFF / 1024 / 1024)) conv=notrunc status=none && sudo sync" \
    || fail "could not write the tail chunk"
sleep 5

echo "[2/4] the file still reports its full logical size"
GOT_SIZE=$(client_run "sudo stat -c %s '$F'" | tr -d '\r')
assert_eq "$SIZE" "$GOT_SIZE" "logical size after truncate + tail write"

echo "[3/4] the HOLE reads as zeros, not as the tail chunk's bytes"
# Offset 0 has no chunk entry at all. Positional indexing resolved it to the
# only entry present - the tail - and served 'Z' bytes as the file header.
HEAD_NONZERO=$(client_run "sudo dd if='$F' bs=1M count=1 status=none 2>/dev/null | tr -d '\\0' | wc -c" | tr -d '\r')
if [ "${HEAD_NONZERO:-0}" -ne 0 ]; then
    fail "hole at offset 0 returned ${HEAD_NONZERO} non-zero bytes - a later chunk's data was served at the wrong offset"
fi
# Same at a middle hole.
MID_NONZERO=$(client_run "sudo dd if='$F' bs=1M count=1 skip=$((CHUNK_MB * 2)) status=none 2>/dev/null | tr -d '\\0' | wc -c" | tr -d '\r')
if [ "${MID_NONZERO:-0}" -ne 0 ]; then
    fail "hole at chunk 2 returned ${MID_NONZERO} non-zero bytes - chunk aliasing"
fi

echo "[4/4] the written chunk reads back at ITS OWN offset"
TAIL_Z=$(client_run "sudo dd if='$F' bs=1M count=1 skip=$((OFF / 1024 / 1024)) status=none 2>/dev/null | tr -cd 'Z' | wc -c" | tr -d '\r')
[ "${TAIL_Z:-0}" -gt 0 ] \
    || fail "the tail chunk's own offset returned no payload - the write or the index resolution is wrong"
echo "  hole reads zeros, tail reads its payload"
pass

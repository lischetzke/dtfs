#!/usr/bin/env bash
# REV-1-6 — a write whose buffer is not chunk-aligned must not lose bytes.
#
# WriteBuffer::extract_complete_chunks' mid-chunk branch starts `pos` at
# bytes_to_complete_first, emits only the whole chunks after that point, then
# drains from the FRONT up to `pos` - discarding the leading partial region it
# never turned into a segment, and advancing dirty_offset past it so nothing can
# recover it. Every non-chunk-aligned flush silently loses up to chunk_size-1
# bytes of an ACKED write. The unit-level counterpart is
# writebuf::audit_co_tests::co_1_7_midchunk_flush_does_not_drop_leading_partial_bytes.
#
# Shape of the probe: write a file through the mount in deliberately unaligned
# pieces, then read it back and compare against the local reference. A lost
# leading region shows up as a checksum mismatch, and cmp reports where.
source "$(dirname "$0")/../lib/common.sh"

CHUNK_MB=${REV78_CHUNK_MB:-4}
CHUNK=$((CHUNK_MB * 1024 * 1024))

F="$MOUNT/release/78-unaligned-$$.bin"
LOCAL="/tmp/78-unaligned-$$.bin"
case_cleanup() {
    client_run "sudo rm -f '$F' '$LOCAL' '$LOCAL.got'" >/dev/null 2>&1 || true
}
trap case_cleanup EXIT

echo "[1/4] build a deterministic reference file on the client"
# 3 chunks + a tail, so several flushes happen and at least one starts mid-chunk.
SIZE=$((CHUNK * 3 + CHUNK / 3))
client_run "sudo mkdir -p $MOUNT/release && head -c $SIZE /dev/urandom > '$LOCAL'" \
    || fail "could not build the reference file"
SRC=$(client_run "sha256sum '$LOCAL' | awk '{print \$1}'" | tr -d '\r')
[ -n "$SRC" ] || fail "reference checksum empty"

echo "[2/4] copy it onto the mount in UNALIGNED pieces"
# A block size that is not a divisor of the chunk size guarantees the write
# buffer's dirty_offset lands mid-chunk on most flushes.
BS=$((1024 * 1000))   # 1,024,000 - deliberately not a power of two
client_run "sudo dd if='$LOCAL' of='$F' bs=$BS conv=notrunc status=none && sudo sync" \
    || fail "unaligned copy failed"
sleep 5

echo "[3/4] read it back and compare"
GOT=$(sha256_via_fuse "$F")
if [ "$GOT" != "$SRC" ]; then
    client_run "sudo dd if='$F' bs=1M status=none > '$LOCAL.got' 2>/dev/null" || true
    echo "  first difference:"
    client_run "cmp '$LOCAL' '$LOCAL.got' 2>&1 | head -3" || true
    client_run "ls -l '$LOCAL' '$LOCAL.got' 2>/dev/null" || true
    fail "unaligned write lost or altered data (expected $SRC, got $GOT)"
fi

echo "[4/4] an unaligned mid-file overwrite also round-trips"
# Rewrite a chunk-and-a-bit starting at an offset that is NOT a chunk multiple -
# the shape that skips the read-modify-write seed and hits the mid-chunk branch.
OFF=$((CHUNK + CHUNK / 2))
LEN=$((CHUNK + 12345))
client_run "sudo dd if=/dev/urandom of='$LOCAL' bs=1 seek=$OFF count=$LEN conv=notrunc status=none" \
    || fail "could not patch the reference"
client_run "sudo dd if='$LOCAL' of='$F' bs=1 skip=$OFF seek=$OFF count=$LEN conv=notrunc status=none && sudo sync" \
    || fail "could not patch through the mount"
sleep 5
SRC2=$(client_run "sha256sum '$LOCAL' | awk '{print \$1}'" | tr -d '\r')
assert_eq "$SRC2" "$(sha256_via_fuse "$F")" "unaligned mid-file overwrite"
pass

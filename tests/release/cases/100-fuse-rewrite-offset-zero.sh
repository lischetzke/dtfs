#!/usr/bin/env bash
# CR-1-1/CR-1-2 (FUSE-1/FUSE-7) — a patch write at offset 0 (or straddling a
# chunk boundary) after a file has already been flushed to the cluster must
# NEVER re-emit the already-committed chunks as zero-filled data. Pre-fix,
# FileWriteBuffer was one contiguous Vec<u8>: a write below the flushed
# dirty_offset zero-filled the gap up to the new write, and the next flush
# re-committed that gap (the already-flushed chunks) as all-zero bytes with
# a valid checksum — silent destruction of committed data. The per-chunk
# BTreeMap<u32, ChunkBuf> buffer (CR-1-1) can only ever touch the chunk(s)
# a write actually overlaps, and CR-1-2 seeds a partially-covered chunk
# (including index 0) from the cluster's existing bytes before the flush
# commits it.
#
# Shape of the probe: write a multi-chunk file, sync (flush to the
# cluster), then `dd conv=notrunc` a small patch at offset 0 AND another
# patch straddling the boundary between chunk 0 and chunk 1. Apply the
# identical patches to a local reference file and cmp byte-for-byte.
#
# The unit-level counterparts are
# writebuf::tests::cr_1_1_rewrite_offset_zero_marks_only_that_chunk_needs_seed
# and filesystem::cr_1_2_seed_tests::offset_zero_is_flagged_for_seeding.
source "$(dirname "$0")/../lib/common.sh"

CHUNK_MB=${CR1_CHUNK_MB:-4}
CHUNK=$((CHUNK_MB * 1024 * 1024))

F="$MOUNT/release/100-rewrite0-$$.bin"
LOCAL="/tmp/100-rewrite0-$$.bin"
GOT="/tmp/100-rewrite0-got-$$.bin"
case_cleanup() {
    client_run "sudo rm -f '$F' '$LOCAL' '$GOT'" >/dev/null 2>&1 || true
}
trap case_cleanup EXIT

echo "[1/5] build a 3-chunk reference file and copy it onto the mount"
SIZE=$((CHUNK * 3))
client_run "sudo mkdir -p $MOUNT/release && head -c $SIZE /dev/urandom > '$LOCAL'" \
    || fail "could not build the reference file"
client_run "sudo cp '$LOCAL' '$F' && sudo sync" || fail "initial write through the mount failed"
sleep 5

SRC=$(client_run "sha256sum '$LOCAL' | awk '{print \$1}'" | tr -d '\r')
GOT_CS=$(sha256_via_fuse "$F")
assert_eq "$SRC" "$GOT_CS" "sha256 after initial write (sanity check before patching)"

echo "[2/5] patch 1 KB at offset 0 on BOTH the reference and the mounted file"
client_run "sudo dd if=/dev/urandom of='$LOCAL' bs=1 seek=0 count=1024 conv=notrunc status=none" \
    || fail "could not patch the local reference at offset 0"
client_run "sudo dd if='$LOCAL' of='$F' bs=1 skip=0 seek=0 count=1024 conv=notrunc status=none && sudo sync" \
    || fail "patch at offset 0 through the mount failed"

echo "[3/5] patch 1 KB straddling the chunk-0/chunk-1 boundary on both"
OFF=$((CHUNK - 512))
client_run "sudo dd if=/dev/urandom of='$LOCAL' bs=1 seek=$OFF count=1024 conv=notrunc status=none" \
    || fail "could not patch the local reference at the boundary"
client_run "sudo dd if='$LOCAL' of='$F' bs=1 skip=$OFF seek=$OFF count=1024 conv=notrunc status=none && sudo sync" \
    || fail "boundary patch through the mount failed"
sleep 5

echo "[4/5] the mounted file's checksum must match the patched reference"
SRC2=$(client_run "sha256sum '$LOCAL' | awk '{print \$1}'" | tr -d '\r')
GOT2=$(sha256_via_fuse "$F")
if [ "$SRC2" != "$GOT2" ]; then
    client_run "sudo dd if='$F' bs=1M status=none > '$GOT' 2>/dev/null" || true
    echo "  first difference:"
    client_run "cmp '$LOCAL' '$GOT' 2>&1 | head -3" || true
    fail "rewrite at offset 0 / chunk boundary corrupted the file (expected $SRC2, got $GOT2)"
fi

echo "[5/5] size unchanged by the in-place patches"
MSIZE=$(client_run "stat -c %s '$F'" | tr -d '\r')
assert_eq "$SIZE" "$MSIZE" "file size after in-place patches"

pass

#!/usr/bin/env bash
# CR-1-3/CR-1-4 (FUSE-2/FUSE-3) — a reader closing a file (kernel FUSE flush
# on close(2), including a read-only open) must NEVER touch the write-path
# buffer of another fh still streaming into the same path. Pre-fix, flush()
# had no is_writable() check, so a reader's close forced a mid-stream commit
# of the writer's partial tail, and release() unconditionally removed the
# path-keyed write buffer — dropping bytes the writer had buffered (or that
# CO-1-8's restore_segments had just put back) with no error surfaced to the
# writer. The unit-level counterparts are
# filesystem::cr_1_3_flush_release_gating_tests::* and
# filesystem::cr_1_4_ghost_unlink_tests::*.
#
# Shape of the probe: start a large streaming write on the mount, then while
# it is still in flight, open the SAME path read-only and close it (cat ...
# > /dev/null) several times. The writer must complete with the full,
# byte-correct content — a dropped buffer or truncated tail shows up as a
# checksum mismatch or a short file.
source "$(dirname "$0")/../lib/common.sh"

SIZE_MB=${CR101_SIZE_MB:-64}
SIZE=$((SIZE_MB * 1024 * 1024))

F="$MOUNT/release/101-writer-$$.bin"
LOCAL="/tmp/101-writer-$$.bin"
case_cleanup() {
    client_run "sudo pkill -f 'of=$F' " >/dev/null 2>&1 || true
    client_run "sudo rm -f '$F' '$LOCAL'" >/dev/null 2>&1 || true
}
trap case_cleanup EXIT

echo "[1/4] build a deterministic ${SIZE_MB} MB reference file"
client_run "sudo mkdir -p $MOUNT/release && head -c $SIZE /dev/urandom > '$LOCAL'" \
    || fail "could not build the reference file"
SRC=$(client_run "sha256sum '$LOCAL' | awk '{print \$1}'" | tr -d '\r')
[ -n "$SRC" ] || fail "reference checksum empty"

echo "[2/4] start a slow-ish streaming write on the mount, in the background"
# pv paces the write so the reader has a real window to race the writer;
# fall back to a plain dd + sleep loop if pv is not installed on the client.
if client_run "command -v pv >/dev/null 2>&1"; then
    client_run "sudo sh -c 'pv -L 8m \"$LOCAL\" > \"$F\"' >/dev/null 2>&1 &" \
        || fail "could not start the paced background write"
else
    client_run "sudo sh -c 'dd if=\"$LOCAL\" of=\"$F\" bs=1M conv=notrunc status=none' >/dev/null 2>&1 &" \
        || fail "could not start the background write"
fi
sleep 2

echo "[3/4] repeatedly open-read-close the SAME path read-only while the write is in flight"
for i in 1 2 3 4 5; do
    client_run "sudo cat '$F' > /dev/null 2>&1" || true
    sleep 1
done

echo "[4/4] the writer must finish with the complete, byte-correct content"
wait_for 90 "background write to finish" bash -c \
    "! ssh ${SSH_OPTS[*]} debian@$CLIENT 'pgrep -f \"of=$F\" >/dev/null 2>&1' " \
    || fail "background write did not finish in time (may indicate it stalled or was aborted)"
client_run "sudo sync"
sleep 2

MSIZE=$(client_run "stat -c %s '$F' 2>/dev/null" | tr -d '\r')
assert_eq "$SIZE" "${MSIZE:-0}" "file size after concurrent reader closes"

GOT=$(sha256_via_fuse "$F")
assert_eq "$SRC" "$GOT" "sha256 after concurrent reader closes (writer's buffer must survive readers' releases)"

pass

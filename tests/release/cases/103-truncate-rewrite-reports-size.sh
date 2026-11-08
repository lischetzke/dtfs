#!/usr/bin/env bash
# CR-2-3 (RAFT-3) — a truncate followed by a rewrite must report the
# rewritten size, not stay pinned at the truncated size forever.
#
# Pre-fix, SetFileSize set `logical_size = Some(new_size)` and nothing
# ever revisited it: UpdateChunkLocations only merged chunks. So
# `open(O_TRUNC)` on an existing file (cp, rsync --inplace, shell `>`,
# SQLite journal recycle) followed by writing new data committed chunks
# past the truncated size, but every reader (including this client after
# cache expiry) kept seeing the STALE truncated size (usually 0) forever.
# The unit-level regression is
# state_machine::posix_path_regression::raft_3_truncate_zero_then_commit_grows_logical_size.
#
# Shape of the probe: write a multi-MB file, truncate it to 0 (shell `>`),
# then write fresh data past the truncated size, and assert `stat -c %s`
# reports the REWRITTEN size, not 0 and not the pre-truncate size.
source "$(dirname "$0")/../lib/common.sh"

F="$MOUNT/release/103-trunc-$$.bin"

case_cleanup() {
    client_run "sudo rm -f '$F'" >/dev/null 2>&1 || true
}
trap case_cleanup EXIT

echo "[1/5] write an initial 8 MiB file"
client_run "sudo mkdir -p $MOUNT/release && sudo dd if=/dev/urandom of='$F' bs=1M count=8 status=none && sudo sync" \
    || fail "initial write failed"
sleep 3
SIZE0=$(client_run "stat -c %s '$F'" | tr -d '\r')
assert_eq "8388608" "$SIZE0" "size after initial 8 MiB write"

echo "[2/5] truncate to 0 (O_TRUNC, e.g. shell '>')"
client_run "sudo sh -c ': > \"$F\"' && sudo sync" || fail "truncate failed"
sleep 3
SIZE1=$(client_run "stat -c %s '$F'" | tr -d '\r')
assert_eq "0" "$SIZE1" "size immediately after truncate"

echo "[3/5] rewrite 3 MiB of fresh data past the truncated size"
client_run "sudo dd if=/dev/urandom of='$F' bs=1M count=3 status=none && sudo sync" \
    || fail "rewrite after truncate failed"
sleep 3

echo "[4/5] reported size must reflect the rewrite, not stay pinned at 0"
SIZE2=$(client_run "stat -c %s '$F'" | tr -d '\r')
assert_eq "3145728" "$SIZE2" "size after truncate-then-rewrite (RAFT-3: was stuck at 0 pre-fix)"

echo "[5/5] a fresh lookup (simulating cache expiry / another client) agrees"
client_run "sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'" >/dev/null 2>&1 || true
SIZE3=$(client_run "stat -c %s '$F'" | tr -d '\r')
assert_eq "3145728" "$SIZE3" "size after cache drop agrees with the rewrite"

pass

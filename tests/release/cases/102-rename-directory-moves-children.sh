#!/usr/bin/env bash
# CR-2-2 (RAFT-2) — renaming a directory must move every child with it,
# not just the exact directory-marker key.
#
# Pre-fix, RenameFile's apply moved only the exact `from` key; a
# directory's children (stored under `from/...` keys) stayed stranded
# under the old prefix, so `mv /Movies/Old /Movies/New` left the new
# directory empty while the old one (still visible as a synthetic dir)
# kept the files — a routine Plex/Sonarr library reorganisation silently
# "lost" every file in the moved directory. The unit-level regression is
# state_machine::posix_path_regression::raft_2_rename_directory_moves_children.
#
# Shape of the probe: mkdir a directory with a nested child, `mv` it to a
# new path through the FUSE mount, and assert the files (and their
# content) are reachable under the NEW path and gone from the OLD path.
source "$(dirname "$0")/../lib/common.sh"

OLD="$MOUNT/release/102-old-$$"
NEW="$MOUNT/release/102-new-$$"

case_cleanup() {
    client_run "sudo rm -rf '$OLD' '$NEW'" >/dev/null 2>&1 || true
}
trap case_cleanup EXIT

echo "[1/5] build a directory with a nested child under the mount"
client_run "sudo mkdir -p $MOUNT/release && sudo mkdir -p '$OLD/sub' && \
    echo 'top-level-content' | sudo tee '$OLD/top.txt' >/dev/null && \
    echo 'nested-content' | sudo tee '$OLD/sub/nested.txt' >/dev/null && sudo sync" \
    || fail "could not build the source tree"
sleep 3

echo "[2/5] sanity: both files are visible before the rename"
client_run "sudo test -f '$OLD/top.txt'" || fail "top.txt missing before rename"
client_run "sudo test -f '$OLD/sub/nested.txt'" || fail "sub/nested.txt missing before rename"

echo "[3/5] rename the directory"
client_run "sudo mv '$OLD' '$NEW'" || fail "mv through the mount failed"
sleep 3

echo "[4/5] children must be reachable under the NEW path with intact content"
client_run "sudo test -f '$NEW/top.txt'" \
    || fail "top.txt not found under the renamed directory (RAFT-2 regression)"
client_run "sudo test -f '$NEW/sub/nested.txt'" \
    || fail "sub/nested.txt not found under the renamed directory (RAFT-2 regression)"
TOP=$(client_run "sudo cat '$NEW/top.txt'" | tr -d '\r\n')
NESTED=$(client_run "sudo cat '$NEW/sub/nested.txt'" | tr -d '\r\n')
assert_eq "top-level-content" "$TOP" "top.txt content survives the rename"
assert_eq "nested-content" "$NESTED" "nested.txt content survives the rename"

echo "[5/5] the OLD path must be gone entirely"
if client_run "sudo test -e '$OLD'" >/dev/null 2>&1; then
    fail "old directory '$OLD' still exists after rename (children stranded, RAFT-2 regression)"
fi

pass

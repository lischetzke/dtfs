# tests/release/lib/fixtures.sh — test-data fixtures for the E2E suite.
# Sourced by lib/common.sh, so every case has these after sourcing
# common.sh. Depends on the helpers defined in common.sh
# (client_run, sha256_via_fuse, MOUNT).
#
# Note: make_fuse_file echoes the path so it is meant to be used in a
# command substitution (F=$(make_fuse_file ...)). That runs in a
# subshell, so fixtures CANNOT auto-register themselves for cleanup —
# the case must remove what it created via rm_fuse_file in its
# case_cleanup trap.

# Create a random file of <size-MB> at <relpath> under the FUSE mount
# and echo the absolute path.
#   F=$(make_fuse_file "release/01-basic-$$.bin" 4)
make_fuse_file() {
    local rel="$1" mb="$2"
    local path="$MOUNT/$rel"
    client_run "sudo mkdir -p \"\$(dirname '$path')\" && \
                sudo dd if=/dev/urandom of='$path' bs=1M count=$mb status=none && \
                sudo sync"
    echo "$path"
}

# A unique, case-scoped fixture path under the mount (not created).
#   F=$(fixture_path basic)   # -> /mnt/dtfs/release/basic-<pid>
fixture_path() {
    echo "$MOUNT/release/${1:-fixture}-$$"
}

# Drop the kernel page cache on the client so the next read re-fetches
# from a data node rather than serving cached bytes.
drop_client_cache() {
    client_run "sudo sh -c 'sync && echo 3 > /proc/sys/vm/drop_caches'" \
        >/dev/null 2>&1 || true
}

# sha256 of a fixture (via the FUSE mount on the client).
fixture_sha256() { sha256_via_fuse "$1"; }

# Remove one or more fixture paths from the mount (best effort, never
# fails). Call from a case_cleanup trap with the paths the case made.
#   rm_fuse_file "$F" "$G"
rm_fuse_file() {
    local f
    for f in "$@"; do
        [ -n "$f" ] || continue
        client_run "sudo rm -f '$f'" >/dev/null 2>&1 || true
    done
}

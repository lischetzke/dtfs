#!/usr/bin/env bash
# CO-4-16: the FUSE adapter must return ENOENT ONLY for a genuine cluster
# NotFound, and EIO for transient errors (manager outage / timeout). The
# old code mapped EVERY cluster error to ENOENT, so a manager blip made Plex
# see "file does not exist" and drop the library entry.
#
# Live, non-destructive direction: a stat of a path that genuinely does not
# exist must report ENOENT (errno 2) over the FUSE mount. This guards the
# NotFound -> ENOENT half and proves the helper did not over-map everything
# to EIO. The transient -> EIO half is exercised by the unit tests in
# crates/dtfs-fuse/src/fuse_adapter.rs (cluster_err_to_errno), which can drive
# a tonic::Status::unavailable directly without taking a manager down on a
# live cluster.
source "$(dirname "$0")/../lib/common.sh"

case_cleanup() { :; }
trap case_cleanup EXIT

# A path that does not exist under the mount.
MISSING="$MOUNT/co-4-16-does-not-exist-$$.nope"

echo "[1/1] stat a genuinely-missing path -> must be ENOENT (errno 2)"
# `stat` prints the errno-bearing message; use python's os.stat to get the
# numeric errno deterministically across coreutils versions.
ERRNO=$(client_run "python3 -c \"
import os, sys
try:
    os.stat('$MISSING')
    print('0')
except OSError as e:
    print(e.errno)
\"") || fail "could not run stat probe on $CLIENT"
ERRNO=$(echo "$ERRNO" | tr -d '[:space:]')
echo "  errno=$ERRNO (2=ENOENT, 5=EIO)"

[ "$ERRNO" = "2" ] || fail "missing path reported errno $ERRNO, expected 2 (ENOENT)"

pass

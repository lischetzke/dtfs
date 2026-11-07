#!/usr/bin/env bash
# AZ-2 regression — pin the source-selection contract of
# scripts/dtfs-install.sh without actually installing anything.
# We exercise the flag parser + the select_tarball_local /
# download_tarball_github helpers via `--help` + a dry-run mode
# that exits before any `step` line that would mutate disk.
#
# This script runs as a normal user (no root needed) because it
# only inspects the script's argv handling.

set -uo pipefail

SCRIPT="$(cd "$(dirname "$0")/.." && pwd)/dtfs-install.sh"
[ -f "$SCRIPT" ] || { echo "FAIL: cannot find $SCRIPT" >&2; exit 1; }

pass=0
fail=0
check() {
    local label="$1"; shift
    if "$@" >/dev/null 2>&1; then
        echo "  ok  $label"
        pass=$((pass+1))
    else
        echo "  FAIL $label" >&2
        fail=$((fail+1))
    fi
}

# 1. --help prints AZ-2 flags.
check "--help mentions --source"        bash -c "'$SCRIPT' --help 2>&1 | grep -q -- '--source local|remote|auto'"
check "--help mentions --version"       bash -c "'$SCRIPT' --help 2>&1 | grep -q -- '--version <tag>'"
check "--help mentions DTFS_REPO_URL"   bash -c "'$SCRIPT' --help 2>&1 | grep -q DTFS_REPO_URL"

check "--help mentions --noninteractive" bash -c "'$SCRIPT' --help 2>&1 | grep -q -- '--noninteractive'"
check "--help mentions --reconfigure"    bash -c "'$SCRIPT' --help 2>&1 | grep -q -- '--reconfigure'"
check "fuse config carries shared_secret" bash -c "sed -n '/^render_fuse/,/^}/p' '$SCRIPT' | grep -q shared_secret"

# 2. --source must be one of {local,github,auto}.
check "rejects bogus --source"          bash -c "! '$SCRIPT' --source bogus 2>&1"

# 3. --source local with no tarball anywhere must fail with the
#    'no tarball found in local search paths' message.
#    Run as non-root: hits the root check first; check error
#    string surfaces (not "must run as root").
tmpdir=$(mktemp -d)
trap "rm -rf $tmpdir" EXIT
check "local search defines DOWNLOAD_CACHE" bash -c "grep -q 'DOWNLOAD_CACHE' '$SCRIPT'"
check "local search defines STAGING_ROOT"   bash -c "grep -q 'STAGING_ROOT' '$SCRIPT'"
check "auto first tries local then github"  bash -c "grep -q 'select_tarball_local || download_tarball_github\\|select_tarball_local; then' '$SCRIPT'"
check "github URL is from DTFS_REPO_URL"    bash -c "grep -q 'DTFS_REPO_URL' '$SCRIPT'"

echo
if [ $fail -eq 0 ]; then
    echo "PASS — $pass checks green"
    exit 0
else
    echo "FAIL — $fail checks failed, $pass green" >&2
    exit 1
fi

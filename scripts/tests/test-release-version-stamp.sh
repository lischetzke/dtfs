#!/usr/bin/env bash
#
# AZ-5 — guard the DTFS_VERSION release stamp (AGENTS.md § A.3.5).
#
# Proves:
#   1. Unset DTFS_VERSION -> `dtfs-cli --version` reports v0.0.0-dev
#      (dev/incremental builds unchanged).
#   2. DTFS_VERSION=<tag> -> `dtfs-cli --version` reports <tag>
#      (release wrapper stamps the real version; build.rs declares
#      rerun-if-env-changed=DTFS_VERSION so the toggle re-stamps).
#
# Build-host check (rebuilds dtfs-cli, the smallest binary); not a
# live-cluster case, because the stamp is a build-time property.
#
# Usage:  scripts/tests/test-release-version-stamp.sh
# Exit:   0 PASS, 1 FAIL.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

# Build dtfs-cli (release) with the given DTFS_VERSION ("" = unset) and
# print its `--version` line.
ver_for() {
    local v="$1"
    if [ -n "$v" ]; then
        DTFS_VERSION="$v" cargo build --release -p dtfs-cli >/dev/null 2>&1 || { echo "BUILD_FAILED"; return 1; }
    else
        cargo build --release -p dtfs-cli >/dev/null 2>&1 || { echo "BUILD_FAILED"; return 1; }
    fi
    ./target/release/dtfs-cli --version 2>/dev/null
}

echo "[1/2] build with DTFS_VERSION unset"
DEFAULT="$(ver_for "")"; echo "  $DEFAULT"
echo "[2/2] build with DTFS_VERSION=v9.9.9-guard"
STAMPED="$(ver_for "v9.9.9-guard")"; echo "  $STAMPED"

FAIL=0
case "$DEFAULT" in
    "dtfs-cli v0.0.0-dev "*) ;;
    *) echo "FAIL: default build is not v0.0.0-dev: '$DEFAULT'"; FAIL=1 ;;
esac
case "$STAMPED" in
    "dtfs-cli v9.9.9-guard "*) ;;
    *) echo "FAIL: DTFS_VERSION did not stamp: '$STAMPED'"; FAIL=1 ;;
esac

# Leave the tree at the dev default so a stamped binary isn't cached.
ver_for "" >/dev/null 2>&1 || true

if [ "$FAIL" -eq 0 ]; then
    echo "PASS: DTFS_VERSION stamps the binary; unset stays v0.0.0-dev"
    exit 0
fi
exit 1

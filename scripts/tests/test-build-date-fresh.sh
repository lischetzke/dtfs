#!/usr/bin/env bash
#
# Bug BS — guard the dev BUILD_DATE freshness invariant (CLAUDE.md §A.3.5).
#
# Proves two properties of the SOURCE_DATE_EPOCH-driven stamp:
#   1. Distinct instants -> distinct BUILD_DATE (the property that broke when
#      build.rs gated re-runs on `rerun-if-changed=build.rs`, freezing the
#      stamp at the first-ever build).
#   2. A fixed SOURCE_DATE_EPOCH reproduces a fixed BUILD_DATE (the sanctioned
#      reproducible-build exception still works).
#
# Build-host check (rebuilds dtfs-cli, the smallest binary, three times); not a
# live-cluster release case, because BUILD_DATE is a build-time property that
# cannot be observed against a running cluster without rebuilding.
#
# Usage:  scripts/tests/test-build-date-fresh.sh
# Exit:   0 PASS, 1 FAIL.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

EPOCH_A=1000000000   # 2001-09-09 01:46:40 UTC -> 20010909-014640
EPOCH_B=1000000060   # +60s                    -> 20010909-014740

# Build dtfs-cli with a pinned SOURCE_DATE_EPOCH and print its stamp.
stamp_for() {
    local epoch="$1"
    SOURCE_DATE_EPOCH="$epoch" cargo build --release -p dtfs-cli >/dev/null 2>&1 || {
        echo "BUILD_FAILED"; return 1
    }
    ./target/release/dtfs-cli --version 2>/dev/null | grep -oE '[0-9]{8}-[0-9]{6}' | head -1
}

echo "[1/3] build @ EPOCH_A=$EPOCH_A"
S_A1=$(stamp_for "$EPOCH_A"); echo "  stamp=$S_A1"
echo "[2/3] build @ EPOCH_B=$EPOCH_B"
S_B=$(stamp_for "$EPOCH_B"); echo "  stamp=$S_B"
echo "[3/3] re-build @ EPOCH_A=$EPOCH_A (reproducibility)"
S_A2=$(stamp_for "$EPOCH_A"); echo "  stamp=$S_A2"

FAIL=0
[ -n "$S_A1" ] && [ -n "$S_B" ] && [ -n "$S_A2" ] || { echo "FAIL: empty stamp (build or --version failed)"; FAIL=1; }
if [ "$S_A1" = "$S_B" ]; then
    echo "FAIL: distinct instants produced the same BUILD_DATE ($S_A1) — stamp is frozen"; FAIL=1
fi
if [ "$S_A1" != "$S_A2" ]; then
    echo "FAIL: fixed SOURCE_DATE_EPOCH was not reproducible ($S_A1 != $S_A2)"; FAIL=1
fi
# Expected exact values (sanity on the formatter).
if [ "$S_A1" != "20010909-014640" ] || [ "$S_B" != "20010909-014740" ]; then
    echo "FAIL: unexpected stamp values (A=$S_A1 B=$S_B; want 20010909-014640 / 20010909-014740)"; FAIL=1
fi

if [ "$FAIL" -eq 0 ]; then
    echo "PASS: BUILD_DATE is fresh per-instant and reproducible under a pinned epoch"
    exit 0
fi
exit 1

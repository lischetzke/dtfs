#!/usr/bin/env bash
# Guard for scripts/ci/version.sh: the commit-subject -> (MODE, VERSION)
# contract the release workflow relies on.
set -u
SCRIPT="$(cd "$(dirname "$0")/.." && pwd)/ci/version.sh"
PASS=0; FAIL=0

check() {
    local label=$1 subject=$2 want=$3
    local got
    got="$(bash "$SCRIPT" "$subject" 2>/dev/null | tr '\n' ' ' | sed 's/ $//')"
    if [ "$got" = "$want" ]; then
        echo "  PASS  $label"; PASS=$((PASS+1))
    else
        echo "  FAIL  $label: want '$want' got '$got'"; FAIL=$((FAIL+1))
    fi
}

check_fails() {
    local label=$1 subject=$2
    if bash "$SCRIPT" "$subject" >/dev/null 2>&1; then
        echo "  FAIL  $label (should have failed)"; FAIL=$((FAIL+1))
    else
        echo "  PASS  $label"; PASS=$((PASS+1))
    fi
}

check "release with v"             "release v0.2.0"          "MODE=release VERSION=v0.2.0"
check "release without v"          "release 0.2.0"           "MODE=release VERSION=v0.2.0"
check "release with trailing text" "release v0.2.0: fixes"   "MODE=release VERSION=v0.2.0"
check "release keyword uppercase"  "Release v1.0.0"          "MODE=release VERSION=v1.0.0"
check "release:v0.2.0 (no space)"  "release:v0.2.0"          "MODE=release VERSION=v0.2.0"
check "plain commit"               "bug: fix the heartbeat"  "MODE=none"
check "deploy is no longer a keyword" "deploy the fix"       "MODE=none"
check "release mid-sentence is not a keyword" "docs: release notes" "MODE=none"
check "releases is not a keyword"  "releases page fix"       "MODE=none"
check "empty subject"              ""                        "MODE=none"
check_fails "release without version"    "release"
check_fails "release with short version" "release v1.2"

echo ""
echo "Results: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]

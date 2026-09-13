#!/usr/bin/env bash
# Guard for scripts/ci/version.sh: the commit-subject -> (MODE, VERSION)
# contract the GitLab pipeline relies on.
set -u
SCRIPT="$(cd "$(dirname "$0")/.." && pwd)/ci/version.sh"
PASS=0; FAIL=0

check() {
    local label=$1 subject=$2 base=$3 stamp=$4 want=$5
    local got
    got="$(bash "$SCRIPT" "$subject" "$base" "$stamp" 2>/dev/null | tr '\n' ' ' | sed 's/ $//')"
    if [ "$got" = "$want" ]; then
        echo "  PASS  $label"; PASS=$((PASS+1))
    else
        echo "  FAIL  $label: want '$want' got '$got'"; FAIL=$((FAIL+1))
    fi
}

check_fails() {
    local label=$1 subject=$2
    if bash "$SCRIPT" "$subject" v0.1.0 202601010000 >/dev/null 2>&1; then
        echo "  FAIL  $label (should have failed)"; FAIL=$((FAIL+1))
    else
        echo "  PASS  $label"; PASS=$((PASS+1))
    fi
}

check "release with v"            "release v0.2.0"                 v0.1.9 202609131530 "MODE=release VERSION=v0.2.0"
check "release without v"         "release 0.2.0"                  v0.1.9 202609131530 "MODE=release VERSION=v0.2.0"
check "release with trailing text" "release v0.2.0: fixes"         v0.1.9 202609131530 "MODE=release VERSION=v0.2.0"
check "release keyword uppercase" "Release v1.0.0"                 v0.1.9 202609131530 "MODE=release VERSION=v1.0.0"
check "deploy alpha"              "deploy the heartbeat fix"       v0.1.9 202609131530 "MODE=deploy VERSION=v0.1.9_202609131530"
check "deploy with colon"         "deploy: heartbeat fix"          v0.1.9 202609131530 "MODE=deploy VERSION=v0.1.9_202609131530"
check "plain commit"              "bug: fix the heartbeat"         v0.1.9 202609131530 "MODE=none VERSION=v0.1.9_202609131530"
check "deploy mid-sentence is not a keyword" "docs: describe deploy" v0.1.9 202609131530 "MODE=none VERSION=v0.1.9_202609131530"
check "release mid-sentence is not a keyword" "docs: release notes"  v0.1.9 202609131530 "MODE=none VERSION=v0.1.9_202609131530"
check "alpha base tag is normalised" "deploy"                       v0.1.9_202609010900 202609131530 "MODE=deploy VERSION=v0.1.9_202609131530"
check "no base tag"               "deploy"                         "" 202609131530 "MODE=deploy VERSION=v0.0.0_202609131530"
check_fails "release without version"  "release"
check_fails "release with short version" "release v1.2"

echo ""
echo "Results: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]

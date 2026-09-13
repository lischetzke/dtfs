#!/usr/bin/env bash
# CI version + deploy-mode resolver (pure; no git, no network).
#
# Reads the commit SUBJECT (first line of the message) and decides what
# the pipeline does with this commit:
#
#   "release v1.2.3 ..."   -> MODE=release VERSION=v1.2.3   (full release)
#   "release 1.2.3 ..."    -> same (the leading v is optional)
#   "deploy ..."           -> MODE=deploy  VERSION=<base>_<stamp>  (alpha)
#   anything else          -> MODE=none    VERSION=<base>_<stamp>  (no deploy)
#
# The keyword must be the FIRST word of the subject so an ordinary commit
# whose text merely mentions "deploy" never ships a build. <base> is the
# last full release tag (vX.Y.Z), <stamp> is UTC YYYYMMDDHHmm.
#
# Usage:
#   scripts/ci/version.sh "<subject>" <base-tag> <stamp>
#   scripts/ci/version.sh "release v0.2.0: notes" v0.1.9 202609131530
#     -> MODE=release
#        VERSION=v0.2.0
#
# Output: two KEY=VALUE lines (dotenv shape). Exit 3 on a malformed
# release version (e.g. "release v1.2").

set -euo pipefail

SUBJECT="${1:-}"
BASE="${2:-v0.0.0}"
STAMP="${3:-}"

if [ -z "$STAMP" ]; then
    STAMP="$(date -u +%Y%m%d%H%M)"
fi

# Normalise the base tag to vX.Y.Z (strip anything after the patch, e.g.
# an old alpha tag "v0.1.9_202609010900" -> "v0.1.9").
if [[ "$BASE" =~ ^v?([0-9]+)\.([0-9]+)\.([0-9]+) ]]; then
    BASE="v${BASH_REMATCH[1]}.${BASH_REMATCH[2]}.${BASH_REMATCH[3]}"
else
    BASE="v0.0.0"
fi

# First token of the subject, lower-cased.
FIRST="$(printf '%s' "$SUBJECT" | awk '{print tolower($1)}')"

case "$FIRST" in
    release|release:)
        SECOND="$(printf '%s' "$SUBJECT" | awk '{print $2}')"
        # Trim a trailing ':' or ',' so "release v1.2.3: notes" parses.
        SECOND="${SECOND%%[:,]}"
        if [[ "$SECOND" =~ ^v?([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
            echo "MODE=release"
            echo "VERSION=v${BASH_REMATCH[1]}.${BASH_REMATCH[2]}.${BASH_REMATCH[3]}"
            exit 0
        fi
        echo "error: 'release' must be followed by a semantic version (vX.Y.Z), got '${SECOND}'" >&2
        exit 3
        ;;
    deploy|deploy:)
        echo "MODE=deploy"
        echo "VERSION=${BASE}_${STAMP}"
        exit 0
        ;;
    *)
        echo "MODE=none"
        echo "VERSION=${BASE}_${STAMP}"
        exit 0
        ;;
esac

#!/usr/bin/env bash
# Release-version resolver for .github/workflows/release.yml (pure; no git,
# no network).
#
# Reads the commit SUBJECT (first line of the message) and decides whether
# the commit is a release:
#
#   "release v1.2.3 ..."   -> MODE=release VERSION=v1.2.3
#   "release 1.2.3 ..."    -> same (the leading v is optional)
#   anything else          -> MODE=none
#
# The keyword must be the FIRST word of the subject so an ordinary commit
# whose text merely mentions "release" never ships a build. It ends at the
# first ':' or whitespace ("release:v1.2.3" works).
#
# Usage:
#   scripts/ci/version.sh "<subject>"
#   scripts/ci/version.sh "release v0.2.0: notes"
#     -> MODE=release
#        VERSION=v0.2.0
#
# Output: KEY=VALUE lines (GITHUB_OUTPUT shape). Exit 3 on a malformed
# release version (e.g. "release v1.2").

set -euo pipefail

SUBJECT="${1:-}"

FIRST="$(printf '%s' "$SUBJECT" | awk '{print tolower($1)}')"
FIRST="${FIRST%%:*}"
REST="$(printf '%s' "$SUBJECT" | sed -E 's/^[[:space:]]*[^:[:space:]]*[:[:space:]]*//')"

if [ "$FIRST" = "release" ]; then
    SECOND="$(printf '%s' "$REST" | awk '{print $1}')"
    # Trim a trailing ':' or ',' so "release v1.2.3: notes" parses.
    SECOND="${SECOND%%[:,]}"
    if [[ "$SECOND" =~ ^v?([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
        echo "MODE=release"
        echo "VERSION=v${BASH_REMATCH[1]}.${BASH_REMATCH[2]}.${BASH_REMATCH[3]}"
        exit 0
    fi
    echo "error: 'release' must be followed by a semantic version (vX.Y.Z), got '${SECOND}'" >&2
    exit 3
fi
echo "MODE=none"

#!/usr/bin/env bash
# Sign <dist-dir>/SHA256SUMS with minisign, writing SHA256SUMS.minisig next
# to it. No-op (exit 0) when MINISIGN_SECRET_KEY is unset, so unsigned
# pipelines keep working.
#
# MINISIGN_SECRET_KEY       repository secret: the secret key file contents,
#                           or a path to the key file.
# MINISIGN_PASSWORD         password of the key; empty for a key made with -W.
#
# Usage: scripts/ci/sign-release.sh <dist-dir>
# Exit codes: 0 signed or skipped, 1 signing failed, 3 bad arguments.

set -euo pipefail

DIST="${1:-}"
[ -d "$DIST" ] || { echo "usage: $0 <dist-dir>" >&2; exit 3; }
if [ -z "${MINISIGN_SECRET_KEY:-}" ]; then
    echo ">>> MINISIGN_SECRET_KEY not set; release stays unsigned"
    exit 0
fi
[ -f "$DIST/SHA256SUMS" ] || { echo "error: $DIST/SHA256SUMS missing" >&2; exit 1; }
command -v minisign >/dev/null 2>&1 || { echo "error: minisign not installed" >&2; exit 1; }

KEY="$MINISIGN_SECRET_KEY"
if [ ! -f "$KEY" ]; then
    KEY="$(mktemp)"; trap 'rm -f "$KEY"' EXIT
    chmod 600 "$KEY"
    printf '%s\n' "$MINISIGN_SECRET_KEY" > "$KEY"
fi
printf '%s\n' "${MINISIGN_PASSWORD:-}" \
    | minisign -S -s "$KEY" -m "$DIST/SHA256SUMS" -x "$DIST/SHA256SUMS.minisig" >/dev/null
echo ">>> Signed SHA256SUMS -> SHA256SUMS.minisig"

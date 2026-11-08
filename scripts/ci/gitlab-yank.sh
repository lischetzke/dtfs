#!/usr/bin/env bash
# Withdraw a DTFS GitLab release after a failed rollout so `latest` (the
# installer, `dtfs-cli update`) no longer resolves to it.
#
# Deletes the GitLab release object only. The git tag and the Generic
# Package Registry files stay (forensics; the version number is burnt, so
# the fix ships as the next version).
#
# Auth: DTFS_RELEASE_API_TOKEN (project access token, scope api, role
# Maintainer) if set, else CI_JOB_TOKEN (works only if the instance lets job
# tokens delete releases).
#
# Usage: scripts/ci/gitlab-yank.sh <version>
# Exit codes: 0 yanked (or already gone), 1 API failure, 3 bad arguments.

set -euo pipefail

VERSION="${1:-}"
[ -n "$VERSION" ] || { echo "usage: $0 <version>" >&2; exit 3; }
for v in CI_API_V4_URL CI_PROJECT_ID; do
    [ -n "${!v:-}" ] || { echo "error: $v is not set (run inside a GitLab CI job)" >&2; exit 3; }
done
if [ -n "${DTFS_RELEASE_API_TOKEN:-}" ]; then
    AUTH="PRIVATE-TOKEN: $DTFS_RELEASE_API_TOKEN"
elif [ -n "${CI_JOB_TOKEN:-}" ]; then
    AUTH="JOB-TOKEN: $CI_JOB_TOKEN"
else
    echo "error: neither DTFS_RELEASE_API_TOKEN nor CI_JOB_TOKEN is set" >&2; exit 3
fi

OUT="$(mktemp)"; trap 'rm -f "$OUT"' EXIT
HTTP="$(curl -sS -o "$OUT" -w '%{http_code}' --header "$AUTH" --request DELETE \
    "$CI_API_V4_URL/projects/$CI_PROJECT_ID/releases/$VERSION")"
case "$HTTP" in
    200) echo ">>> Yanked release $VERSION (tag and packages kept)" ;;
    404) echo ">>> Release $VERSION not found; nothing to yank" ;;
    *) echo "error: release delete returned HTTP $HTTP:" >&2; cat "$OUT" >&2; exit 1 ;;
esac

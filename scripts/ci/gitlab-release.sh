#!/usr/bin/env bash
# Publish a DTFS build as a GitLab release from inside a CI job.
#
#   1. uploads every file in <dist-dir> to the project's Generic Package
#      Registry under package "dtfs", version <version>;
#   2. creates (or, with --update, patches) the release <version> pointing
#      at $CI_COMMIT_SHA — the Releases API creates the git tag when it
#      does not exist yet — with one asset link per uploaded file and the
#      contents of <dist-dir>/notes.md as description.
#
# No release-cli needed: plain curl + jq against the GitLab API, so it
# runs on a shell executor. Needs the CI-provided variables
# CI_API_V4_URL, CI_PROJECT_ID, CI_JOB_TOKEN, CI_COMMIT_SHA.
#
# Usage:
#   scripts/ci/gitlab-release.sh <version> <dist-dir> [--prerelease]
#
# Exit codes: 0 ok, 1 upload/API failure, 3 bad arguments.

set -euo pipefail

VERSION="${1:-}"
DIST="${2:-}"
PRERELEASE=0
shift 2 || true
while [ $# -gt 0 ]; do
    case "$1" in
        --prerelease) PRERELEASE=1 ;;
        *) echo "error: unknown argument: $1" >&2; exit 3 ;;
    esac
    shift
done

[ -n "$VERSION" ] && [ -d "$DIST" ] || { echo "usage: $0 <version> <dist-dir> [--prerelease]" >&2; exit 3; }
for v in CI_API_V4_URL CI_PROJECT_ID CI_JOB_TOKEN CI_COMMIT_SHA; do
    [ -n "${!v:-}" ] || { echo "error: $v is not set (run inside a GitLab CI job)" >&2; exit 3; }
done
for tool in curl jq; do
    command -v "$tool" >/dev/null 2>&1 || { echo "error: $tool not found" >&2; exit 3; }
done

PKG_BASE="$CI_API_V4_URL/projects/$CI_PROJECT_ID/packages/generic/dtfs/$VERSION"
LINKS='[]'

echo ">>> Uploading assets to $PKG_BASE"
for f in "$DIST"/*; do
    [ -f "$f" ] || continue
    name="$(basename "$f")"
    case "$name" in notes.md) continue ;; esac
    echo "    $name"
    curl -fsS --header "JOB-TOKEN: $CI_JOB_TOKEN" --upload-file "$f" "$PKG_BASE/$name" >/dev/null
    LINKS="$(jq -c --arg n "$name" --arg u "$PKG_BASE/$name" \
        '. + [{name: $n, url: $u, link_type: "package"}]' <<< "$LINKS")"
done

if [ -f "$DIST/notes.md" ]; then
    DESC="$(cat "$DIST/notes.md")"
else
    DESC="Build $VERSION from $CI_COMMIT_SHA."
fi
if [ "$PRERELEASE" -eq 1 ]; then
    DESC="**Alpha build** (automatic deploy). Not a release; superseded by the next \`release vX.Y.Z\` commit.

$DESC"
fi

BODY="$(jq -n --arg name "$VERSION" --arg tag "$VERSION" --arg ref "$CI_COMMIT_SHA" \
    --arg desc "$DESC" --argjson links "$LINKS" \
    '{name: $name, tag_name: $tag, ref: $ref, description: $desc, assets: {links: $links}}')"

echo ">>> Creating release $VERSION (tag from $CI_COMMIT_SHA)"
HTTP="$(curl -sS -o /tmp/gitlab-release.out -w '%{http_code}' \
    --header "JOB-TOKEN: $CI_JOB_TOKEN" --header "Content-Type: application/json" \
    --request POST --data "$BODY" "$CI_API_V4_URL/projects/$CI_PROJECT_ID/releases")"
if [ "$HTTP" != "201" ]; then
    echo "error: release create returned HTTP $HTTP:" >&2
    cat /tmp/gitlab-release.out >&2
    exit 1
fi
echo ">>> Released $VERSION: $(jq -r '._links.self // .tag_name' /tmp/gitlab-release.out)"

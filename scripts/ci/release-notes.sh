#!/usr/bin/env bash
# Composes the GitHub release notes (pure; no git, no network).
#
# Usage: scripts/ci/release-notes.sh <vX.Y.Z> <cliff-notes-file> [<commit-message-file>]
#
# Output, in order: the welcome line; the release commit body (subject line
# and the trailing trailer paragraph removed); the Upgrading block; `---`;
# `## What's Changed` + the git-cliff notes. Without a body (or without a
# message file) only the welcome line, Upgrading and the changelog print.
# A body that already starts with the welcome line does not get a second one.
# Style and template: .github/release-notes-template.md.

set -euo pipefail

VERSION="${1:-}"
CLIFF="${2:-}"
MSG="${3:-}"

if ! printf '%s' "$VERSION" | grep -Eq '^v[0-9]+\.[0-9]+\.[0-9]+$'; then
    echo "usage: $0 <vX.Y.Z> <cliff-notes-file> [<commit-message-file>] (bad version '$VERSION')" >&2
    exit 2
fi
[ -f "$CLIFF" ] || { echo "cliff notes file not found: '$CLIFF'" >&2; exit 2; }
if [ -n "$MSG" ] && [ ! -f "$MSG" ]; then
    echo "commit message file not found: '$MSG'" >&2; exit 2
fi

# Body: drop the subject line, trim blank edges, drop the last paragraph when
# every line of it is a `Token: value` trailer (or its indented continuation),
# the shape git's trailer parser accepts.
body=""
if [ -n "$MSG" ]; then
    body="$(awk '
        { sub(/\r$/, ""); if (NR > 1) l[++n] = $0 }
        # "Also:" opens the optional last prose line of the highlights, not a trailer.
        function tr(s) { return (s ~ /^[A-Za-z][A-Za-z0-9-]*:[ \t]/ && s !~ /^Also:/) || s ~ /^[ \t]/ }
        END {
            while (n > 0 && l[n] ~ /^[ \t]*$/) n--
            s = 1; while (s <= n && l[s] ~ /^[ \t]*$/) s++
            p = n; while (p >= s && l[p] !~ /^[ \t]*$/) p--
            p++                       # first line of the last paragraph
            t = (n >= p)
            for (i = p; i <= n; i++) if (!tr(l[i])) t = 0
            if (t) { n = p - 1; while (n >= s && l[n] ~ /^[ \t]*$/) n-- }
            for (i = s; i <= n; i++) print l[i]
        }' "$MSG")"
fi

welcome="Welcome to DTFS \`$VERSION\`!"
if [ -n "$body" ] && printf '%s\n' "$body" | head -n1 | grep -q '^Welcome to DTFS'; then
    welcome=""
fi

[ -z "$welcome" ] || printf '%s\n\n' "$welcome"
[ -z "$body" ] || printf '%s\n\n' "$body"
cat <<EOF
## Upgrading

\`\`\`bash
ansible-playbook -i /path/to/inventory.yml deploy/ansible/update-cluster.yml -e dtfs_version=$VERSION
\`\`\`

Or per host: \`dtfs-cli update all --tag $VERSION\` (as root), managers first (followers, then the leader), then data nodes, then clients.

---

## What's Changed

EOF
if grep -q '[^[:space:]]' "$CLIFF"; then
    cat "$CLIFF"
else
    echo "No user-visible changes."
fi

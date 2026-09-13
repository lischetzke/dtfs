#!/usr/bin/env bash
# .claude/hooks/guard-git.sh
#
# PreToolUse hook on the Bash tool. Encodes the three "never" rules from
# CLAUDE.md B.3 as a hard block instead of prose Claude has to remember:
#   - never --no-verify
#   - never push --force (without --force-with-lease)
#   - never `commit --amend` a commit that has already been pushed
# Reads the tool_input JSON from stdin (see
# https://code.claude.com/docs/en/hooks). Exit 2 + stderr blocks the call;
# exit 0 lets it through.

set -u

INPUT="$(cat)"

# Pull tool_name and tool_input.command out of the JSON without requiring
# jq (not guaranteed present in Git Bash on Windows). python3 is present
# on this host; fall back to a best-effort grep/sed extraction if it's
# ever missing so the hook fails open rather than crashing the tool call.
extract() {
    local key="$1"
    if command -v python3 >/dev/null 2>&1; then
        printf '%s' "$INPUT" | python3 -c "
import json, sys
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(0)
v = d
for part in '$key'.split('.'):
    if isinstance(v, dict):
        v = v.get(part)
    else:
        v = None
        break
if v is not None:
    print(v)
" 2>/dev/null
    else
        printf '%s' "$INPUT" | grep -o "\"${key##*.}\"[[:space:]]*:[[:space:]]*\"[^\"]*\"" \
            | head -1 | sed -E 's/.*: *"(.*)"/\1/'
    fi
}

TOOL_NAME="$(extract tool_name)"
[ "$TOOL_NAME" = "Bash" ] || exit 0

CMD="$(extract tool_input.command)"
[ -n "$CMD" ] || exit 0

block() {
    echo "guard-git: $1" >&2
    exit 2
}

# Evaluate one shell "segment" of the command (split below on control
# operators) rather than the whole string, so a flag name that merely
# appears as TEXT elsewhere in the command (inside a grep pattern, a
# commit message body, an echo, ...) never triggers a false block --
# only a segment that is actually invoking git with that flag does.
check_segment() {
    local seg="$1"

    if printf '%s' "$seg" | grep -Eq 'git[[:space:]]+commit' \
       && printf '%s' "$seg" | grep -Eq -- '--no-verify'; then
        block "git commit --no-verify is blocked (CLAUDE.md B.3). Fix the failing hook instead of bypassing it."
    fi

    if printf '%s' "$seg" | grep -Eq 'git[[:space:]]+push' \
       && printf '%s' "$seg" | grep -Eq '(^|[[:space:]])--force([[:space:]]|$)' \
       && ! printf '%s' "$seg" | grep -Eq -- '--force-with-lease'; then
        block "git push --force is blocked; use --force-with-lease, or get explicit user sign-off for a raw --force (CLAUDE.md B.3)."
    fi

    if printf '%s' "$seg" | grep -Eq 'git[[:space:]]+commit' \
       && printf '%s' "$seg" | grep -Eq -- '--amend'; then
        HEAD_SHA="$(git rev-parse HEAD 2>/dev/null || true)"
        UPSTREAM_SHA="$(git rev-parse '@{u}' 2>/dev/null || git rev-parse origin/master 2>/dev/null || true)"
        if [ -n "$HEAD_SHA" ] && [ -n "$UPSTREAM_SHA" ] && [ "$HEAD_SHA" = "$UPSTREAM_SHA" ]; then
            block "HEAD ($HEAD_SHA) matches the pushed upstream tip; --amend would rewrite a pushed commit (CLAUDE.md B.3). Create a new commit instead."
        fi
    fi

    if printf '%s' "$seg" | grep -Eq 'git[[:space:]]+checkout[[:space:]]+--[[:space:]]'; then
        local target
        target="$(printf '%s' "$seg" | sed -E 's/.*checkout[[:space:]]+--[[:space:]]+//')"
        if [ -n "$target" ] && ! git diff --quiet -- "$target" 2>/dev/null; then
            block "git checkout -- $target would discard uncommitted changes to a dirty tracked file. Stash or commit first, or get explicit user sign-off."
        fi
    fi
}

# Naive split on shell control operators (;, &&, ||, |). Doesn't account
# for quoting, so a control operator INSIDE a quoted string still splits
# here -- acceptable: it only makes the checks run on smaller pieces of
# text, never merges two real commands into one, so it can't hide a
# real git invocation, only occasionally over-split harmless text.
OLDIFS="$IFS"
IFS=$'\n'
# shellcheck disable=SC2207
SEGMENTS=($(printf '%s' "$CMD" | sed -E 's/(&&|\|\||;|\|)/\n/g'))
IFS="$OLDIFS"
for seg in "${SEGMENTS[@]}"; do
    check_segment "$seg"
done

exit 0

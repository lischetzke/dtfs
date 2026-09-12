#!/usr/bin/env bash
# .claude/hooks/check-release-case.sh
#
# PreToolUse hook on the Bash tool. Implements AGENTS.md B.4 / D6:
# a commit that touches cluster-visible code must also stage a
# tests/release/cases/NN-*.sh, or carry a `Release-case: none (<reason>)`
# trailer in the commit message. Exit 2 blocks; exit 0 lets it through.
# Only evaluates `git commit` invocations; every other command is a
# no-op pass-through.

set -u

INPUT="$(cat)"

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

# Only act on a segment that is actually a git commit invocation (same
# per-segment discipline as guard-git.sh, to avoid false positives on
# unrelated text that happens to contain "git commit").
IS_COMMIT=0
OLDIFS="$IFS"
IFS=$'\n'
# shellcheck disable=SC2207
SEGMENTS=($(printf '%s' "$CMD" | sed -E 's/(&&|\|\||;|\|)/\n/g'))
IFS="$OLDIFS"
for seg in "${SEGMENTS[@]}"; do
    if printf '%s' "$seg" | grep -Eq 'git[[:space:]]+commit([[:space:]]|$)'; then
        IS_COMMIT=1
    fi
done
[ "$IS_COMMIT" = "1" ] || exit 0

STAGED="$(git diff --cached --name-only 2>/dev/null || true)"
[ -n "$STAGED" ] || exit 0   # nothing staged (e.g. an empty/amend-only commit) -- not this hook's concern

CLUSTER_TOUCHED=0
while IFS= read -r f; do
    case "$f" in
        crates/dtfs-manager/src/api.rs|crates/dtfs-manager/src/metadata.rs|crates/dtfs-manager/src/scheduler.rs) CLUSTER_TOUCHED=1 ;;
        crates/dtfs-data/src/*) CLUSTER_TOUCHED=1 ;;
        crates/dtfs-fuse/src/*) CLUSTER_TOUCHED=1 ;;
        crates/dtfs-raft/src/*) CLUSTER_TOUCHED=1 ;;
        proto/*) CLUSTER_TOUCHED=1 ;;
    esac
done <<< "$STAGED"
[ "$CLUSTER_TOUCHED" = "1" ] || exit 0

CASE_STAGED=0
while IFS= read -r f; do
    case "$f" in
        tests/release/cases/*.sh) CASE_STAGED=1 ;;
    esac
done <<< "$STAGED"
[ "$CASE_STAGED" = "1" ] && exit 0

# Any `Release-case:` trailer satisfies the hook: `none (<reason>)` for a
# pure internal change, or `tests/release/cases/NNN-*.sh (<task-id>)` when
# the case lands in a later commit of the same plan (one commit per task).
if printf '%s' "$CMD" | grep -Eq 'Release-case:[[:space:]]*[^[:space:]]'; then
    exit 0
fi

{
    echo "check-release-case: staged changes touch cluster-visible code"
    echo "  (dtfs-manager api/metadata/scheduler, dtfs-data/src, dtfs-fuse/src, or proto/)"
    echo "  but no tests/release/cases/NN-*.sh is staged and the commit message"
    echo "  carries no 'Release-case:' trailer (AGENTS.md B.4 / tests/release/README.md)."
    echo "  Add a case, or a trailer: 'Release-case: none (<reason>)' for a pure internal"
    echo "  change, or 'Release-case: tests/release/cases/NNN-name.sh (<task-id>)' when the"
    echo "  case lands in a later commit of the same plan."
} >&2
exit 2

#!/usr/bin/env bash
# .claude/hooks/rustfmt-check.sh
#
# PostToolUse hook on Edit|Write. Advisory only: for an edited
# crates/**/*.rs file, runs `rustfmt --check --edition 2021` inside the
# dtfs-buildenv Docker image (AGENTS.md A.4) and prints the diff if it's
# not clean. Never blocks (always exits 0) -- a formatting nit mid-task
# is more disruptive than letting it through; `cargo fmt` cleans it up
# later. No-ops silently if docker, or the dtfs-buildenv image, isn't
# available, or the edited file isn't a crates/**/*.rs file.

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
case "$TOOL_NAME" in
    Edit|Write) ;;
    *) exit 0 ;;
esac

FILE_PATH="$(extract tool_input.file_path)"
[ -n "$FILE_PATH" ] || exit 0

case "$FILE_PATH" in
    */crates/*.rs) ;;
    *) exit 0 ;;
esac

command -v docker >/dev/null 2>&1 || exit 0
docker image inspect dtfs-buildenv:latest >/dev/null 2>&1 || exit 0

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
case "$FILE_PATH" in
    "$REPO_ROOT"/*) REL="${FILE_PATH#"$REPO_ROOT"/}" ;;
    *) REL="$FILE_PATH" ;;
esac

DIFF="$(MSYS_NO_PATHCONV=1 docker run --rm -v "$REPO_ROOT:/build" -w /build \
    dtfs-buildenv:latest rustfmt --check --edition 2021 "$REL" 2>&1)" || true

if [ -n "$DIFF" ]; then
    echo "rustfmt-check: $REL is not rustfmt-clean (advisory, not blocking):" >&2
    echo "$DIFF" >&2
fi

exit 0

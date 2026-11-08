#!/usr/bin/env bash
#
# Build the DTFS HTML documentation (mdBook) from docs/src/*.md.
#
# Output: docs/book/ (gitignored). Internal Markdown links are checked first
# (scripts/check-docs-links.py) unless --no-linkcheck is given.
#
# mdBook resolution, first match wins:
#   1. a local `mdbook` on PATH (a version other than the pinned one only warns);
#   2. the pinned release binary below (static musl build), downloaded once
#      into ${DTFS_MDBOOK_CACHE:-$HOME/.cache/dtfs/mdbook} and sha256-verified;
#      run directly on Linux x86_64, otherwise inside a debian container
#      (needs docker; works from Git Bash on Windows).
#
# Usage:
#   ./scripts/build-docs.sh               build
#   ./scripts/build-docs.sh --clean       remove docs/book first
#   ./scripts/build-docs.sh --serve       build + live-reload server on :3000
#   ./scripts/build-docs.sh --no-linkcheck
#
# Env: MDBOOK_OUTPUT__HTML__SITE_URL overrides [output.html] site-url (the
#      GitLab Pages job sets it to the project sub-path).

set -euo pipefail

# Pinned mdBook. The digest is the sha256 of the GitHub release asset
# https://github.com/rust-lang/mdBook/releases/tag/v0.5.2
MDBOOK_VERSION="0.5.2"
MDBOOK_ASSET="mdbook-v${MDBOOK_VERSION}-x86_64-unknown-linux-musl.tar.gz"
MDBOOK_URL="https://github.com/rust-lang/mdBook/releases/download/v${MDBOOK_VERSION}/${MDBOOK_ASSET}"
MDBOOK_SHA256="8e9d802bbecea34380aabfc65bec4fdba23c07453ef2c7e06fa34c282082e2c4"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DOCS_DIR="$PROJECT_ROOT/docs"
OUTPUT_DIR="$DOCS_DIR/book"
CACHE_DIR="${DTFS_MDBOOK_CACHE:-$HOME/.cache/dtfs/mdbook}"

info()  { echo "[INFO]  $*"; }
warn()  { echo "[WARN]  $*" >&2; }
error() { echo "[ERROR] $*" >&2; }

SERVE=0; CLEAN=0; LINKCHECK=1
for arg in "$@"; do
    case "$arg" in
        --serve) SERVE=1 ;;
        --clean) CLEAN=1 ;;
        --no-linkcheck) LINKCHECK=0 ;;
        --help|-h) sed -n '2,/^set -e/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) error "unknown argument: $arg"; exit 1 ;;
    esac
done

# Download + verify the pinned tarball once; prints the cached binary path.
fetch_pinned() {
    local bin="$CACHE_DIR/mdbook-$MDBOOK_VERSION"
    if [ ! -x "$bin" ]; then
        command -v curl >/dev/null 2>&1 || { error "curl is required to download mdBook"; return 1; }
        mkdir -p "$CACHE_DIR"
        local tmp; tmp="$(mktemp -d)"
        info "downloading $MDBOOK_URL" >&2
        curl -fsSL -o "$tmp/$MDBOOK_ASSET" "$MDBOOK_URL"
        echo "$MDBOOK_SHA256  $tmp/$MDBOOK_ASSET" | sha256sum -c - >&2 \
            || { error "sha256 mismatch for $MDBOOK_ASSET"; rm -rf "$tmp"; return 1; }
        tar -C "$tmp" -xzf "$tmp/$MDBOOK_ASSET" mdbook
        install -m 0755 "$tmp/mdbook" "$bin"
        rm -rf "$tmp"
    fi
    echo "$bin"
}

# MDBOOK = argv prefix that runs mdbook in docs/.
SERVE_ARGS=()
if command -v mdbook >/dev/null 2>&1; then
    have="$(mdbook --version | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1)"
    [ "$have" = "$MDBOOK_VERSION" ] || warn "local mdbook $have, pinned $MDBOOK_VERSION; output may differ"
    MDBOOK=(mdbook)
else
    BIN="$(fetch_pinned)"
    if [ "$(uname -s)" = "Linux" ] && [ "$(uname -m)" = "x86_64" ]; then
        MDBOOK=("$BIN")
    else
        command -v docker >/dev/null 2>&1 || {
            error "no mdbook for this host and no docker; install mdBook $MDBOOK_VERSION from https://github.com/rust-lang/mdBook/releases"
            exit 1
        }
        PORT=(); [ "$SERVE" -eq 0 ] || PORT=(-p 3000:3000)
        MDBOOK=(env MSYS_NO_PATHCONV=1 docker run --rm "${PORT[@]}" -e MDBOOK_OUTPUT__HTML__SITE_URL
                -v "$PROJECT_ROOT:/build" -v "$CACHE_DIR:/mdbook-cache:ro" -w /build/docs
                debian:bookworm-slim "/mdbook-cache/mdbook-$MDBOOK_VERSION")
        SERVE_ARGS=(--hostname 0.0.0.0 --port 3000)
    fi
fi

[ "$CLEAN" -eq 0 ] || { rm -rf "$OUTPUT_DIR"; info "removed $OUTPUT_DIR"; }

if [ "$LINKCHECK" -eq 1 ]; then
    PY="$(command -v python3 || command -v python || true)"
    if [ -n "$PY" ]; then
        "$PY" -I "$SCRIPT_DIR/check-docs-links.py" "$DOCS_DIR/src"
    else
        warn "python not found; skipping the link check"
    fi
fi

cd "$DOCS_DIR"
if [ "$SERVE" -eq 1 ]; then
    info "serving http://localhost:3000 (Ctrl+C to stop)"
    exec "${MDBOOK[@]}" serve "${SERVE_ARGS[@]}"
fi

info "building with mdBook $MDBOOK_VERSION"
"${MDBOOK[@]}" build

pages="$(find "$OUTPUT_DIR" -name '*.html' | wc -l | tr -d ' ')"
info "built $pages HTML pages -> $OUTPUT_DIR (print.html is the single-page version)"

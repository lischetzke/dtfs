#!/usr/bin/env bash
#
# Build DTFS HTML documentation using mdBook.
#
# Generates a static HTML site from the Markdown sources in docs/src/.
# Output goes to docs/book/ (gitignored).
#
# Usage:
#   ./scripts/build-docs.sh           # build docs
#   ./scripts/build-docs.sh --serve   # build and serve locally (live reload)
#   ./scripts/build-docs.sh --open    # build and open in browser
#   ./scripts/build-docs.sh --clean   # remove output directory first
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DOCS_DIR="$PROJECT_ROOT/docs"
OUTPUT_DIR="$DOCS_DIR/book"

# Colours (disabled if not a terminal)
if [ -t 1 ]; then
    BOLD='\033[1m'; GREEN='\033[0;32m'; YELLOW='\033[0;33m'
    RED='\033[0;31m'; RESET='\033[0m'
else
    BOLD='' GREEN='' YELLOW='' RED='' RESET=''
fi

info()  { echo -e "${GREEN}[INFO]${RESET}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${RESET}  $*"; }
error() { echo -e "${RED}[ERROR]${RESET} $*"; }
step()  { echo -e "\n${BOLD}>>> $*${RESET}"; }

MODE="build"
OPEN=false
CLEAN=false

for arg in "$@"; do
    case "$arg" in
        --serve) MODE="serve" ;;
        --open)  OPEN=true ;;
        --clean) CLEAN=true ;;
        --help|-h)
            echo "Usage: $0 [--serve] [--open] [--clean]"
            echo "  --serve   Build and start live-reload dev server (http://localhost:3000)"
            echo "  --open    Open the built docs in the default browser"
            echo "  --clean   Remove output directory before building"
            exit 0
            ;;
        *)
            error "Unknown argument: $arg"
            exit 1
            ;;
    esac
done

# ---------------------------------------------------------------------------
# 1. Check / install mdBook
# ---------------------------------------------------------------------------
step "Checking mdBook"

MDBOOK_MIN="0.4.36"

install_mdbook() {
    info "Installing mdBook via cargo..."
    cargo install mdbook --version "^${MDBOOK_MIN}" --locked
}

if ! command -v mdbook &>/dev/null; then
    warn "mdbook not found."
    if command -v cargo &>/dev/null; then
        install_mdbook
    else
        error "cargo not found either.  Install Rust (https://rustup.rs) or mdBook manually:"
        error "  cargo install mdbook"
        error "  # or download from https://github.com/rust-lang/mdBook/releases"
        exit 1
    fi
else
    INSTALLED="$(mdbook --version 2>&1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+')"
    info "mdBook version: $INSTALLED"
fi

# Optional plugins (non-fatal if absent)
for plugin in mdbook-admonish mdbook-toc mdbook-mermaid; do
    if ! command -v "$plugin" &>/dev/null; then
        warn "$plugin not installed (optional) — run: cargo install $plugin"
    else
        info "$plugin: $(${plugin} --version 2>&1 | head -1)"
    fi
done

# ---------------------------------------------------------------------------
# 2. Clean (optional)
# ---------------------------------------------------------------------------
if [ "$CLEAN" = true ]; then
    step "Cleaning output directory"
    rm -rf "$OUTPUT_DIR"
    info "Removed $OUTPUT_DIR"
fi

# ---------------------------------------------------------------------------
# 3. Build or serve
# ---------------------------------------------------------------------------
cd "$DOCS_DIR"

if [ "$MODE" = "serve" ]; then
    step "Starting live-reload server"
    info "Docs will be served at http://localhost:3000"
    info "Press Ctrl+C to stop."
    exec mdbook serve --open
fi

step "Building documentation"
BUILD_START=$(date +%s)
mdbook build
BUILD_END=$(date +%s)
BUILD_SECS=$((BUILD_END - BUILD_START))
info "Build completed in ${BUILD_SECS}s"

# ---------------------------------------------------------------------------
# 4. Stats
# ---------------------------------------------------------------------------
step "Documentation statistics"

PAGE_COUNT=$(find "$OUTPUT_DIR" -name "*.html" | wc -l)
TOTAL_SIZE=$(du -sh "$OUTPUT_DIR" 2>/dev/null | cut -f1)

info "Pages:  $PAGE_COUNT HTML files"
info "Size:   $TOTAL_SIZE"
info "Output: $OUTPUT_DIR"

# ---------------------------------------------------------------------------
# 5. Open in browser (optional)
# ---------------------------------------------------------------------------
INDEX="$OUTPUT_DIR/index.html"

if [ "$OPEN" = true ]; then
    if command -v xdg-open &>/dev/null; then
        xdg-open "$INDEX" &
    elif command -v open &>/dev/null; then
        open "$INDEX"
    else
        info "Open manually: file://$INDEX"
    fi
fi

# ---------------------------------------------------------------------------
# 6. Summary
# ---------------------------------------------------------------------------
echo ""
echo "  Docs built successfully:"
echo "    file://$INDEX"
echo ""
echo "  To serve locally with live reload:"
echo "    ./scripts/build-docs.sh --serve"
echo ""
echo "  The manager binary serves docs at runtime:"
echo "    http://<manager-host>:9402/docs/"
echo ""

#!/usr/bin/env bash
#
# Build DTFS binaries for Linux/Debian 13 (trixie) amd64.
#
# Produces dynamically-linked executables targeting the native GNU/Linux
# platform. The resulting binaries are compatible with Debian 13 and
# newer glibc-based distributions.
#
# For a fully static build (musl), use build-linux-amd64-static.sh.
#
# Usage:
#   ./scripts/build-linux-amd64.sh          # build all binaries
#   ./scripts/build-linux-amd64.sh --quick  # skip strip + checksums
#
# Output directory: target/release/dist/linux-amd64/

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

TARGET="x86_64-unknown-linux-gnu"
# Honour CARGO_TARGET_DIR (cargo writes there; relative = from the project root).
TARGET_DIR="${CARGO_TARGET_DIR:-$PROJECT_ROOT/target}"
case "$TARGET_DIR" in /*) ;; *) TARGET_DIR="$PROJECT_ROOT/$TARGET_DIR" ;; esac
DIST_DIR="$TARGET_DIR/release/dist/linux-amd64"
QUICK="${1:-}"

# Colours (disabled if not a terminal)
if [ -t 1 ]; then
    BOLD='\033[1m'
    GREEN='\033[0;32m'
    YELLOW='\033[0;33m'
    RED='\033[0;31m'
    RESET='\033[0m'
else
    BOLD='' GREEN='' YELLOW='' RED='' RESET=''
fi

info()  { echo -e "${GREEN}[INFO]${RESET}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${RESET}  $*"; }
error() { echo -e "${RED}[ERROR]${RESET} $*"; }
step()  { echo -e "\n${BOLD}>>> $*${RESET}"; }

BINARIES=(
    dtfs-manager
    dtfs-data
    dtfs-fuse
    dtfs-cli
)

# ---------------------------------------------------------------------------
# 1. Pre-flight checks
# ---------------------------------------------------------------------------
step "Pre-flight checks"

if ! command -v cargo &>/dev/null; then
    error "cargo not found. Install Rust: https://rustup.rs"
    exit 1
fi

RUSTC_VERSION=$(rustc --version)
info "Rust toolchain: $RUSTC_VERSION"

# Ensure build essentials are installed (needed for C deps: ring, rocksdb).
for tool in cc c++ pkg-config; do
    if ! command -v "$tool" &>/dev/null; then
        warn "$tool not found. Installing build-essential..."
        if command -v apt-get &>/dev/null; then
            sudo apt-get update -qq && sudo apt-get install -y -qq build-essential pkg-config libclang-dev
        else
            error "Please install build-essential, pkg-config, and libclang-dev manually."
            exit 1
        fi
        break
    fi
done

info "cc:  $(cc --version 2>&1 | head -1)"
info "c++: $(c++ --version 2>&1 | head -1)"
info "Target: $TARGET (native)"

# ---------------------------------------------------------------------------
# 2. Build
# ---------------------------------------------------------------------------
step "Building all binaries (release, target=$TARGET)"

# Suppress dead_code warnings to work around rustc 1.94 ICE in the
# annotate-snippets warning renderer (crashes when emitting certain
# lint diagnostics in JSON mode).  The code is correct; the ICE is a
# known compiler bug that will be fixed in a future release.
export RUSTFLAGS="${RUSTFLAGS:-} -A dead_code -A unused_imports -A unused_variables"

# Bug BS — stamp BUILD_DATE with this build's instant. dtfs-common/build.rs
# formats SOURCE_DATE_EPOCH and declares rerun-if-env-changed for it, so
# exporting a fresh value here re-stamps every wrapper build (two builds a
# minute apart -> two distinct BUILD_DATE values; CLAUDE.md §A.3.5).
# Honour a caller-pinned value for reproducible builds (the sanctioned
# exception): only default to "now" when unset.
export SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH:-$(date -u +%s)}"

BUILD_START=$(date +%s)

cargo build \
    --release \
    --bin dtfs-manager \
    --bin dtfs-data \
    --bin dtfs-fuse \
    --bin dtfs-cli

BUILD_END=$(date +%s)
BUILD_SECS=$((BUILD_END - BUILD_START))

info "Build completed in ${BUILD_SECS}s"

# ---------------------------------------------------------------------------
# 3. Collect binaries into dist directory
# ---------------------------------------------------------------------------
step "Collecting binaries into $DIST_DIR"

rm -rf "$DIST_DIR"
mkdir -p "$DIST_DIR"

for bin in "${BINARIES[@]}"; do
    SRC="$TARGET_DIR/release/$bin"
    if [ ! -f "$SRC" ]; then
        error "Binary not found: $SRC"
        exit 1
    fi
    cp "$SRC" "$DIST_DIR/$bin"
    info "Copied $bin"
done

# ---------------------------------------------------------------------------
# 4. Strip binaries (unless --quick)
# ---------------------------------------------------------------------------
if [ "$QUICK" != "--quick" ]; then
    step "Stripping debug symbols"

    if command -v strip &>/dev/null; then
        for bin in "${BINARIES[@]}"; do
            BEFORE=$(stat -c%s "$DIST_DIR/$bin" 2>/dev/null || stat -f%z "$DIST_DIR/$bin")
            strip "$DIST_DIR/$bin"
            AFTER=$(stat -c%s "$DIST_DIR/$bin" 2>/dev/null || stat -f%z "$DIST_DIR/$bin")
            SAVED=$(( (BEFORE - AFTER) / 1024 / 1024 ))
            info "Stripped $bin: $(numfmt --to=iec $BEFORE) -> $(numfmt --to=iec $AFTER) (saved ${SAVED}MB)"
        done
    else
        warn "strip not found, skipping"
    fi
fi

# ---------------------------------------------------------------------------
# 5. Generate checksums
# ---------------------------------------------------------------------------
if [ "$QUICK" != "--quick" ]; then
    step "Generating checksums"

    cd "$DIST_DIR"
    if command -v sha256sum &>/dev/null; then
        sha256sum "${BINARIES[@]}" > SHA256SUMS
        info "SHA256SUMS written"
        cat SHA256SUMS
    elif command -v shasum &>/dev/null; then
        shasum -a 256 "${BINARIES[@]}" > SHA256SUMS
        info "SHA256SUMS written"
        cat SHA256SUMS
    fi
    cd "$PROJECT_ROOT"
fi

# ---------------------------------------------------------------------------
# 6. Verify binaries
# ---------------------------------------------------------------------------
step "Verifying binaries"

for bin in "${BINARIES[@]}"; do
    FILE_TYPE=$(file "$DIST_DIR/$bin")
    if echo "$FILE_TYPE" | grep -q "ELF 64-bit LSB"; then
        info "$bin: OK (ELF 64-bit x86-64)"
    else
        warn "$bin: unexpected file type: $FILE_TYPE"
    fi
done

# Quick smoke test: each binary should respond to --version or --help.
for bin in "${BINARIES[@]}"; do
    if "$DIST_DIR/$bin" --version &>/dev/null || "$DIST_DIR/$bin" --help &>/dev/null; then
        info "$bin: runs OK"
    else
        warn "$bin: could not run --version/--help (may need config)"
    fi
done

# ---------------------------------------------------------------------------
# 7. Show results
# ---------------------------------------------------------------------------
step "Build artifacts"

echo ""
echo "  Platform:  Linux amd64 (glibc, Debian 13 compatible)"
echo "  Target:    $TARGET"
echo "  Directory: $DIST_DIR"
echo ""

ls -lh "$DIST_DIR/"

echo ""

# Show glibc requirement.
for bin in "${BINARIES[@]}"; do
    GLIBC_VER=$(objdump -T "$DIST_DIR/$bin" 2>/dev/null | grep GLIBC | sed 's/.*GLIBC_//' | sort -V | tail -1 || echo "unknown")
    info "$bin requires glibc >= $GLIBC_VER"
done

echo ""
info "Done! Built ${#BINARIES[@]} binaries in ${BUILD_SECS}s."
echo ""
echo "  Install on Debian 13:"
echo "    sudo cp $DIST_DIR/dtfs-* /usr/local/bin/"
echo "    sudo chmod +x /usr/local/bin/dtfs-*"
echo ""

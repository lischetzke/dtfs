#!/usr/bin/env bash
# DTFS build orchestrator.
#
# Wraps `cargo build --release` for the four DTFS binaries, runs the
# self-test suite, and stages the result into a tarball suitable for
# pushing to manager / data / FUSE-client hosts.
#
# Usage:
#   ./scripts/dtfs-build.sh             # full build + tests + tarball
#   ./scripts/dtfs-build.sh --no-tests  # skip unit tests
#   ./scripts/dtfs-build.sh --debug     # debug profile (faster, slower binary)
#   ./scripts/dtfs-build.sh --help
#
# Exit codes:
#   0  success — tarball at target/dist/dtfs-<git-rev>-linux-amd64.tar.gz
#   1  build failed
#   2  tests failed
#   3  invalid argument

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

PROFILE=release
RUN_TESTS=1

# ─── arg parsing ─────────────────────────────────────────────────────────────
while [ $# -gt 0 ]; do
    case "$1" in
        --no-tests) RUN_TESTS=0 ;;
        --debug)    PROFILE=debug ;;
        --help|-h)
            sed -n '2,18p' "$0"
            exit 0
            ;;
        *) echo "unknown arg: $1" >&2; exit 3 ;;
    esac
    shift
done

# ─── prereqs ─────────────────────────────────────────────────────────────────
if ! command -v cargo >/dev/null 2>&1; then
    echo "error: cargo not in PATH" >&2; exit 1
fi
if ! command -v protoc >/dev/null 2>&1; then
    echo "warn: protoc not in PATH — build may fail if proto code regen is needed" >&2
fi

REV=$(git rev-parse --short HEAD 2>/dev/null || echo "nogit")
OUT_DIR="$PROJECT_ROOT/target/dist"
TARBALL="$OUT_DIR/dtfs-${REV}-linux-amd64.tar.gz"
BIN_DIR="$PROJECT_ROOT/target/$PROFILE"
mkdir -p "$OUT_DIR"

# Bug BS — re-stamp BUILD_DATE per build via SOURCE_DATE_EPOCH (build.rs
# honours it + declares rerun-if-env-changed). Caller-pinned value wins
# (reproducible-build exception); otherwise default to this build's instant.
export SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH:-$(date -u +%s)}"

echo ">>> Building DTFS (profile=$PROFILE, rev=$REV)"
if [ "$PROFILE" = "release" ]; then
    cargo build --release
else
    cargo build
fi

# ─── unit tests ───────────────────────────────────────────────────────────────
if [ "$RUN_TESTS" -eq 1 ]; then
    echo ">>> Running unit tests"
    if ! cargo test --release -p dtfs-raft --lib; then
        echo "error: dtfs-raft unit tests failed" >&2; exit 2
    fi
    if ! cargo test --release -p dtfs-fuse --bins; then
        echo "error: dtfs-fuse unit tests failed" >&2; exit 2
    fi
    echo ">>> Unit tests OK"
fi

# ─── stage tarball ────────────────────────────────────────────────────────────
echo ">>> Staging tarball"
STAGE=$(mktemp -d)
trap 'rm -rf "$STAGE"' EXIT
mkdir -p "$STAGE/dtfs/bin" "$STAGE/dtfs/configs" "$STAGE/dtfs/systemd"
for b in dtfs-manager dtfs-data dtfs-fuse dtfs-cli; do
    if [ ! -x "$BIN_DIR/$b" ]; then
        echo "error: $BIN_DIR/$b missing" >&2; exit 1
    fi
    cp "$BIN_DIR/$b" "$STAGE/dtfs/bin/$b"
done

# Sample configs + systemd units
cp deploy/configs/*.toml "$STAGE/dtfs/configs/" 2>/dev/null || true
cp deploy/systemd/*.service "$STAGE/dtfs/systemd/" 2>/dev/null || true

# Manifest
cat > "$STAGE/dtfs/MANIFEST" <<EOF
DTFS build manifest
rev:     $REV
profile: $PROFILE
built:   $(date -u +%Y-%m-%dT%H:%M:%SZ)
host:    $(uname -srm)
EOF
for b in "$STAGE"/dtfs/bin/*; do
    echo "$(sha256sum "$b" | awk '{print $1}')  $(basename "$b")" >> "$STAGE/dtfs/MANIFEST"
done

tar -C "$STAGE" -czf "$TARBALL" dtfs
SIZE=$(du -h "$TARBALL" | awk '{print $1}')
echo ">>> Done: $TARBALL ($SIZE)"

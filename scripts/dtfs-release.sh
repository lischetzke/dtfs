#!/usr/bin/env bash
# AZ-5 — DTFS release builder.
#
# Turns a version string into a publishable GitHub release: builds the
# four binaries with the version stamped in (DTFS_VERSION, AGENTS.md
# § A.3.5), assembles the fixed-name asset the updater downloads, writes
# checksums + release notes, and prints (or runs, with --publish) the
# `gh release create` command.
#
# Usage:
#   ./scripts/dtfs-release.sh <version> [--publish] [--notes-file FILE]
#   ./scripts/dtfs-release.sh v0.3.0
#   ./scripts/dtfs-release.sh v0.3.0 --publish
#
# Output: target/release-artifacts/<version>/
#   dtfs-linux-amd64.tar.gz   asset the updater pulls (dtfs/bin + systemd + MANIFEST)
#   SHA256SUMS                checksum of the asset
#   RELEASE_NOTES.md          --notes-file, else changelog/UNRELEASED.md
#
# Exit codes: 0 ok, 1 build/assembly failed, 3 invalid argument.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

if [ -t 1 ]; then
    GREEN='\033[0;32m'; YELLOW='\033[0;33m'; RED='\033[0;31m'; BOLD='\033[1m'; OFF='\033[0m'
else
    GREEN=''; YELLOW=''; RED=''; BOLD=''; OFF=''
fi
info()  { echo -e "${GREEN}[INFO]${OFF}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${OFF}  $*"; }
error() { echo -e "${RED}[ERROR]${OFF} $*"; }
step()  { echo -e "\n${BOLD}>>> $*${OFF}"; }

VERSION=""
PUBLISH=0
NOTES_FILE=""

while [ $# -gt 0 ]; do
    case "$1" in
        --publish) PUBLISH=1 ;;
        --notes-file) shift; NOTES_FILE="${1:-}" ;;
        --help|-h) sed -n '2,21p' "$0"; exit 0 ;;
        -*) error "unknown flag: $1"; exit 3 ;;
        *)
            if [ -z "$VERSION" ]; then VERSION="$1"; else
                error "unexpected argument: $1"; exit 3
            fi
            ;;
    esac
    shift
done

[ -n "$VERSION" ] || { error "missing <version> (e.g. v0.3.0)"; exit 3; }
if ! echo "$VERSION" | grep -qE '^v[0-9]+\.[0-9]+\.[0-9]+'; then
    warn "version '$VERSION' doesn't look like vX.Y.Z — using it as-is"
fi
if [ -n "$NOTES_FILE" ] && [ ! -f "$NOTES_FILE" ]; then
    error "--notes-file not found: $NOTES_FILE"; exit 3
fi

# Stamp the version + a fresh build date into the binaries (§ A.3.5), then
# build + stage the canonical tarball via dtfs-build.sh.
export DTFS_VERSION="$VERSION"
export SOURCE_DATE_EPOCH="$(date -u +%s)"

step "Building + staging $VERSION"
"$SCRIPT_DIR/dtfs-build.sh"

SRC_TARBALL="$(ls -t "$PROJECT_ROOT"/target/dist/dtfs-*-linux-amd64.tar.gz 2>/dev/null | head -1 || true)"
[ -n "$SRC_TARBALL" ] && [ -f "$SRC_TARBALL" ] || { error "dtfs-build.sh produced no tarball"; exit 1; }

OUT_DIR="$PROJECT_ROOT/target/release-artifacts/$VERSION"
ASSET="dtfs-linux-amd64.tar.gz"
step "Assembling release artifacts in $OUT_DIR"
rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"
cp "$SRC_TARBALL" "$OUT_DIR/$ASSET"
( cd "$OUT_DIR" && sha256sum "$ASSET" > SHA256SUMS )

if [ -n "$NOTES_FILE" ]; then
    cp "$NOTES_FILE" "$OUT_DIR/RELEASE_NOTES.md"
elif [ -f "$PROJECT_ROOT/changelog/UNRELEASED.md" ]; then
    cp "$PROJECT_ROOT/changelog/UNRELEASED.md" "$OUT_DIR/RELEASE_NOTES.md"
else
    echo "DTFS release $VERSION" > "$OUT_DIR/RELEASE_NOTES.md"
fi

# Confirm the version stamp survived into the asset (§ A.3.5).
step "Verifying version stamp"
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
tar -C "$TMP" -xzf "$OUT_DIR/$ASSET" dtfs/bin/dtfs-cli
STAMP="$("$TMP/dtfs/bin/dtfs-cli" --version 2>/dev/null || true)"
case "$STAMP" in
    "dtfs-cli $VERSION "*) info "stamp OK: $STAMP" ;;
    *) error "version stamp mismatch: expected 'dtfs-cli $VERSION ...', got '$STAMP'"; exit 1 ;;
esac

GH_CMD="gh release create $VERSION $OUT_DIR/$ASSET $OUT_DIR/SHA256SUMS --title $VERSION --notes-file $OUT_DIR/RELEASE_NOTES.md"

step "Release artifacts"
ls -lh "$OUT_DIR"
echo

if [ "$PUBLISH" -eq 1 ]; then
    command -v gh >/dev/null 2>&1 || { error "gh not found; cannot --publish"; exit 1; }
    step "Publishing with gh"
    eval "$GH_CMD"
    info "published $VERSION"
else
    info "Not publishing (no --publish). To create the GitHub release, run:"
    echo "    $GH_CMD"
fi

#!/usr/bin/env bash
# AZ-5: `dtfs-cli update` — offline local-install path.
#
# Builds a fake `file://` release (dtfs/bin/dtfs-cli + MANIFEST), then:
#   1. `update --help` works.
#   2. `update cli --repo-url file://… --install-dir <tmp> --no-restart -y`
#      downloads, sha256-verifies against MANIFEST, and atomically installs.
#   3. Integrity gate: a binary whose sha256 != MANIFEST is refused and
#      nothing is installed.
# No network, no root, no systemd. Self-SKIPs if the cli or the deploy
# tools (curl/tar/install/sha256sum) aren't present.
source "$(dirname "$0")/../lib/common.sh"

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
CLI="${DTFS_CLI:-}"
[ -z "$CLI" ] && [ -x "$ROOT/target/release/dtfs-cli" ] && CLI="$ROOT/target/release/dtfs-cli"
[ -z "$CLI" ] && CLI="$(command -v dtfs-cli 2>/dev/null || true)"
{ [ -n "$CLI" ] && [ -x "$CLI" ]; } || skip "dtfs-cli not found (set DTFS_CLI or build target/release/dtfs-cli)"
for t in curl tar install sha256sum; do
    command -v "$t" >/dev/null 2>&1 || skip "$t not available"
done

WORK="$(mktemp -d)"
case_cleanup() { rm -rf "$WORK"; }
trap case_cleanup EXIT

# 1. help
"$CLI" update --help >/dev/null 2>&1 || fail "update --help failed"

# 2. fake file:// release containing dtfs/bin/dtfs-cli + MANIFEST
REPO="$WORK/repo"
ASSETDIR="$REPO/releases/latest/download"
STAGEBIN="$WORK/stage/dtfs/bin"
mkdir -p "$ASSETDIR" "$STAGEBIN"
printf '#!/bin/sh\necho fake-dtfs-cli\n' > "$STAGEBIN/dtfs-cli"
chmod +x "$STAGEBIN/dtfs-cli"
SUM=$(sha256sum "$STAGEBIN/dtfs-cli" | awk '{print $1}')
printf 'DTFS build manifest\nrev: test\n%s  dtfs-cli\n' "$SUM" > "$WORK/stage/dtfs/MANIFEST"
tar -C "$WORK/stage" -czf "$ASSETDIR/dtfs-linux-amd64.tar.gz" dtfs

# 3. install into a temp dir (no root, no restart)
DEST="$WORK/bin"; mkdir -p "$DEST"
if ! "$CLI" update cli --repo-url "file://$REPO" --install-dir "$DEST" --no-restart -y >"$WORK/out.log" 2>&1; then
    cat "$WORK/out.log" >&2
    fail "update cli (file://) failed"
fi
[ -f "$DEST/dtfs-cli" ] || fail "binary not installed to $DEST"
assert_eq "$SUM" "$(sha256sum "$DEST/dtfs-cli" | awk '{print $1}')" "installed sha256"

# 4. integrity gate: corrupt the staged binary but keep the old MANIFEST
DEST2="$WORK/bin2"; mkdir -p "$DEST2"
printf 'CORRUPTED' > "$STAGEBIN/dtfs-cli"
tar -C "$WORK/stage" -czf "$ASSETDIR/dtfs-linux-amd64.tar.gz" dtfs
if "$CLI" update cli --repo-url "file://$REPO" --install-dir "$DEST2" --no-restart -y >"$WORK/out2.log" 2>&1; then
    fail "corrupt binary accepted (integrity gate broken)"
fi
[ -f "$DEST2/dtfs-cli" ] && fail "corrupt binary installed despite sha256 mismatch"

pass

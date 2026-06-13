#!/usr/bin/env bash
# CO-2-4: `dtfs-cli update` must stage into an unpredictable tempdir and
# install atomically (copy to <dst>.new, then rename over dst), so a
# failed/partial install never leaves a truncated, unrunnable destination
# binary. This is local-node updater behaviour (no cluster surface), so the
# invariant is primarily unit-backed (updater.rs co_2_4_* tests); this case
# is the release-suite anchor for the same scenario via the offline
# file:// install path:
#   1. install over a PRE-EXISTING dst replaces it wholesale.
#   2. no <dst>.new residue is left behind after a successful install.
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

# fake file:// release containing dtfs/bin/dtfs-cli + MANIFEST
REPO="$WORK/repo"
ASSETDIR="$REPO/releases/latest/download"
STAGEBIN="$WORK/stage/dtfs/bin"
mkdir -p "$ASSETDIR" "$STAGEBIN"
printf '#!/bin/sh\necho fake-dtfs-cli-NEW\n' > "$STAGEBIN/dtfs-cli"
chmod +x "$STAGEBIN/dtfs-cli"
SUM=$(sha256sum "$STAGEBIN/dtfs-cli" | awk '{print $1}')
printf 'DTFS build manifest\nrev: test\n%s  dtfs-cli\n' "$SUM" > "$WORK/stage/dtfs/MANIFEST"
tar -C "$WORK/stage" -czf "$ASSETDIR/dtfs-linux-amd64.tar.gz" dtfs

# pre-existing destination binary (the "old" install) we must replace
DEST="$WORK/bin"; mkdir -p "$DEST"
printf 'OLD-INSTALLED-BINARY-CONTENT-THAT-IS-LONGER\n' > "$DEST/dtfs-cli"
chmod +x "$DEST/dtfs-cli"

echo "[1/3] update over a pre-existing dst"
if ! "$CLI" update cli --repo-url "file://$REPO" --install-dir "$DEST" --no-restart -y >"$WORK/out.log" 2>&1; then
    cat "$WORK/out.log" >&2
    fail "update cli (file://) failed"
fi

echo "[2/3] dst replaced wholesale with the new binary"
assert_eq "$SUM" "$(sha256sum "$DEST/dtfs-cli" | awk '{print $1}')" "installed sha256 == manifest"

echo "[3/3] no <dst>.new residue left behind"
[ -e "$DEST/dtfs-cli.new" ] && fail "atomic-install left a <dst>.new residue"

pass

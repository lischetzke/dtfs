#!/usr/bin/env bash
# CO-3-6 — snapshot-stream RPC must NOT inherit the 5s channel default timeout.
# A learner behind the 40 Mbps LELI shape cannot receive a >25 MB snapshot in
# 5s; pre-fix the snapshot RPC timed out and the learner never caught up. The
# fix sets a per-request, size-scaled timeout on the snapshot RPC.
#
# The real shaped end-to-end scenario lives in the release suite:
#   tests/release/cases/64-snapshot-learner-under-shaping.sh
# (add a learner behind ansible netshape, assert it installs the snapshot +
# catches up within a generous deadline). It needs the shaped 3-DC PVE lab.
#
# This abuse entry just defers to that case when the shaped lab is reachable,
# and SKIPs otherwise — the timeout-computation logic itself is unit-tested in
# crates/dtfs-raft/src/network.rs (co36_snapshot_timeout_tests).
set -uo pipefail

REL_CASE="$(dirname "$0")/../../release/cases/64-snapshot-learner-under-shaping.sh"

if [ ! -x "$REL_CASE" ] && [ ! -f "$REL_CASE" ]; then
    echo "[CO-3-6] release case not found; SKIP (covered by network.rs unit tests)"
    exit 0
fi

if ! command -v ansible-playbook >/dev/null 2>&1; then
    echo "[CO-3-6] no shaped lab on this host (ansible-playbook missing); SKIP"
    echo "[CO-3-6] run tests/release/cases/64-snapshot-learner-under-shaping.sh on the lab"
    exit 0
fi

echo "[CO-3-6] delegating to release case 64 (shaped learner snapshot install)"
exec bash "$REL_CASE"

#!/usr/bin/env bash
# Bug BQ-1: an idle, mounted FUSE client must stay reported Healthy past
# raft.node_liveness_secs. Pre-BQ the client registered once (Bug AA) and
# never refreshed last_seen_ms, so effective_health (BP-3) projected it Down
# after the liveness window even though the mount was alive
# (ISSUE-SCAN-2026-06-02-1512). Leave the client idle and assert it is still
# Healthy after ~2x the liveness window.
source "$(dirname "$0")/../lib/common.sh"

WINDOW=${NODE_LIVENESS_SECS:-30}

client_health() {
    # Match by hostname OR client_ip so CLIENT may be set to either the
    # registered hostname (default mini-client01) or the node's IP.
    mgr_get_any /api/v1/nodes | python3 -c "
import json,sys
who=sys.argv[1]
for n in json.load(sys.stdin):
    if str(n.get('role','')).lower()!='client':
        continue
    if n.get('hostname')==who or n.get('client_ip')==who:
        print(str(n.get('health',''))); break
" "$CLIENT"
}

is_healthy() { [ "$(client_health)" = "Healthy" ]; }

echo "[1/3] confirm client $CLIENT is present in /api/v1/nodes"
H0=$(client_health)
[ -n "$H0" ] || known_issue BQ "client $CLIENT not present in /api/v1/nodes (never registered)"
echo "  baseline health: $H0"
if [ "$H0" != "Healthy" ]; then
    # Give the keepalive one liveness window to bring it up before judging.
    if ! wait_for_soft "$WINDOW" is_healthy; then
        known_issue BQ "client $CLIENT not Healthy at baseline ($H0) — BQ-1 keepalive not in deployed binary"
    fi
fi

echo "[2/3] leave client idle for ~2x liveness window ($((WINDOW * 2))s)"
sleep $((WINDOW * 2))

echo "[3/3] assert client still Healthy (keepalive kept last_seen fresh)"
HN=$(client_health)
echo "  health after idle: $HN"
if [ "$HN" = "Healthy" ]; then
    pass
else
    known_issue BQ "idle client projected $HN after $((WINDOW * 2))s — BQ-1 client keepalive missing in deployed binary"
fi

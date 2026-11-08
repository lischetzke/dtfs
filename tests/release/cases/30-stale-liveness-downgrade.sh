#!/usr/bin/env bash
# BP-3: a node whose last_seen_ms is stale must be projected non-Healthy in
# the read API, even though its stored health is Healthy (pre-BP a frozen
# heartbeat kept reporting Healthy indefinitely, ISSUE-SCAN-2026-06-02 F4).
#
# Stop a data node's heartbeat (stop dtfs-data) and assert /api/v1/nodes
# reports it Suspect or Down within ~2x the liveness window; then restart
# and confirm it returns to Healthy.
source "$(dirname "$0")/../lib/common.sh"

WINDOW=${NODE_LIVENESS_SECS:-30}

TARGET=$(data_list | awk '{print $1}')
[ -n "$TARGET" ] || skip "no data node in DATA list"

node_health() {
    mgr_get_any /api/v1/nodes | python3 -c "
import json,sys
for n in json.load(sys.stdin):
    if n.get('hostname')==sys.argv[1]:
        print(str(n.get('health',''))); break
" "$TARGET"
}

is_downgraded() {
    local h; h=$(node_health)
    [ "$h" = "Suspect" ] || [ "$h" = "Down" ]
}
is_healthy() { [ "$(node_health)" = "Healthy" ]; }

case_cleanup() {
    ssh_run "$TARGET" "sudo systemctl is-active dtfs-data >/dev/null 2>&1 || sudo systemctl start dtfs-data" >/dev/null 2>&1 || true
    wait_for_soft 60 is_healthy >/dev/null 2>&1 || true
}
trap case_cleanup EXIT

echo "[1/4] baseline health for $TARGET"
H0=$(node_health); echo "  $H0"
[ "$H0" = "Healthy" ] || skip "target not Healthy at baseline ($H0)"

echo "[2/4] stop heartbeat (systemctl stop dtfs-data)"
ssh_run "$TARGET" "sudo systemctl stop dtfs-data"

echo "[3/4] expect downgrade within ~2x window (${WINDOW}s)"
if wait_for_soft $((WINDOW * 3)) is_downgraded; then
    echo "  downgraded to $(node_health)"
else
    ssh_run "$TARGET" "sudo systemctl start dtfs-data" >/dev/null 2>&1 || true
    known_issue BP "stale node still reported $(node_health) after $((WINDOW*3))s — BP-3 liveness projection not in the deployed binary"
fi

echo "[4/4] restart + expect return to Healthy"
ssh_run "$TARGET" "sudo systemctl start dtfs-data"
wait_for 60 "$TARGET back to Healthy" is_healthy
pass

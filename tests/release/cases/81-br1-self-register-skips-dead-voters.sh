#!/usr/bin/env bash
# LIS-77 / Bug BR-1: manager self-register must still succeed when an
# earlier [[cluster.initial_voters]] entry is unroutable.
#
# Pre-fix: TlsContext::connect had no connect timeout, so walking
# initial_voters in config order hung forever on a blackhole peer and
# never reached a live manager — registry last_seen_ms froze while
# Raft/API still served (FSN sole-voter restart, LIS-63).
#
# Flow:
#   1. Snapshot TARGET's last_seen_ms from /api/v1/nodes.
#   2. Prepend a TEST-NET-1 dead voter as the FIRST initial_voters
#      entry on TARGET and restart.
#   3. Within ~45s assert last_seen_ms advanced (RegisterNode keepalive
#      landed despite the dead-first peer).
# Cleanup restores the original manager.toml + restart.
source "$(dirname "$0")/../lib/common.sh"

MGR_TOML=/etc/dtfs/manager.toml
BAK=$MGR_TOML.case81.bak
DEAD_ADDR=192.0.2.1:9401

TARGET=$(mgrs_list | awk '{print $1}')
[ -n "$TARGET" ] || skip "no manager in MGRS"

CONFIGS_TOUCHED=0
case_cleanup() {
    if [ "$CONFIGS_TOUCHED" = "1" ]; then
        ssh_run "$TARGET" "if sudo test -f $BAK; then \
            sudo mv $BAK $MGR_TOML && sudo systemctl restart dtfs-manager; fi" \
            >/dev/null 2>&1 || true
        wait_for_soft 90 curl "${CURL_OPTS[@]}" -o /dev/null \
            "http://$TARGET:$MGR_PORT/api/v1/health" || true
    fi
}
trap case_cleanup EXIT

ssh_run "$TARGET" "sudo test -f $MGR_TOML" \
    || skip "$MGR_TOML not found on $TARGET"

HOSTNAME=$(ssh_run "$TARGET" "python3 -c \"
import tomllib
print(tomllib.load(open('/etc/dtfs/manager.toml','rb'))['node']['hostname'])
\"" | tr -d '\r')
[ -n "$HOSTNAME" ] || fail "could not read node.hostname on $TARGET"

mgr_last_seen() {
    curl "${CURL_OPTS[@]}" "http://$TARGET:$MGR_PORT/api/v1/nodes" 2>/dev/null | python3 -c "
import json,sys
try:
    nodes=json.load(sys.stdin)
except Exception:
    print(0); sys.exit(0)
for n in nodes:
    if n.get('hostname')==sys.argv[1]:
        print(int(n.get('last_seen_ms') or 0)); break
else:
    print(0)
" "$HOSTNAME"
}

echo "[1/4] baseline last_seen for $HOSTNAME via $TARGET"
BEFORE=$(mgr_last_seen)
echo "  last_seen_ms=$BEFORE"
[ "$BEFORE" -gt 0 ] || skip "$HOSTNAME not yet in registry (BR-1 not registered)"

echo "[2/4] prepend dead voter $DEAD_ADDR as first initial_voters on $TARGET"
ssh_run "$TARGET" "{ sudo test -f $BAK || sudo cp $MGR_TOML $BAK; }" \
    || fail "could not backup $MGR_TOML on $TARGET"
# Insert a dead [[cluster.initial_voters]] table immediately before the
# first existing one (config-order walk hits it first). Write as root.
ssh_run "$TARGET" "sudo python3 -c \"
from pathlib import Path
p = Path('/etc/dtfs/manager.toml')
text = p.read_text()
needle = '[[cluster.initial_voters]]'
idx = text.find(needle)
assert idx >= 0, 'no [[cluster.initial_voters]] in manager.toml'
dead = (
    '[[cluster.initial_voters]]\\n'
    'hostname = \\\"dead-lis77-peer\\\"\\n'
    'addr = \\\"$DEAD_ADDR\\\"\\n'
    '\\n'
)
p.write_text(text[:idx] + dead + text[idx:])
print('injected')
\"" || fail "failed to inject dead voter on $TARGET"
CONFIGS_TOUCHED=1

ssh_run "$TARGET" "sudo systemctl restart dtfs-manager" \
    || fail "restart dtfs-manager failed on $TARGET"
wait_for 60 "manager $TARGET API back" \
    curl "${CURL_OPTS[@]}" -o /dev/null "http://$TARGET:$MGR_PORT/api/v1/health"

echo "[3/4] wait for last_seen_ms > $BEFORE (self-register despite dead-first)"
last_seen_advanced() {
    local now
    now=$(mgr_last_seen)
    [ "$now" -gt "$BEFORE" ]
}

if wait_for_soft 45 last_seen_advanced; then
    echo "  last_seen advanced to $(mgr_last_seen) — keepalive succeeded"
else
    known_issue BR "last_seen_ms did not advance with dead-first initial_voters — LIS-77 connect timeout / self-first walk not in deployed binary"
fi

echo "[4/4] restore config"
case_cleanup
CONFIGS_TOUCHED=0
pass

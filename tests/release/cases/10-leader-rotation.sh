#!/usr/bin/env bash
# BE-14: stop the current Raft leader, verify quorum holds + new
# leader elected within 30s. Skips when the lab is single-node.
source "$(dirname "$0")/../lib/common.sh"

LEADER_HOST=$(mgr_get_any /api/v1/nodes | python3 -c "
import json,sys
for n in json.load(sys.stdin):
    if n.get('is_leader'): print(n.get('hostname','')); break
")
[ -n "$LEADER_HOST" ] || skip "no leader reported yet"

# Quorum check
N=$(echo "$(mgrs_list)" | wc -w)
[ "$N" -ge 3 ] || skip "need 3+ managers for safe leader rotation; got $N"

echo "[1/3] stop dtfs-manager on $LEADER_HOST"
ssh_run "$LEADER_HOST" "sudo systemctl stop dtfs-manager" || fail "stop failed"

echo "[2/3] wait up to 30s for a new leader"
NEW=""
for _ in $(seq 1 30); do
    sleep 1
    NEW=$(mgr_get_any /api/v1/nodes | python3 -c "
import json,sys
for n in json.load(sys.stdin):
    if n.get('is_leader') and n.get('hostname')!=sys.argv[1]:
        print(n.get('hostname','')); break
" "$LEADER_HOST" 2>/dev/null)
    [ -n "$NEW" ] && break
done

echo "[3/3] restart old leader (defensive cleanup)"
ssh_run "$LEADER_HOST" "sudo systemctl start dtfs-manager" || true
[ -n "$NEW" ] || fail "no new leader elected within 30s"
echo "  new leader: $NEW"
pass

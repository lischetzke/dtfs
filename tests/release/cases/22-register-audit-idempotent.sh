#!/usr/bin/env bash
# BJ-2: re-registering an unchanged node must not grow the audit log.
# Restart a data node so it re-registers with identical material info,
# and assert the count of `RegisterNode: <its id>` audit rows does not
# increase (Bug BJ idempotent-audit fix). Rows are counted for the
# target node's id ONLY, so unrelated registrations (e.g. a client) do
# not pollute the delta.
#
# Pre-BJ-1 binary: an identical re-register still emits a row → the case
# reports KNOWN_ISSUE [BJ] (auto-promotes to PASS once BJ-1 ships).
source "$(dirname "$0")/../lib/common.sh"

TARGET_HOST=$(data_list | awk '{print $1}')
[ -n "$TARGET_HOST" ] || skip "no data node in DATA list"

# Resolve the target's node_id (audit summary is "RegisterNode: <id>").
NID=$(mgr_get_any /api/v1/nodes | python3 -c "
import json,sys
for n in json.load(sys.stdin):
    if n.get('hostname')==sys.argv[1]:
        print(n.get('node_id')); break
" "$TARGET_HOST")
[ -n "$NID" ] || skip "could not resolve node_id for $TARGET_HOST"

count_reg() {
    mgr_get_any "/api/v1/audit?limit=400" | python3 -c "
import json,sys
d=json.load(sys.stdin)
rows=d if isinstance(d,list) else d.get('entries',d.get('rows',[]))
print(sum(1 for r in rows if str(r.get('summary','')).startswith('RegisterNode: '+sys.argv[1])))
" "$NID"
}

node_healthy() {
    mgr_get_any /api/v1/nodes | python3 -c "
import json,sys
for n in json.load(sys.stdin):
    if n.get('hostname')==sys.argv[1]:
        sys.exit(0 if str(n.get('health','')).lower()=='healthy' else 1)
sys.exit(1)
" "$TARGET_HOST"
}

case_cleanup() { wait_for_soft 30 node_healthy >/dev/null 2>&1 || true; }
trap case_cleanup EXIT

echo "[1/3] baseline RegisterNode audit count for $TARGET_HOST ($NID)"
BEFORE=$(count_reg)
echo "  before=$BEFORE"

echo "[2/3] restart $TARGET_HOST (identical re-registration)"
ssh_run "$TARGET_HOST" "sudo systemctl restart dtfs-data"
wait_for 60 "$TARGET_HOST healthy again after restart" node_healthy
sleep 3 # let the RegisterNode apply + audit (if any) land

echo "[3/3] compare audit count"
AFTER=$(count_reg)
DELTA=$(( AFTER - BEFORE ))
echo "  after=$AFTER delta=$DELTA"

if [ "$DELTA" -le 0 ]; then
    pass
else
    known_issue BJ "identical re-registration added $DELTA RegisterNode audit row(s) — BJ-1 idempotent-audit not in the deployed manager binary"
fi

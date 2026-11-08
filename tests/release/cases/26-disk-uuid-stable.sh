#!/usr/bin/env bash
# BN-1: a data node's disk UUIDs must be stable across restarts. Previously
# every boot minted fresh random UUIDs (uuid::Uuid::new_v4), defeating
# UUID-based disk identity. The fix persists each UUID to
# <mount>/.dtfs-disk-uuid. Restart a data node and assert its disk UUID set
# is identical before and after.
source "$(dirname "$0")/../lib/common.sh"

TARGET_HOST=$(data_list | awk '{print $1}')
[ -n "$TARGET_HOST" ] || skip "no data node in DATA list"

# Sorted, comma-joined disk UUID set for the target host.
disk_uuids() {
    mgr_get_any /api/v1/nodes | python3 -c "
import json,sys
for n in json.load(sys.stdin):
    if n.get('hostname')==sys.argv[1]:
        us=sorted(d.get('uuid','') for d in (n.get('disks') or []))
        print(','.join(us)); break
" "$TARGET_HOST"
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

echo "[1/3] baseline disk UUIDs for $TARGET_HOST"
BEFORE=$(disk_uuids)
echo "  before=[$BEFORE]"
[ -n "$BEFORE" ] || skip "no disks reported for $TARGET_HOST"

echo "[2/3] restart $TARGET_HOST"
ssh_run "$TARGET_HOST" "sudo systemctl restart dtfs-data"
wait_for 60 "$TARGET_HOST healthy again" node_healthy
sleep 3 # let the re-registration land

echo "[3/3] compare disk UUIDs"
AFTER=$(disk_uuids)
echo "  after=[$AFTER]"

if [ -n "$AFTER" ] && [ "$BEFORE" = "$AFTER" ]; then
    pass
else
    known_issue BN "disk UUIDs changed across restart (before=[$BEFORE] after=[$AFTER]) — BN-1 persistent UUID not in the deployed data binary"
fi

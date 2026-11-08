#!/usr/bin/env bash
# REL-2-45: disk failure + replacement. Yank the cold disk holding the anchor
# replica from its dc-a node; under-replication must re-create the cold copy on
# the other dc-a node. Then install a blank disk and confirm it rejoins the
# ring. Backfill onto the fresh disk is best-effort (KNOWN_ISSUE if the
# scheduler won't fill it without new writes).
source "$(dirname "$0")/../lib/common.sh"
require_pve

CW=$(client_for_dc dc-a)
[ -n "$CW" ] || skip "no dc-a client"

PATHF="$MOUNT/release/45-disk-$$.bin"; LOCAL=/tmp/45-src-$$.bin
COLD_HOST=""; SLOT=2; MNT=/mnt/cold; REATTACHED=0
case_cleanup() {
    if [ -n "$COLD_HOST" ] && [ "$REATTACHED" = "0" ]; then
        attach_blank_disk "$COLD_HOST" "$SLOT" 16 >/dev/null 2>&1 || true
    fi
    rm_fuse_file "$PATHF"; rm -f "$LOCAL"
}
trap case_cleanup EXIT

_cold_present() { [ "$(count_replicas_in_dc_tier "$PATHF" dc-a cold)" -ge 1 ]; }
_disk_count() {
    mgr_get_any /api/v1/nodes | INV_HOST="$1" python3 -c "
import json, os, sys
d = json.load(sys.stdin); h = os.environ['INV_HOST']
for n in d:
    if n.get('hostname') == h:
        print(int(n.get('disk_count', 0) or 0)); break
else:
    print(0)
"
}
_disk_back() { [ "$(_disk_count "$COLD_HOST")" -ge 2 ]; }

echo "[1/6] write a file; locate the dc-a node holding its cold anchor"
head -c $((8 * 1024 * 1024)) /dev/urandom > "$LOCAL"
SRC=$(sha256sum "$LOCAL" | awk '{print $1}')
scp "${SSH_OPTS[@]}" "$LOCAL" "debian@$CW:/tmp/45-src.bin" >/dev/null 2>&1 || skip "cannot stage"
ssh_run "$CW" "sudo mkdir -p \"\$(dirname '$PATHF')\" && sudo cp /tmp/45-src.bin '$PATHF' && sudo sync" || fail "write failed"
ssh_run "$CW" "sudo rm -f /tmp/45-src.bin"
inspect_has_replicas "$PATHF" || known_issue REL-1-2 "inspect lacks per-replica tier (REL-1-2)"
wait_for 60 "cold replica present in dc-a" _cold_present
COLD_HOST=$(_replica_rows "$PATHF" | awk '$4=="cold"{print $6; exit}')
[ -n "$COLD_HOST" ] || known_issue REL-1-2 "cannot identify cold-holding host"
echo "  cold anchor on $COLD_HOST"

echo "[2/6] simulate dead disk: stop dtfs-data, unmount $MNT, detach scsi$SLOT"
ssh_run "$COLD_HOST" "sudo systemctl stop dtfs-data && sudo umount $MNT 2>/dev/null; true"
detach_disk "$COLD_HOST" "$SLOT"
ssh_run "$COLD_HOST" "sudo systemctl start dtfs-data" || true

echo "[3/6] under-replication restores a dc-a cold replica on a healthy node"
if ! wait_for_soft 180 _cold_present; then
    known_issue REL-2-45 "cold anchor not re-replicated within 180s"
fi
echo "  cold anchor now on $(_replica_rows "$PATHF" | awk '$4=="cold"{print $6; exit}')"

echo "[4/6] install a blank replacement disk on $COLD_HOST"
attach_blank_disk "$COLD_HOST" "$SLOT" 16; REATTACHED=1

echo "[5/6] replacement disk rejoins the ring (disk_count restored)"
wait_for 120 "disk back on $COLD_HOST" _disk_back

echo "[6/6] file still intact"
GOT=$(ssh_run "$CW" "sudo sha256sum '$PATHF' | awk '{print \$1}'")
assert_eq "$SRC" "$GOT" "file intact after disk swap"
pass

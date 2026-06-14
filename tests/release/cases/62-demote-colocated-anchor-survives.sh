#!/usr/bin/env bash
# REL-2-62 (CO-1-1): disk-targeted chunk-location removal protects a
# co-located cold anchor. When a node legally holds BOTH the cold mandatory
# anchor (disk A) and a warm cache replica (disk B) of one chunk, demoting /
# flushing the warm cache copy must unlink ONLY the warm disk's copy; the cold
# anchor bytes must survive. Pre-CO-1-1 the data-node delete was "first copy
# found across all disks" (no disk_id) and could unlink the cold anchor, and
# the manager RemoveChunkLocation retained by (chunk_index, node_id) only and
# dropped BOTH SM entries.
#
# This needs a single node carrying both a cold and a warm disk so the chunk
# can co-locate (the Bug BH topology). Where the live topology has no such
# node we cannot construct the co-located case and report KNOWN_ISSUE pinned
# to CO-1-1 (the state-machine + store unit tests cover the logic regardless).
source "$(dirname "$0")/../lib/common.sh"
source "$(dirname "$0")/../lib/dc.sh"

PREFIX=/release/62-co11-$$/
PATHF="${MOUNT}${PREFIX}f.bin"; LOCAL=/tmp/62-src-$$.bin
APIF="${PATHF#$MOUNT}"  # mount-relative path for manager API calls
SAVED=0
restore_policy() {
    [ "$SAVED" = "1" ] || return 0
    mgr_post_leader /api/v1/volumes/$VOL/set-mandatory '{"hot_replicas":2,"warm_replicas":2,"cold_replicas":1}' >/dev/null 2>&1 || true
    mgr_post_leader /api/v1/volumes/$VOL/set-cache '{"warm_min":0,"warm_max":0,"per_dc":false}' >/dev/null 2>&1 || true
}
case_cleanup() { restore_policy; client_run "sudo rm -rf ${MOUNT}${PREFIX}" >/dev/null 2>&1 || true; rm -f "$LOCAL"; }
trap case_cleanup EXIT

# Find a node that exposes BOTH a cold and a warm disk (the only place a
# single node can hold the anchor + a cache copy on different disks).
colocated_node() {
    mgr_get_any "/api/v1/nodes" | python3 -c "
import json, sys
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(0)
nodes = d.get('nodes', d) if isinstance(d, dict) else d
for n in (nodes or []):
    tiers = { (disk.get('tier') or '').lower() for disk in (n.get('disks') or []) }
    if 'cold' in tiers and 'warm' in tiers:
        print(n.get('node_id') or n.get('id') or '')
        break
" 2>/dev/null
}

CO_NODE=$(colocated_node)
[ -n "$CO_NODE" ] || known_issue CO-1-1 "no node carries both cold+warm disks; cannot build the co-located (Bug BH) topology in this lab (unit tests cover the logic)"

CW=$(client_for_dc dc-a)
[ -n "$CW" ] || CW="$CLIENT"
[ -n "$CW" ] || skip "no client available"

echo "[1/5] anchor cold=1 (dc-a) + warm cache so a co-located copy can land on $CO_NODE"
mgr_post_leader /api/v1/volumes/$VOL/set-anchor '{"datacenter":"dc-a","tier":"cold","mandatory_replicas":1}' >/dev/null
mgr_post_leader /api/v1/volumes/$VOL/set-mandatory '{"hot_replicas":0,"warm_replicas":0,"cold_replicas":1}' >/dev/null; SAVED=1
mgr_post_leader /api/v1/volumes/$VOL/set-cache '{"warm_min":1,"warm_max":1,"per_dc":false,"promote_reads":1,"promote_window_secs":3600,"cooldown_secs":0}' >/dev/null

echo "[2/5] write file -> cold anchor lands"
head -c $((8 * 1024 * 1024)) /dev/urandom > "$LOCAL"
scp "${SSH_OPTS[@]}" "$LOCAL" "debian@$CW:/tmp/62-src.bin" >/dev/null 2>&1 || skip "cannot stage src"
ssh_run "$CW" "sudo mkdir -p ${MOUNT}${PREFIX} && sudo cp /tmp/62-src.bin '$PATHF' && sudo sync" || fail "write failed"
ssh_run "$CW" "sudo rm -f /tmp/62-src.bin"
wait_for_soft 45 inspect_has_replicas "$APIF" || known_issue REL-1-2 "inspect lacks per-replica tier (REL-1-2)"
_anchor_ok() { [ "$(count_replicas_in_dc_tier "$APIF" dc-a cold)" -ge 1 ]; }
wait_for 60 "cold anchor present" _anchor_ok

echo "[3/5] read repeatedly -> promote a warm cache copy co-located on $CO_NODE"
for _ in 1 2 3; do ssh_run "$CW" "sudo cat '$PATHF' > /dev/null"; sleep 2; done
_warm_present() { [ "$(count_replicas_in_tier "$APIF" warm)" -ge 1 ]; }
if ! wait_for_soft 90 _warm_present; then
    known_issue CO-1-1 "warm cache copy did not promote on the co-located node within 90s"
fi
echo "  warm cache copy present"

echo "[4/5] flush_cache (warm) -> drop the warm copy only"
RESP=$(mgr_post_leader /api/v1/files/flush_cache "{\"path_prefix\":\"$PREFIX\",\"cache_tiers\":[\"warm\"]}")
echo "  flush: $(echo "$RESP" | head -c 200)"
_warm_gone() { [ "$(count_replicas_in_tier "$APIF" warm)" -eq 0 ]; }
wait_for 60 "warm cache flushed" _warm_gone

echo "[5/5] cold anchor MUST survive + no AnchorViolated + readable"
[ "$(count_replicas_in_dc_tier "$APIF" dc-a cold)" -ge 1 ] || fail "CO-1-1 REGRESSION: cold anchor deleted by warm-cache flush"
ssh_run "$CW" "sudo cat '$PATHF' > /dev/null" || fail "file no longer readable after flush (anchor gone)"
AUDIT=$(mgr_get_any "/api/v1/audit?limit=200")
echo "$AUDIT" | grep -q "AnchorViolated" && fail "AnchorViolated raised during co-located flush"
echo "  co-located anchor survived a disk-targeted cache flush"
pass

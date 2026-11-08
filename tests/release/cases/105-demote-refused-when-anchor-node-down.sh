#!/usr/bin/env bash
# CR-3-1 (MGR-2): the cache-flush/demote planners must resolve every
# placement's liveness through `live_replica_tier` before counting it as
# the surviving mandatory/anchor copy. Pre-fix, `plan_flush_cache`'s
# `mandatory_survives` and `plan_chunk_demotions`'s `mandatory_still_present`
# counted a replica on a Down node as a live survivor, so a warm cache
# copy could be flushed even though the cold anchor on a Down node was
# the ONLY other placement — leaving zero servable replicas.
#
# Shape of the probe: anchor a volume cold=1 in dc-a, enable a warm cache
# policy, write a file so the cold anchor lands, read it repeatedly to
# promote a warm cache copy, then stop the cold anchor's data node
# (systemctl stop dtfs-data -> Down). A subsequent flush_cache(warm) call
# must now be REFUSED (mandatory_refusals >= 1 in the report / warm copy
# still present), not silently drop the last live replica. Restarting the
# node and repeating the flush must then succeed (mandatory copy live
# again).
#
# The state-machine-level regression is
# scheduler::bug_av_flush_cache_tests::cr_3_1_flush_refused_when_mandatory_replica_is_on_dead_node
# and tiering::audit_co_tests::cr_3_1_demote_refused_when_only_surviving_mandatory_replica_is_dead.
source "$(dirname "$0")/../lib/common.sh"
source "$(dirname "$0")/../lib/dc.sh"

PREFIX=/release/105-cr31-$$/
PATHF="${MOUNT}${PREFIX}f.bin"; LOCAL=/tmp/105-src-$$.bin
APIF="${PATHF#$MOUNT}"
SAVED=0
ANCHOR_HOST=""

restore_policy() {
    [ "$SAVED" = "1" ] || return 0
    mgr_post_leader /api/v1/volumes/$VOL/set-mandatory '{"hot_replicas":2,"warm_replicas":2,"cold_replicas":1}' >/dev/null 2>&1 || true
    mgr_post_leader /api/v1/volumes/$VOL/set-cache '{"warm_min":0,"warm_max":0,"per_dc":false}' >/dev/null 2>&1 || true
}
case_cleanup() {
    if [ -n "$ANCHOR_HOST" ]; then
        ssh_run "$ANCHOR_HOST" "sudo systemctl is-active dtfs-data >/dev/null 2>&1 || sudo systemctl start dtfs-data" >/dev/null 2>&1 || true
    fi
    restore_policy
    client_run "sudo rm -rf ${MOUNT}${PREFIX}" >/dev/null 2>&1 || true
    rm -f "$LOCAL"
}
trap case_cleanup EXIT

anchor_node_host() {
    # First cold-tier node in dc-a (the anchor DC).
    mgr_get_any "/api/v1/nodes" | python3 -c "
import json, sys
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(0)
nodes = d.get('nodes', d) if isinstance(d, dict) else d
for n in (nodes or []):
    loc = n.get('location') or {}
    if (loc.get('datacenter') or '').lower() != 'dc-a':
        continue
    tiers = { (disk.get('tier') or '').lower() for disk in (n.get('disks') or []) }
    if 'cold' in tiers:
        print(n.get('hostname') or '')
        break
" 2>/dev/null
}

CW=$(client_for_dc dc-a)
[ -n "$CW" ] || CW="$CLIENT"
[ -n "$CW" ] || skip "no client available"

echo "[1/6] anchor cold=1 (dc-a) + warm cache so a cache copy can promote"
mgr_post_leader /api/v1/volumes/$VOL/set-anchor '{"datacenter":"dc-a","tier":"cold","mandatory_replicas":1}' >/dev/null
mgr_post_leader /api/v1/volumes/$VOL/set-mandatory '{"hot_replicas":0,"warm_replicas":0,"cold_replicas":1}' >/dev/null; SAVED=1
mgr_post_leader /api/v1/volumes/$VOL/set-cache '{"warm_min":1,"warm_max":1,"per_dc":false,"promote_reads":1,"promote_window_secs":3600,"cooldown_secs":0}' >/dev/null

echo "[2/6] write file -> cold anchor lands"
head -c $((8 * 1024 * 1024)) /dev/urandom > "$LOCAL"
scp "${SSH_OPTS[@]}" "$LOCAL" "debian@$CW:/tmp/105-src.bin" >/dev/null 2>&1 || skip "cannot stage src"
ssh_run "$CW" "sudo mkdir -p ${MOUNT}${PREFIX} && sudo cp /tmp/105-src.bin '$PATHF' && sudo sync" || fail "write failed"
ssh_run "$CW" "sudo rm -f /tmp/105-src.bin"
wait_for_soft 45 inspect_has_replicas "$APIF" || known_issue REL-1-2 "inspect lacks per-replica tier (REL-1-2)"
_anchor_ok() { [ "$(count_replicas_in_dc_tier "$APIF" dc-a cold)" -ge 1 ]; }
wait_for 60 "cold anchor present" _anchor_ok

echo "[3/6] read repeatedly -> promote a warm cache copy"
for _ in 1 2 3; do ssh_run "$CW" "sudo cat '$PATHF' > /dev/null"; sleep 2; done
_warm_present() { [ "$(count_replicas_in_tier "$APIF" warm)" -ge 1 ]; }
if ! wait_for_soft 90 _warm_present; then
    known_issue CR-3-1 "warm cache copy did not promote within 90s"
fi
echo "  warm cache copy present"

echo "[4/6] stop the cold anchor's data node (dc-a cold -> Down)"
ANCHOR_HOST=$(anchor_node_host)
[ -n "$ANCHOR_HOST" ] || known_issue CR-3-1 "no dc-a cold-tier node found; cannot build the Down-anchor topology"
ssh_run "$ANCHOR_HOST" "sudo systemctl stop dtfs-data"
node_health() {
    mgr_get_any /api/v1/nodes | python3 -c "
import json,sys
for n in json.load(sys.stdin):
    if n.get('hostname')==sys.argv[1]:
        print(str(n.get('health','')).lower()); break
" "$ANCHOR_HOST"
}
_anchor_down() { [ "$(node_health)" = "down" ] || [ "$(node_health)" = "suspect" ]; }
if ! wait_for_soft 90 _anchor_down; then
    ssh_run "$ANCHOR_HOST" "sudo systemctl start dtfs-data" >/dev/null 2>&1 || true
    known_issue CR-3-1 "anchor node never reported down/suspect within 90s"
fi
echo "  anchor node health: $(node_health)"

echo "[5/6] flush_cache(warm) must be REFUSED now (only live replica is warm)"
RESP=$(mgr_post_leader /api/v1/files/flush_cache "{\"path_prefix\":\"$PREFIX\",\"cache_tiers\":[\"warm\"]}")
echo "  flush: $(echo "$RESP" | head -c 300)"
_warm_still_present() { [ "$(count_replicas_in_tier "$APIF" warm)" -ge 1 ]; }
if ! _warm_still_present; then
    fail "CR-3-1 REGRESSION: warm cache copy dropped while the cold anchor was Down (zero live replicas left)"
fi
REFUSALS=$(echo "$RESP" | python3 -c "import json,sys; print(json.load(sys.stdin).get('mandatory_refusals',0))" 2>/dev/null || echo 0)
[ "${REFUSALS:-0}" -ge 1 ] || echo "  (note: mandatory_refusals not surfaced as >=1 in the report; warm copy survival is the authoritative check)"

echo "[6/6] restart anchor node -> flush now succeeds (mandatory copy live again)"
ssh_run "$ANCHOR_HOST" "sudo systemctl start dtfs-data"
_anchor_healthy() { [ "$(node_health)" = "healthy" ]; }
wait_for 90 "anchor node back to healthy" _anchor_healthy
_anchor_ok
RESP2=$(mgr_post_leader /api/v1/files/flush_cache "{\"path_prefix\":\"$PREFIX\",\"cache_tiers\":[\"warm\"]}")
echo "  flush after recovery: $(echo "$RESP2" | head -c 300)"
_warm_gone() { [ "$(count_replicas_in_tier "$APIF" warm)" -eq 0 ]; }
wait_for 60 "warm cache flushed once the anchor is live again" _warm_gone

pass

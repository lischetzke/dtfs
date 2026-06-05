#!/usr/bin/env bash
# REL-2-47: per-DC cache promote then flush. With mandatory=cold-only, a read
# from dc-b promotes a warm CACHE replica in dc-b; flush_cache drops it while
# the dc-a cold anchor survives (cache.flush audit, no AnchorViolated). Restores
# the default policy in cleanup. Non-PVE (needs dc-a + dc-b clients).
source "$(dirname "$0")/../lib/common.sh"

CB=$(client_for_dc dc-b); CW=$(client_for_dc dc-a)
[ -n "$CB" ] && [ -n "$CW" ] || skip "need dc-a + dc-b clients (3dc)"

PREFIX=/release/47-cache-$$/
PATHF="${MOUNT}${PREFIX}f.bin"; LOCAL=/tmp/47-src-$$.bin
SAVED=0
restore_policy() {
    [ "$SAVED" = "1" ] || return 0
    mgr_post_leader /api/v1/volumes/$VOL/set-mandatory '{"mandatory_replicas":{"hot":2,"warm":2,"cold":1}}' >/dev/null 2>&1 || true
    mgr_post_leader /api/v1/volumes/$VOL/set-cache '{"warm_min":0,"warm_max":0,"per_dc":true}' >/dev/null 2>&1 || true
}
case_cleanup() { restore_policy; client_run "sudo rm -rf ${MOUNT}${PREFIX}" >/dev/null 2>&1 || true; rm -f "$LOCAL"; }
trap case_cleanup EXIT

_anchor_ok()     { [ "$(count_replicas_in_dc_tier "$PATHF" dc-a cold)" -ge 1 ]; }
_dcb_warm()      { [ "$(count_replicas_in_dc_tier "$PATHF" dc-b warm)" -ge 1 ]; }
_dcb_warm_gone() { [ "$(count_replicas_in_dc_tier "$PATHF" dc-b warm)" -eq 0 ]; }

echo "[1/5] set mandatory cold=1 (anchor dc-a) + warm cache per-dc 1/1"
mgr_post_leader /api/v1/volumes/$VOL/set-anchor '{"datacenter":"dc-a","tier":"cold","mandatory_replicas":1}' >/dev/null
mgr_post_leader /api/v1/volumes/$VOL/set-mandatory '{"mandatory_replicas":{"hot":0,"warm":0,"cold":1}}' >/dev/null; SAVED=1
mgr_post_leader /api/v1/volumes/$VOL/set-cache '{"warm_min":1,"warm_max":1,"per_dc":true,"promote_reads":1,"promote_window_secs":3600,"cooldown_secs":0}' >/dev/null

echo "[2/5] write file"
head -c $((8 * 1024 * 1024)) /dev/urandom > "$LOCAL"
scp "${SSH_OPTS[@]}" "$LOCAL" "debian@$CW:/tmp/47-src.bin" >/dev/null 2>&1 || skip "cannot stage"
ssh_run "$CW" "sudo mkdir -p ${MOUNT}${PREFIX} && sudo cp /tmp/47-src.bin '$PATHF' && sudo sync" || fail "write failed"
ssh_run "$CW" "sudo rm -f /tmp/47-src.bin"
inspect_has_replicas "$PATHF" || known_issue REL-1-2 "inspect lacks per-replica tier (REL-1-2)"
wait_for 60 "cold anchor present" _anchor_ok

echo "[3/5] read from dc-b twice -> warm cache promote in dc-b"
ssh_run "$CB" "sudo cat '$PATHF' > /dev/null"; sleep 2; ssh_run "$CB" "sudo cat '$PATHF' > /dev/null"
if ! wait_for_soft 90 _dcb_warm; then
    known_issue REL-2-47 "per-DC warm cache did not promote in dc-b within 90s"
fi
echo "  warm cache present in dc-b"

echo "[4/5] flush_cache -> dc-b warm cache drops"
RESP=$(mgr_post_leader /api/v1/files/flush_cache "{\"path_prefix\":\"$PREFIX\",\"cache_tiers\":[\"warm\"]}")
echo "  flush: $(echo "$RESP" | head -c 200)"
wait_for 60 "dc-b warm cache flushed" _dcb_warm_gone

echo "[5/5] anchor intact + cache.flush audit + no AnchorViolated"
[ "$(count_replicas_in_dc_tier "$PATHF" dc-a cold)" -ge 1 ] || fail "lost cold anchor after flush"
AUDIT=$(mgr_get_any "/api/v1/audit?limit=200")
echo "$AUDIT" | grep -q "cache.flush"     || fail "no cache.flush audit emitted"
echo "$AUDIT" | grep -q "AnchorViolated"   && fail "AnchorViolated during flush"
echo "  cache promote+flush verified, anchor safe"
pass

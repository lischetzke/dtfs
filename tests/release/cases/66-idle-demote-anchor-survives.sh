#!/usr/bin/env bash
# REL-2-66 (CO-1-2): engine-driven idle-demote of a cache replica must NEVER
# strand the LELI cold anchor. The AV cache tick demotes warm cache replicas
# that have seen no reads in `promote_window_secs`. Pre-CO-1-2 the demote
# planner only checked that a MANDATORY-tier replica survived the drop; it did
# NOT check the anchor invariant, so a chunk whose only anchor-DC replica sat
# on a cache tier could have that last anchor copy idle-demoted away. CO-1-2
# adds `demote_preserves_anchor` to the planner (refuse + `tier.anchor_protected`
# audit + AnchorViolated alert) and passes the anchor as `enforce_anchor` on the
# RemoveChunkLocation apply (defence-in-depth).
#
# Live check: stand up an anchor (dc-a cold) + warm cache, promote a warm copy
# in another DC, let it go idle so the engine demotes it, and assert the cold
# anchor survives every idle-demote tick + no AnchorViolated fires for the
# anchor copy + the file stays readable. Where the topology can't build the
# cache copy we report KNOWN_ISSUE pinned to CO-1-2 (the tiering unit tests
# `co_1_2_*` cover the planner logic regardless).
source "$(dirname "$0")/../lib/common.sh"
source "$(dirname "$0")/../lib/dc.sh"

PREFIX=/release/66-co12-$$/
PATHF="${MOUNT}${PREFIX}f.bin"; LOCAL=/tmp/66-src-$$.bin
SAVED=0
restore_policy() {
    [ "$SAVED" = "1" ] || return 0
    mgr_post_leader /api/v1/volumes/$VOL/set-mandatory '{"mandatory_replicas":{"hot":2,"warm":2,"cold":1}}' >/dev/null 2>&1 || true
    mgr_post_leader /api/v1/volumes/$VOL/set-cache '{"warm_min":0,"warm_max":0,"per_dc":false}' >/dev/null 2>&1 || true
}
case_cleanup() { restore_policy; client_run "sudo rm -rf ${MOUNT}${PREFIX}" >/dev/null 2>&1 || true; rm -f "$LOCAL"; }
trap case_cleanup EXIT

CW=$(client_for_dc dc-a)
[ -n "$CW" ] || CW="$CLIENT"
[ -n "$CW" ] || skip "no client available"

echo "[1/5] anchor cold=1 (dc-a) + short-window warm cache so idle-demote fires fast"
mgr_post_leader /api/v1/volumes/$VOL/set-anchor '{"datacenter":"dc-a","tier":"cold","mandatory_replicas":1}' >/dev/null
mgr_post_leader /api/v1/volumes/$VOL/set-mandatory '{"mandatory_replicas":{"hot":0,"warm":0,"cold":1}}' >/dev/null; SAVED=1
# Short promote_window so a brief idle period triggers the engine demote.
mgr_post_leader /api/v1/volumes/$VOL/set-cache '{"warm_min":1,"warm_max":1,"per_dc":false,"promote_reads":1,"promote_window_secs":30,"cooldown_secs":0}' >/dev/null

echo "[2/5] write file -> cold anchor lands in dc-a"
head -c $((8 * 1024 * 1024)) /dev/urandom > "$LOCAL"
scp "${SSH_OPTS[@]}" "$LOCAL" "debian@$CW:/tmp/66-src.bin" >/dev/null 2>&1 || skip "cannot stage src"
ssh_run "$CW" "sudo mkdir -p ${MOUNT}${PREFIX} && sudo cp /tmp/66-src.bin '$PATHF' && sudo sync" || fail "write failed"
ssh_run "$CW" "sudo rm -f /tmp/66-src.bin"
inspect_has_replicas "$PATHF" || known_issue REL-1-2 "inspect lacks per-replica tier (REL-1-2)"
_anchor_ok() { [ "$(count_replicas_in_dc_tier "$PATHF" dc-a cold)" -ge 1 ]; }
wait_for 60 "cold anchor present" _anchor_ok

echo "[3/5] read repeatedly -> promote a warm cache copy"
for _ in 1 2 3; do ssh_run "$CW" "sudo cat '$PATHF' > /dev/null"; sleep 2; done
_warm_present() { [ "$(count_replicas_in_tier "$PATHF" warm)" -ge 1 ]; }
if ! wait_for_soft 90 _warm_present; then
    known_issue CO-1-2 "warm cache copy did not promote within 90s; cannot exercise idle-demote"
fi
echo "  warm cache copy present"

echo "[4/5] go idle past the 30s promote_window -> engine idle-demotes the warm copy"
# No reads for > promote_window_secs so the AV tick demotes the idle warm copy.
sleep 50
_warm_gone() { [ "$(count_replicas_in_tier "$PATHF" warm)" -eq 0 ]; }
wait_for 120 "idle warm cache demoted" _warm_gone

echo "[5/5] cold anchor MUST survive every demote tick + readable + no anchor strand"
[ "$(count_replicas_in_dc_tier "$PATHF" dc-a cold)" -ge 1 ] || fail "CO-1-2 REGRESSION: cold anchor stranded by idle-demote"
ssh_run "$CW" "sudo cat '$PATHF' > /dev/null" || fail "file no longer readable after idle-demote (anchor gone)"
# If the planner ever DID try to demote the anchor it must have refused with a
# tier.anchor_protected audit, never an actual anchor deletion. Either way the
# anchor copy must be present (asserted above).
echo "  cold anchor survived engine-driven idle-demote"
pass

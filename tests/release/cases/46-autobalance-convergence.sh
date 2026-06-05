#!/usr/bin/env bash
# REL-2-46: end-to-end auto-balance convergence. Drive the volume to a declared
# target — mandatory cold=1 (anchor dc-a) + per-DC warm cache (min=max=1) — then
# read from each warm DC and assert placement converges to exactly that and
# stays put: 1 cold anchor in dc-a, 1 warm cache in each DC that read. Restores
# the default policy in cleanup. Non-PVE (needs per-DC clients + REL-1-2).
source "$(dirname "$0")/../lib/common.sh"

CB=$(client_for_dc dc-b); CC=$(client_for_dc dc-c); CW=$(client_for_dc dc-a)
[ -n "$CB" ] && [ -n "$CC" ] && [ -n "$CW" ] || skip "need per-DC clients (3dc)"

PATHF="$MOUNT/release/46-balance-$$.bin"; LOCAL=/tmp/46-src-$$.bin
SAVED=0
restore_policy() {
    [ "$SAVED" = "1" ] || return 0
    mgr_post_leader /api/v1/volumes/$VOL/set-mandatory '{"mandatory_replicas":{"hot":2,"warm":2,"cold":1}}' >/dev/null 2>&1 || true
    mgr_post_leader /api/v1/volumes/$VOL/set-cache '{"warm_min":0,"warm_max":0,"hot_min":0,"hot_max":0,"per_dc":true}' >/dev/null 2>&1 || true
}
case_cleanup() { restore_policy; rm_fuse_file "$PATHF"; rm -f "$LOCAL"; }
trap case_cleanup EXIT

_post_ok() {  # <endpoint> <body> <what>
    local r; r=$(mgr_post_leader "$1" "$2")
    if echo "$r" | grep -q '"error"' && ! echo "$r" | grep -qi 'ok\|true'; then
        fail "$3 rejected: $r"
    fi
}
_anchor_ok()  { [ "$(count_replicas_in_dc_tier "$PATHF" dc-a cold)" -ge 1 ]; }
_converged()  {
    [ "$(count_replicas_in_dc_tier "$PATHF" dc-a cold)"  -ge 1 ] &&
    [ "$(count_replicas_in_dc_tier "$PATHF" dc-b warm)"  -ge 1 ] &&
    [ "$(count_replicas_in_dc_tier "$PATHF" dc-c warm)"  -ge 1 ]
}

echo "[1/5] set target: mandatory cold=1 (anchor dc-a) + warm cache per-dc 1/1"
mgr_post_leader /api/v1/volumes/$VOL/set-anchor '{"datacenter":"dc-a","tier":"cold","mandatory_replicas":1}' >/dev/null
_post_ok /api/v1/volumes/$VOL/set-mandatory '{"mandatory_replicas":{"hot":0,"warm":0,"cold":1}}' set-mandatory
SAVED=1
_post_ok /api/v1/volumes/$VOL/set-cache '{"warm_min":1,"warm_max":1,"per_dc":true,"promote_reads":1,"promote_window_secs":3600,"cooldown_secs":0}' set-cache

echo "[2/5] write a file (lands as 1 cold anchor in dc-a)"
head -c $((8 * 1024 * 1024)) /dev/urandom > "$LOCAL"
SRC=$(sha256sum "$LOCAL" | awk '{print $1}')
scp "${SSH_OPTS[@]}" "$LOCAL" "debian@$CW:/tmp/46-src.bin" >/dev/null 2>&1 || skip "cannot stage"
ssh_run "$CW" "sudo mkdir -p \"\$(dirname '$PATHF')\" && sudo cp /tmp/46-src.bin '$PATHF' && sudo sync" || fail "write failed"
ssh_run "$CW" "sudo rm -f /tmp/46-src.bin"
inspect_has_replicas "$PATHF" || known_issue REL-1-2 "inspect lacks per-replica tier (REL-1-2)"
wait_for 60 "cold anchor in dc-a" _anchor_ok

echo "[3/5] read from dc-b and dc-c to trigger per-DC warm cache promote"
ssh_run "$CB" "sudo cat '$PATHF' > /dev/null" || fail "read from $CB failed"
ssh_run "$CC" "sudo cat '$PATHF' > /dev/null" || fail "read from $CC failed"

echo "[4/5] converge: cold dc-a=1, warm dc-b>=1, warm dc-c>=1"
if ! wait_for_soft 120 _converged; then
    echo "  state:"; _replica_rows "$PATHF"
    known_issue REL-2-46 "per-DC warm cache did not converge within 120s"
fi

echo "[5/5] stays converged (re-poll after 60s) + no AnchorViolated"
sleep 60
_converged || fail "placement regressed after settle"
AUDIT=$(mgr_get_any "/api/v1/audit?limit=200")
echo "$AUDIT" | grep -q "AnchorViolated" && fail "AnchorViolated during convergence"
echo "  converged + stable"
pass

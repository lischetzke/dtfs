#!/usr/bin/env bash
# REL-2-48: cross-DC read completes under realistic bandwidth shaping. Pin the
# only replica to dc-a (mandatory cold=1, cache off), apply per-DC tc shaping,
# then read from dc-b — forcing a fetch across the shaped 40 Mbps dc-a uplink —
# within a generous deadline, with Raft staying stable (ports exempt). Reverts
# shaping + policy in cleanup.
source "$(dirname "$0")/../lib/common.sh"
require_pve
command -v ansible-playbook >/dev/null 2>&1 || skip "ansible not on runner (needed for netshape)"

CB=$(client_for_dc dc-b); CW=$(client_for_dc dc-a)
[ -n "$CB" ] && [ -n "$CW" ] || skip "need per-DC clients"

PATHF="$MOUNT/release/48-shaped-$$.bin"; LOCAL=/tmp/48-src-$$.bin
SHAPED=0; SAVED=0
restore_policy() {
    [ "$SAVED" = "1" ] || return 0
    mgr_post_leader /api/v1/volumes/$VOL/set-mandatory '{"hot_replicas":2,"warm_replicas":2,"cold_replicas":1}' >/dev/null 2>&1 || true
}
case_cleanup() {
    [ "$SHAPED" = "1" ] && ansible-playbook -i "$INV3DC" "$ANSIBLE_DIR/playbooks/revert-shaping.yml" >/dev/null 2>&1 || true
    restore_policy
    rm_fuse_file "$PATHF"; rm -f "$LOCAL"
}
trap case_cleanup EXIT

echo "[1/4] pin the only replica to dc-a (cold=1, cache off) and write 16 MB"
mgr_post_leader /api/v1/volumes/$VOL/set-anchor '{"datacenter":"dc-a","tier":"cold","mandatory_replicas":1}' >/dev/null
mgr_post_leader /api/v1/volumes/$VOL/set-mandatory '{"hot_replicas":0,"warm_replicas":0,"cold_replicas":1}' >/dev/null; SAVED=1
mgr_post_leader /api/v1/volumes/$VOL/set-cache '{"warm_min":0,"warm_max":0,"hot_min":0,"hot_max":0,"per_dc":true}' >/dev/null
head -c $((16 * 1024 * 1024)) /dev/urandom > "$LOCAL"
SRC=$(sha256sum "$LOCAL" | awk '{print $1}')
scp "${SSH_OPTS[@]}" "$LOCAL" "debian@$CW:/tmp/48-src.bin" >/dev/null 2>&1 || skip "cannot stage"
ssh_run "$CW" "sudo mkdir -p \"\$(dirname '$PATHF')\" && sudo cp /tmp/48-src.bin '$PATHF' && sudo sync" || fail "write failed"
ssh_run "$CW" "sudo rm -f /tmp/48-src.bin"
sleep 5

echo "[2/4] apply per-DC bandwidth shaping"
ansible-playbook -i "$INV3DC" "$ANSIBLE_DIR/playbooks/apply-shaping.yml" >/dev/null 2>&1 && SHAPED=1 || skip "shaping apply failed"

echo "[3/4] cross-DC read from $CB completes within deadline + sha matches"
ssh_run "$CB" "sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'" >/dev/null 2>&1 || true
START=$(date +%s)
GOT=$(timeout 120 ssh "${SSH_OPTS[@]}" "debian@$CB" "sudo sha256sum '$PATHF' | awk '{print \$1}'") || fail "shaped cross-DC read timed out (>120s)"
ELAPSED=$(( $(date +%s) - START ))
assert_eq "$SRC" "$GOT" "shaped cross-DC read"
echo "  read completed in ${ELAPSED}s"

echo "[4/4] Raft stayed stable under shaping (ports exempt)"
quorum_ok || fail "lost quorum under shaping (Raft/API ports not exempt?)"
pass

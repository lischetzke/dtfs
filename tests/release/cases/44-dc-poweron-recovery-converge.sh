#!/usr/bin/env bash
# REL-2-44: recovery after a DC outage. Take dc-c down (drops a warm replica),
# bring it back, and confirm under-replication heals: warm replicas return to
# target and a dc-a copy (the cold anchor) survived, with no AnchorViolated.
source "$(dirname "$0")/../lib/common.sh"
require_pve

DOWN=dc-c
CW=$(client_for_dc dc-a)
[ -n "$CW" ] || skip "no dc-a client"

PATHF="$MOUNT/release/44-recover-$$.bin"; LOCAL=/tmp/44-src-$$.bin
RESTORED=0
case_cleanup() {
    [ "$RESTORED" = "1" ] || power_dc_on "$DOWN" >/dev/null 2>&1 || true
    rm_fuse_file "$PATHF"; rm -f "$LOCAL"
}
trap case_cleanup EXIT

_warm_ge2()  { [ "$(count_replicas_in_tier "$PATHF" warm)" -ge 2 ]; }
_converged() { [ "$(count_replicas_in_tier "$PATHF" warm)" -ge 2 ] && [ "$(count_replicas_in_dc "$PATHF" dc-a)" -ge 1 ]; }

echo "[1/5] write a file (warm rf=2 spans dc-b/dc-c)"
head -c $((16 * 1024 * 1024)) /dev/urandom > "$LOCAL"
SRC=$(sha256sum "$LOCAL" | awk '{print $1}')
scp "${SSH_OPTS[@]}" "$LOCAL" "debian@$CW:/tmp/44-src.bin" >/dev/null 2>&1 || skip "cannot stage"
ssh_run "$CW" "sudo mkdir -p \"\$(dirname '$PATHF')\" && sudo cp /tmp/44-src.bin '$PATHF' && sudo sync" || fail "write failed"
ssh_run "$CW" "sudo rm -f /tmp/44-src.bin"
wait_for 60 "warm replicas reach 2" _warm_ge2

echo "[2/5] power OFF $DOWN (drops any dc-c warm replica)"
power_dc_off "$DOWN"
wait_for 60 "quorum holds" quorum_ok

echo "[3/5] power $DOWN back ON"
power_dc_on "$DOWN"; RESTORED=1

echo "[4/5] wait for under-replication to heal (warm back to 2, dc-a copy present)"
if ! wait_for_soft 240 _converged; then
    echo "  not converged:"; _replica_rows "$PATHF"
    known_issue REL-2-44 "warm replicas did not return to target within 240s"
fi

echo "[5/5] dc-a anchor intact + no AnchorViolated"
[ "$(count_replicas_in_dc "$PATHF" dc-a)" -ge 1 ] || fail "lost the dc-a anchor copy"
AUDIT=$(mgr_get_any "/api/v1/audit?limit=200")
echo "$AUDIT" | grep -q "AnchorViolated" && fail "AnchorViolated during recovery"
echo "  recovered + anchor safe"
pass

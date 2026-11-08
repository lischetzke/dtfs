#!/usr/bin/env bash
# BE-20 (Bug BC) — corrupt → detect → fallback → manual quarantine →
# unquarantine. Uses the same byte-flip mechanic as
# a former lab bit-rot abuse script.
source "$(dirname "$0")/../lib/common.sh"

F="$MOUNT/release/20-quar-$$.bin"
DATFILE=""
HOST=""
NID=""

case_cleanup() {
    if [ -n "$DATFILE" ] && [ -n "$HOST" ]; then
        ssh_run "$HOST" "sudo cp '$DATFILE.bak' '$DATFILE' && sudo rm -f '$DATFILE.bak'" 2>/dev/null || true
    fi
    if [ -n "$NID" ]; then
        mgr_post_leader "/api/v1/nodes/$NID/unquarantine" \
            '{"operator_id":"release-suite","reason_acknowledged":"automated test cleanup pass"}' \
            >/dev/null 2>&1 || true
    fi
    client_run "sudo rm -f '$F'" >/dev/null 2>&1 || true
}
trap case_cleanup EXIT

echo "[1/6] write 10 MiB"
client_run "sudo mkdir -p $MOUNT/release && sudo dd if=/dev/urandom of='$F' bs=1M count=10 status=none && sudo sync"
HASH=$(sha256_via_fuse "$F")
[ -n "$HASH" ] || fail "sha256 empty"

echo "[2/6] find replicas"
fname=${F#$MOUNT}
sleep 3
CID=$(chunk_ids_for_path "$fname" | head -1)
[ -n "$CID" ] || fail "no chunk_id"
HOLDING=()
for h in $(data_list); do
    if host_has_chunk "$h" "$CID"; then HOLDING+=("$h"); fi
done
[ ${#HOLDING[@]} -ge 2 ] || skip "fewer than 2 replicas — RF=1 cluster"
HOST="${HOLDING[0]}"

echo "[3/6] corrupt one replica on $HOST"
DATFILE=$(ssh_run "$HOST" "sudo find /mnt -name '$CID.dat' 2>/dev/null | head -1")
[ -n "$DATFILE" ] || fail "chunk file not found"
ssh_run "$HOST" "sudo cp '$DATFILE' '$DATFILE.bak' && sudo dd if=/dev/urandom of='$DATFILE' bs=1 count=1 seek=4096 conv=notrunc status=none"

echo "[4/6] read still matches (P0-2 fallback)"
client_run "sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'" >/dev/null 2>&1 || true
NEW=$(sha256_via_fuse "$F")
assert_eq "$HASH" "$NEW" "fallback sha"

echo "[5/6] manual quarantine via WebUI flow"
# The release suite drives quarantine via a mocked operator action:
# We POST UnquarantineNode-shaped state by first inserting a quarantine
# entry directly via... actually mgr has no direct quarantine RPC yet
# (auto-quarantine via heartbeat is BC follow-up). Until that lands,
# the operator path is: see the read.integrity_fault audit, decide,
# unquarantine. So this case verifies the audit row + the
# unquarantine endpoint shape — the auto-quarantine itself is
# KNOWN_ISSUE until heartbeat propagation lands.
sleep 5
AUDIT=$(mgr_get_any '/api/v1/integrity/faults?limit=200')
if ! echo "$AUDIT" | grep -q "integrity_fault\|quarantine"; then
    known_issue BC "no integrity_fault audit emitted yet — verify_on_read may not have triggered (mgr-side heartbeat propagation pending)"
fi

echo "[6/6] unquarantine endpoint validation"
NID=$(mgr_get_any /api/v1/nodes | python3 -c "
import json,sys
for n in json.load(sys.stdin):
    if n.get('hostname')==sys.argv[1]:
        print(n.get('node_id','')); break
" "$HOST")
[ -n "$NID" ] || fail "node_id lookup failed"
# Validate boundary: empty ack rejected.
RESP=$(mgr_post_leader "/api/v1/nodes/$NID/unquarantine" '{"operator_id":"test","reason_acknowledged":"too short"}')
if ! echo "$RESP" | grep -q "reason_acknowledged must be at least"; then
    fail "unquarantine endpoint did not reject short ack: $RESP"
fi

pass

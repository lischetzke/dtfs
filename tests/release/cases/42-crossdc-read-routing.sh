#!/usr/bin/env bash
# REL-2-42: cross-DC nearest-replica read routing. A first-time read from a
# client in dc-b must be served by the dc-b replica, not a remote DC. Proven
# via per-node total_bytes_read (REL-1-1): the dc-b data nodes' served-bytes
# jump by ~filesize while dc-a/dc-c barely move.
source "$(dirname "$0")/../lib/common.sh"

DC=dc-b
CB=$(client_for_dc "$DC"); CW=$(client_for_dc dc-a)
[ -n "$CB" ] && [ -n "$CW" ] || skip "no per-DC clients in inventory (need 3dc)"
nodes_expose_io || known_issue REL-1-1 "/api/v1/nodes lacks total_bytes_read"

PATHF="$MOUNT/release/42-crossdc-$$.bin"
LOCAL=/tmp/42-src-$$.bin
case_cleanup() { rm_fuse_file "$PATHF"; rm -f "$LOCAL"; }
trap case_cleanup EXIT

snap_dc() {  # <dc> -> summed total_bytes_read over its data hosts
    local dc="$1" tot=0 h v
    for h in $(data_hosts_in_dc "$dc"); do
        v=$(served_bytes_for_node "$h")
        [ "$v" -ge 0 ] 2>/dev/null || v=0
        tot=$((tot + v))
    done
    echo "$tot"
}
_path_has_dc_replica() { [ "$(count_replicas_in_dc "$PATHF" "$DC")" -ge 1 ]; }

SIZE=$((8 * 1024 * 1024))
echo "[1/5] write ${SIZE}B from $CW (dc-a)"
head -c "$SIZE" /dev/urandom > "$LOCAL"
SRC=$(sha256sum "$LOCAL" | awk '{print $1}')
scp "${SSH_OPTS[@]}" "$LOCAL" "debian@$CW:/tmp/42-src.bin" >/dev/null 2>&1 || skip "cannot stage to $CW"
ssh_run "$CW" "sudo mkdir -p \"\$(dirname '$PATHF')\" && sudo cp /tmp/42-src.bin '$PATHF' && sudo sync" || fail "write failed"
ssh_run "$CW" "sudo rm -f /tmp/42-src.bin"

echo "[2/5] wait for a replica to land in $DC"
wait_for_soft 60 _path_has_dc_replica || skip "no $DC replica placed (cannot test local routing)"

echo "[3/5] snapshot served-bytes per DC"
B0=$(snap_dc dc-b); A0=$(snap_dc dc-a); C0=$(snap_dc dc-c)

echo "[4/5] first-time read from $CB (dc-b) — should hit the $DC replica"
ssh_run "$CB" "sudo cat '$PATHF' > /dev/null" || fail "read from $CB failed"
sleep 10   # let the served-bytes counter propagate via heartbeat

echo "[5/5] assert $DC served the read"
B1=$(snap_dc dc-b); A1=$(snap_dc dc-a); C1=$(snap_dc dc-c)
DB=$((B1 - B0)); DA=$((A1 - A0)); DCC=$((C1 - C0))
echo "  served delta: dc-a=$DA dc-b=$DB dc-c=$DCC (read ${SIZE}B)"
assert_ge "$DB" "$((SIZE * 7 / 10))" "dc-b served-bytes delta"
[ "$DB" -gt "$DA" ]  || fail "dc-b ($DB) did not out-serve dc-a ($DA)"
[ "$DB" -gt "$DCC" ] || fail "dc-b ($DB) did not out-serve dc-c ($DCC)"
pass

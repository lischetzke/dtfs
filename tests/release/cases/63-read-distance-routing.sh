#!/usr/bin/env bash
# REL-2-63 (CO-3-10): the FUSE client's known_locations map is populated from
# GetFileLocations (the new positional ChunkLocationInfo.node_locations field),
# so read replica selection actually ranks by physical distance. With NO
# read_location_priority configured, a client reads its OWN-DC (nearer) replica
# rather than a remote one, regardless of which replica the manager lists first.
#
# Before CO-3-10 known_locations was never .insert()ed, so every replica looked
# CrossRegion and the client served from whichever replica came first in the
# list (arbitrary). This case proves distance routing is live: the client's own
# DC out-serves the remote DC by ~filesize.
#
# Proven via per-node total_bytes_read (REL-1-1). No config rewrite (unlike
# case 59, which tests the priority OVERRIDE) — this exercises the default
# distance path the CO-3-10 wiring enables.
source "$(dirname "$0")/../lib/common.sh"

# Need >=2 DCs each with data nodes, and a client in one of them, so "prefer my
# own DC" is a meaningful, observable distance decision.
NEAR_DC=dc-b              # the client's own DC (the distance-winner)
FAR_DC=dc-a              # the remote DC holding the other replica
CLN=$(client_for_dc "$NEAR_DC")
[ -n "$CLN" ] || skip "no client in $NEAR_DC (need the 3dc topology)"
[ -n "$(data_hosts_in_dc "$FAR_DC")" ] || skip "no data nodes in $FAR_DC"
[ -n "$(data_hosts_in_dc "$NEAR_DC")" ] || skip "no data nodes in $NEAR_DC"
nodes_expose_io || known_issue REL-1-1 "/api/v1/nodes lacks total_bytes_read"

PATHF="$MOUNT/release/63-distance-$$.bin"
APIF="${PATHF#$MOUNT}"  # mount-relative path for manager API calls
LOCAL=/tmp/63-src-$$.bin

case_cleanup() {
    rm_fuse_file "$PATHF"
    rm -f "$LOCAL"
}
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
_path_has_far_replica()  { [ "$(count_replicas_in_dc "$APIF" "$FAR_DC")" -ge 1 ]; }
_path_has_near_replica() { [ "$(count_replicas_in_dc "$APIF" "$NEAR_DC")" -ge 1 ]; }

SIZE=$((8 * 1024 * 1024))
echo "[1/5] write ${SIZE}B from $CLN ($NEAR_DC)"
head -c "$SIZE" /dev/urandom > "$LOCAL"
scp "${SSH_OPTS[@]}" "$LOCAL" "debian@$CLN:/tmp/63-src.bin" >/dev/null 2>&1 || skip "cannot stage to $CLN"
ssh_run "$CLN" "sudo mkdir -p \"\$(dirname '$PATHF')\" && sudo cp /tmp/63-src.bin '$PATHF' && sudo sync" || fail "write failed"
ssh_run "$CLN" "sudo rm -f /tmp/63-src.bin"

echo "[2/5] wait for replicas in BOTH $NEAR_DC and $FAR_DC"
wait_for_soft 90 _path_has_far_replica  || skip "no $FAR_DC replica placed (no remote alternative)"
wait_for_soft 30 _path_has_near_replica || skip "no $NEAR_DC replica placed (no nearer replica to prefer)"

# Make sure no stale priority config is steering the read; this case relies on
# pure distance. We do NOT modify the config (default = no priority).
echo "[3/5] snapshot served-bytes per DC"
F0=$(snap_dc "$FAR_DC"); N0=$(snap_dc "$NEAR_DC")

echo "[4/5] read from $CLN — distance routing must serve from local $NEAR_DC"
ssh_run "$CLN" "sudo cat '$PATHF' > /dev/null" || fail "read from $CLN failed"
sleep 10   # let the served-bytes counter propagate via heartbeat

echo "[5/5] assert local $NEAR_DC out-served remote $FAR_DC"
F1=$(snap_dc "$FAR_DC"); N1=$(snap_dc "$NEAR_DC")
DF=$((F1 - F0)); DN=$((N1 - N0))
echo "  served delta: $NEAR_DC(local)=$DN $FAR_DC(remote)=$DF (read ${SIZE}B)"
assert_ge "$DN" "$((SIZE * 7 / 10))" "$NEAR_DC served-bytes delta"
[ "$DN" -gt "$DF" ] || fail "local $NEAR_DC ($DN) did not out-serve remote $FAR_DC ($DF) — known_locations may be empty (CO-3-10)"
pass

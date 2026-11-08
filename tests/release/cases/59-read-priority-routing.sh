#!/usr/bin/env bash
# REL-2-59 (U4): configurable read-location priority overrides distance. We
# point a client's read_location_priority at a REMOTE DC's replica and prove
# reads go there instead of the nearer/local one. Proven via per-node
# total_bytes_read (REL-1-1): the prioritised DC's data hosts' served-bytes
# jump by ~filesize while the client's own DC barely moves.
#
# Not in QUICK_CASES: it rewrites a client's fuse.toml and restarts the FUSE
# service, then restores the original config.
source "$(dirname "$0")/../lib/common.sh"

# Need >=2 DCs with a client and data nodes so "prefer the remote DC" is a
# meaningful, observable override of distance.
NEAR_DC=dc-b              # the client's own DC (the distance-winner)
FAR_DC=dc-a               # the DC we force reads to via read_location_priority
CLN=$(client_for_dc "$NEAR_DC")
[ -n "$CLN" ] || skip "no client in $NEAR_DC (need the 3dc topology)"
[ -n "$(data_hosts_in_dc "$FAR_DC")" ] || skip "no data nodes in $FAR_DC"
[ -n "$(data_hosts_in_dc "$NEAR_DC")" ] || skip "no data nodes in $NEAR_DC"
nodes_expose_io || known_issue REL-1-1 "/api/v1/nodes lacks total_bytes_read"

FUSE_TOML=${FUSE_TOML:-/etc/dtfs/fuse.toml}
FUSE_UNIT=${FUSE_UNIT:-dtfs-fuse}
PATHF="$MOUNT/release/59-priority-$$.bin"
LOCAL=/tmp/59-src-$$.bin
BACKUP="/tmp/59-fuse-toml-bak-$$"

restore_fuse_cfg() {
    # Roll back the config we injected and restart so the suite leaves the
    # client exactly as it found it.
    ssh_run "$CLN" "sudo test -f '$BACKUP' && sudo mv '$BACKUP' '$FUSE_TOML' && sudo systemctl restart '$FUSE_UNIT'" >/dev/null 2>&1 || true
}
case_cleanup() {
    rm_fuse_file "$PATHF"
    rm -f "$LOCAL"
    restore_fuse_cfg
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
_path_has_far_replica() { [ "$(count_replicas_in_dc "$PATHF" "$FAR_DC")" -ge 1 ]; }
_path_has_near_replica() { [ "$(count_replicas_in_dc "$PATHF" "$NEAR_DC")" -ge 1 ]; }

SIZE=$((8 * 1024 * 1024))
echo "[1/7] write ${SIZE}B from $CLN ($NEAR_DC)"
head -c "$SIZE" /dev/urandom > "$LOCAL"
scp "${SSH_OPTS[@]}" "$LOCAL" "debian@$CLN:/tmp/59-src.bin" >/dev/null 2>&1 || skip "cannot stage to $CLN"
ssh_run "$CLN" "sudo mkdir -p \"\$(dirname '$PATHF')\" && sudo cp /tmp/59-src.bin '$PATHF' && sudo sync" || fail "write failed"
ssh_run "$CLN" "sudo rm -f /tmp/59-src.bin"

echo "[2/7] wait for replicas in BOTH $NEAR_DC and $FAR_DC"
wait_for_soft 90 _path_has_far_replica  || skip "no $FAR_DC replica placed (cannot test remote priority)"
wait_for_soft 30 _path_has_near_replica || skip "no $NEAR_DC replica placed (no nearer alternative to override)"

# Determine the FAR DC's datacenter string as the client's [location] sees it.
# inventory `dc` is the routing label; the manager/topology datacenter string
# matches it in the 3dc fixture, so we key the priority list on it directly.
echo "[3/7] inject read_location_priority = [\"$FAR_DC\"] into $FUSE_TOML on $CLN"
ssh_run "$CLN" "sudo cp '$FUSE_TOML' '$BACKUP'" || skip "cannot back up $FUSE_TOML on $CLN"
# Strip any prior top-level read_location_priority line, then prepend the new
# one (it must precede the first [table] header to stay top-level).
ssh_run "$CLN" "sudo sed -i '/^[[:space:]]*read_location_priority[[:space:]]*=/d' '$FUSE_TOML' && \
                printf 'read_location_priority = [\"%s\"]\n' '$FAR_DC' | sudo tee /tmp/59-prio.line >/dev/null && \
                sudo sh -c 'cat /tmp/59-prio.line '\"'$FUSE_TOML'\"' > /tmp/59-fuse.new && mv /tmp/59-fuse.new '\"'$FUSE_TOML'\"'' && \
                sudo rm -f /tmp/59-prio.line" || fail "could not rewrite $FUSE_TOML"

echo "[4/7] restart $FUSE_UNIT on $CLN and wait for the mount"
ssh_run "$CLN" "sudo systemctl restart '$FUSE_UNIT'" || fail "fuse restart failed"
wait_for 60 "mount back after restart" ssh_run "$CLN" "test -d '$MOUNT' && sudo ls '$MOUNT' >/dev/null 2>&1"

echo "[5/7] snapshot served-bytes per DC"
F0=$(snap_dc "$FAR_DC"); N0=$(snap_dc "$NEAR_DC")

echo "[6/7] read from $CLN — priority must steer it to $FAR_DC, not local $NEAR_DC"
ssh_run "$CLN" "sudo cat '$PATHF' > /dev/null" || fail "read from $CLN failed"
sleep 10   # let the served-bytes counter propagate via heartbeat

echo "[7/7] assert $FAR_DC (prioritised) out-served $NEAR_DC (local)"
F1=$(snap_dc "$FAR_DC"); N1=$(snap_dc "$NEAR_DC")
DF=$((F1 - F0)); DN=$((N1 - N0))
echo "  served delta: $FAR_DC(prio)=$DF $NEAR_DC(local)=$DN (read ${SIZE}B)"
assert_ge "$DF" "$((SIZE * 7 / 10))" "$FAR_DC served-bytes delta"
[ "$DF" -gt "$DN" ] || fail "prioritised $FAR_DC ($DF) did not out-serve local $NEAR_DC ($DN)"
pass

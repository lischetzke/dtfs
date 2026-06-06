#!/usr/bin/env bash
# CF-3b — write-safety ack floor: the manager rejects a commit_chunk_locations
# whose chunks landed fewer distinct (node_id, disk_id) replicas than the
# volume's write_safety.min_replicas_before_ack, instead of acking a write it
# refuses to record.
#
# Two legs, both on the `default` volume (the FUSE mount always writes there):
#   A. ack floor = 2 on a placement that lands a SINGLE replica  -> write FAILS
#   B. ack floor = 1 on the same placement                       -> write SUCCEEDS
#
# Forcing a single-replica placement: pin the default volume's mandatory
# replication to one tier at rf=1 (saved + restored in cleanup) so allocate
# hands the FUSE writer exactly one target and the commit carries one replica.
#
# DEPENDENCY: toggling the floor needs the set-write-safety endpoint (CF-3a,
# a sibling worker's change). This case does NOT hard-depend on it: when the
# endpoint is absent it reports KNOWN_ISSUE [CF-3a], which auto-promotes once
# CF-3a merges. The gate's correctness is unit-tested in dtfs-manager
# (metadata::ack_floor_satisfied) regardless.
source "$(dirname "$0")/../lib/common.sh"

TESTFILE="$MOUNT/cf3b-ack-floor.bin"
LOCAL_SRC="/tmp/cf3b-src-$$.bin"
SAVED_FLOOR=""
SAVED_MAND_WARM=""
SAVED_MAND_COLD=""

# Read a write_safety.min_replicas_before_ack for $VOL.
vol_floor() {
    mgr_get_any "/api/v1/volumes/$VOL" | python3 -c "
import json,sys
try:
    d=json.load(sys.stdin)
except Exception:
    sys.exit(0)
print(d.get('write_safety',{}).get('min_replicas_before_ack',''))
"
}

# Read effective mandatory_replicas.{warm,cold} for $VOL.
vol_mandatory() {
    mgr_get_any "/api/v1/volumes/$VOL" | python3 -c "
import json,sys
try:
    d=json.load(sys.stdin)
except Exception:
    sys.exit(0)
m=d.get('mandatory_replicas',{})
print(int(m.get('warm',0)), int(m.get('cold',0)))
"
}

# Set the ack floor via the CF-3a endpoint. Returns 0 only if the endpoint
# exists AND the volume reflects the new floor afterwards.
set_floor() {
    local want="$1"
    local resp
    resp=$(mgr_post_leader "/api/v1/volumes/$VOL/set-write-safety" \
        "{\"min_replicas_before_ack\":$want}")
    # Endpoint missing -> 404 body or empty; treat as not-available.
    echo "$resp" | grep -qiE '404|not found|no route|unknown' && return 1
    wait_for_soft 10 _floor_is "$want"
}
_floor_is() { [ "$(vol_floor)" = "$1" ]; }

write_via_fuse() {
    # Returns 0 if the FUSE write succeeds, non-zero on EIO/commit reject.
    client_run "sudo cp /tmp/cf3b-src.bin '$TESTFILE' && sync"
}

case_cleanup() {
    client_run "sudo rm -f '$TESTFILE' /tmp/cf3b-src.bin" >/dev/null 2>&1 || true
    rm -f "$LOCAL_SRC" 2>/dev/null || true
    # Restore the ack floor (best-effort; only if we changed it).
    if [ -n "$SAVED_FLOOR" ]; then
        mgr_post_leader "/api/v1/volumes/$VOL/set-write-safety" \
            "{\"min_replicas_before_ack\":$SAVED_FLOOR}" >/dev/null 2>&1 || true
    fi
    # Restore mandatory replication.
    if [ -n "$SAVED_MAND_WARM" ]; then
        mgr_post_leader "/api/v1/volumes/$VOL/set-mandatory" \
            "{\"mandatory_replicas\":{\"warm\":$SAVED_MAND_WARM,\"cold\":$SAVED_MAND_COLD}}" \
            >/dev/null 2>&1 || true
    fi
}
trap case_cleanup EXIT

echo "[1/7] snapshot default-volume policy"
SAVED_FLOOR=$(vol_floor)
read -r SAVED_MAND_WARM SAVED_MAND_COLD < <(vol_mandatory)
[ -n "$SAVED_FLOOR" ] || known_issue CF-3b "could not read $VOL write_safety (manager down?)"
echo "  saved: floor=$SAVED_FLOOR mandatory(warm,cold)=($SAVED_MAND_WARM,$SAVED_MAND_COLD)"

echo "[2/7] probe for the set-write-safety endpoint (CF-3a)"
if ! set_floor 2; then
    # Restore the floor we may have nudged, then defer to CF-3a.
    SAVED_MAND_WARM=""   # we never touched mandatory yet
    known_issue CF-3a "set-write-safety endpoint not present; CF-3b live verification needs CF-3a's setter (gate unit-tested in dtfs-manager::ack_floor_satisfied)"
fi

echo "[3/7] pin placement to a SINGLE replica (mandatory cold=1, warm=0)"
RESP=$(mgr_post_leader "/api/v1/volumes/$VOL/set-mandatory" \
    '{"mandatory_replicas":{"warm":0,"cold":1}}')
echo "$RESP" | grep -q '"error"' && ! echo "$RESP" | grep -q "ok\|true" \
    && fail "set-mandatory rejected: $RESP"
sleep 5   # let the UpsertVolume apply + placement ring rebuild

echo "[4/7] prepare a 1 MB (single-chunk) source"
head -c 1048576 /dev/urandom > "$LOCAL_SRC"
SRC_SHA=$(sha256sum "$LOCAL_SRC" | awk '{print $1}')
scp "${SSH_OPTS[@]}" "$LOCAL_SRC" "debian@$CLIENT:/tmp/cf3b-src.bin" >/dev/null 2>&1 \
    || known_issue CF-3b "could not stage source to client"

echo "[5/7] leg A: floor=2 on a single-replica placement -> write MUST fail"
# floor already set to 2 in step [2/7].
if write_via_fuse; then
    fail "write SUCCEEDED with ack floor=2 but only 1 replica placeable (gate did not reject)"
fi
echo "  write correctly rejected (EIO) under floor=2"
# The rejected commit may leave an orphan chunk on the data node; the orphan
# reaper (Bug CB, mtime grace) reclaims it. Not asserted here.

echo "[6/7] leg B: drop floor to 1 -> the same write MUST succeed"
client_run "sudo rm -f '$TESTFILE'" >/dev/null 2>&1 || true
set_floor 1 || fail "could not lower ack floor to 1"
if ! write_via_fuse; then
    fail "write FAILED with ack floor=1 and 1 replica placeable (gate over-rejected)"
fi

echo "[7/7] read back + verify integrity"
GOT_SHA=$(sha256_via_fuse "$TESTFILE")
assert_eq "$SRC_SHA" "$GOT_SHA" "round-trip sha256 of the file written under floor=1"

pass

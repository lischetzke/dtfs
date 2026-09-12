#!/usr/bin/env bash
# CR-6-4 (FUSE-6): a file created in a NON-default volume must keep that
# volume's own RF (and anchor, when set) for every chunk committed after
# the first open -- including chunks written after a close+reopen, which
# is exactly the path that regressed pre-fix. `fetch_file_locations`
# hardcoded `volume_id: "default"` on every refresh (the manager's
# GetFileLocationsResponse carried no volume_id field at all), so the
# SECOND open of an existing non-default-volume file silently placed
# every subsequent write with the DEFAULT volume's RF/tier/anchor
# instead of the volume it was actually created under.
#
# FUSE has no client-side way to pick a volume at create() time (always
# CreateFile(path, None)), so this case creates the file directly via
# the manager's CreateFile gRPC (grpcurl, LIS-56-stamped) with an
# explicit volume_id, then drives every subsequent write through the
# real FUSE mount (open, write, flush, close, reopen, append) and
# inspects placement via /api/v1/files/inspect.
#
# Forging CreateFile needs grpcurl + proto/manager.proto; when
# unavailable we KNOWN_ISSUE (the fix is unit-tested directly:
# cr_6_4_resolve_placement_volume_id_prefers_existing_file in
# dtfs-manager, cr_6_4_resolve_refreshed_volume_id_tests in dtfs-fuse).
source "$(dirname "$0")/../lib/common.sh"

PROTO_DIR="$(cd "$(dirname "$0")/../../../proto" 2>/dev/null && pwd)"
MGR_GRPC_PORT=${MGR_GRPC_PORT:-9401}
VOL_NAME="release109strict"
PREFIX="/release/109-vol-$$"
PATHF="${MOUNT}${PREFIX}.bin"
APIF="${PATHF#$MOUNT}"
EXPECT_RF=3

case_cleanup() { client_run "sudo rm -f '$PATHF'" >/dev/null 2>&1 || true; }
trap case_cleanup EXIT

replica_counts_ok() {
    mgr_get_any "/api/v1/files/inspect?path=$APIF" | python3 -c "
import json, sys
d = json.load(sys.stdin)
chunks = d.get('chunks', [])
if not chunks:
    sys.exit(1)
for c in chunks:
    if len(set(c.get('node_ids', []))) < $EXPECT_RF:
        sys.exit(1)
print(len(chunks))
"
}

echo "[1/6] preconditions: grpcurl + proto/manager.proto"
command -v grpcurl >/dev/null 2>&1 \
    || known_issue FUSE-6 "grpcurl unavailable: CreateFile with an explicit volume_id not live-wirable; fix is unit-tested (cr_6_4_*)"
[ -n "$PROTO_DIR" ] && [ -f "$PROTO_DIR/manager.proto" ] \
    || known_issue FUSE-6 "manager.proto not found at $PROTO_DIR; cannot resolve the RPC without server reflection"

echo "[2/6] create a volume whose RF ($EXPECT_RF) differs from \$VOL's default"
RESP=$(mgr_post_leader /api/v1/volumes \
    "{\"name\":\"$VOL_NAME\",\"replication_hot\":0,\"replication_warm\":$EXPECT_RF,\"replication_cold\":0}")
if echo "$RESP" | grep -qi '"error"'; then
    # CO-4-14: re-running this case against a cluster that already has the
    # volume from a prior (failed) run gets a 409/error here -- that's fine,
    # the volume is still usable.
    echo "  create-volume: $RESP (treating as already-exists)"
fi

echo "[3/6] resolve a manager host + build the LIS-56 CreateFile stamp"
HOST=""
for h in $(mgrs_list); do HOST="$h"; break; done
[ -n "$HOST" ] || fail "no manager host configured"
mgr_rpc_stamp CreateFile "$APIF" || fail "could not build the CreateFile auth stamp"

echo "[4/6] CreateFile($APIF, volume_id=$VOL_NAME) via grpcurl"
OUT=$(grpcurl -plaintext -max-time 8 "${MGR_RPC_HDRS[@]}" -import-path "$PROTO_DIR" -proto manager.proto \
      -d "{\"path\":\"$APIF\",\"volume_id\":\"$VOL_NAME\"}" \
      "$HOST:$MGR_GRPC_PORT" dtfs.manager.ManagerService/CreateFile 2>&1) \
    || fail "CreateFile RPC failed: $OUT"
echo "$OUT" | grep -qi '"success": *true' || fail "CreateFile did not report success: $OUT"

echo "[5/6] write, close, reopen, append via the real FUSE mount"
client_run "sudo mkdir -p '$(dirname "$PATHF")'" || fail "mkdir failed"
client_run "head -c \$((2*1024*1024)) /dev/urandom | sudo tee '$PATHF' >/dev/null && sudo sync" \
    || fail "initial write failed"
client_run "head -c \$((2*1024*1024)) /dev/urandom | sudo tee -a '$PATHF' >/dev/null && sudo sync" \
    || fail "reopen+append failed"

echo "[6/6] every chunk (first write AND the post-reopen append) carries \$VOL_NAME's RF ($EXPECT_RF)"
wait_for 30 "chunk locations converge to $EXPECT_RF replicas per chunk" replica_counts_ok
N=$(replica_counts_ok) || fail "replica_counts_ok failed on final check"
[ "$N" -ge 1 ] || fail "expected at least one chunk, got $N"

echo "  $N chunk(s), each with >= $EXPECT_RF replicas from volume '$VOL_NAME'"
pass

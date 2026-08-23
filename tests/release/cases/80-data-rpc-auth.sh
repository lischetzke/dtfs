#!/usr/bin/env bash
# 80-data-rpc-auth.sh — LIS-19 / Sec R2: unauthenticated PutChunk and
# DeleteChunk on a data node MUST be rejected with PERMISSION_DENIED when
# cluster.shared_secret is set. Pre-fix any peer that could reach :9500
# could write or unlink chunk bytes while file metadata still referenced
# the id (silent data-loss class).
#
# Forging the RPCs needs grpcurl + proto/data.proto. When grpcurl is
# unavailable we SKIP (unit tests put_chunk_rejects_missing_auth +
# delete_chunk_rejects_missing_auth cover the gate; live forge is
# belt-and-suspenders).
source "$(dirname "$0")/../lib/common.sh"

PROTO_DIR="$(cd "$(dirname "$0")/../../../proto" 2>/dev/null && pwd)"
PREFIX=/release/80-data-rpc-auth-$$/
PATHF="${MOUNT}${PREFIX}f.bin"
APIF="${PATHF#$MOUNT}"

case_cleanup() { client_run "sudo rm -rf ${MOUNT}${PREFIX}" >/dev/null 2>&1 || true; }
trap case_cleanup EXIT

file_exists() {
    mgr_get_any "/api/v1/files/inspect?path=$APIF" \
        | python3 -c "import json,sys;d=json.load(sys.stdin);sys.exit(0 if d.get('chunks') else 1)" 2>/dev/null
}

echo "[1/5] preconditions: shared secret + grpcurl + proto"
SECRET="${SHARED_SECRET:-}"
[ -n "$SECRET" ] || SECRET="$(mgr_shared_secret 2>/dev/null || true)"
[ -n "$SECRET" ] || skip "shared_secret empty; auth check is back-compat-skipped on a no-secret cluster"
command -v grpcurl >/dev/null 2>&1 \
    || skip "grpcurl unavailable: live forge skipped; Put/DeleteChunk auth unit-tested in dtfs-data"
[ -n "$PROTO_DIR" ] && [ -f "$PROTO_DIR/data.proto" ] \
    || skip "data.proto not found at $PROTO_DIR; cannot resolve the RPC without server reflection"

echo "[2/5] write a file so a real chunk id exists on a data node"
client_run "sudo mkdir -p ${MOUNT}${PREFIX} && head -c 4096 /dev/urandom | sudo tee '$PATHF' >/dev/null && sudo sync" \
    || fail "could not stage target file"
wait_for 30 "target file registered" file_exists

CID=$(chunk_ids_for_path "$APIF" | head -1)
[ -n "$CID" ] || fail "no chunk_id for $APIF"
HOST=""
for h in $(data_list); do
    if host_has_chunk "$h" "$CID"; then HOST="$h"; break; fi
done
[ -n "$HOST" ] || fail "no data node holds chunk $CID"
echo "  chunk=$CID host=$HOST"

echo "[3/5] forge PutChunk WITHOUT auth headers"
# Auth is checked before any stream payload is consumed — an empty client
# stream still exercises the gate.
PUT_OUT=$(grpcurl -plaintext -max-time 8 -import-path "$PROTO_DIR" -proto data.proto \
      -d "{\"chunk_id\":\"$CID\",\"volume_id\":\"default\",\"tier\":\"hot\",\"data\":\"QQ==\"}" \
      "$HOST:9500" dtfs.data.DataService/PutChunk 2>&1 || true)
echo "  grpcurl PutChunk: $PUT_OUT"
echo "$PUT_OUT" | grep -qiE 'PermissionDenied|permission denied' \
    || fail "PutChunk expected PermissionDenied, got: $PUT_OUT"

echo "[4/5] forge DeleteChunk WITHOUT auth headers"
DEL_OUT=$(grpcurl -plaintext -max-time 8 -import-path "$PROTO_DIR" -proto data.proto \
      -d "{\"chunk_id\":\"$CID\"}" \
      "$HOST:9500" dtfs.data.DataService/DeleteChunk 2>&1 || true)
echo "  grpcurl DeleteChunk: $DEL_OUT"

echo "[5/5] DeleteChunk must be PermissionDenied and chunk must survive on disk"
echo "$DEL_OUT" | grep -qiE 'PermissionDenied|permission denied' \
    || fail "DeleteChunk expected PermissionDenied, got: $DEL_OUT"
host_has_chunk "$HOST" "$CID" || fail "chunk $CID was deleted despite auth rejection"
pass

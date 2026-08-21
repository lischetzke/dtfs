#!/usr/bin/env bash
# LIS-83: unauthenticated GetChunk / ListChunks / VerifyChunk on a data
# node MUST be rejected with PERMISSION_DENIED when cluster.shared_secret
# is set. Pre-fix any peer that could reach :9500 could exfil chunk bytes,
# inventory every chunk id, or oracle presence/integrity without knowing
# the shared secret.
#
# Forging the RPCs needs grpcurl + proto/data.proto. When grpcurl is
# unavailable we SKIP (unit tests get/list/verify_chunk_rejects_missing_auth
# cover the gate; live forge is belt-and-suspenders).
source "$(dirname "$0")/../lib/common.sh"

PROTO_DIR="$(cd "$(dirname "$0")/../../../proto" 2>/dev/null && pwd)"
PREFIX=/release/82-data-rpc-auth-reads-$$/
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
    || skip "grpcurl unavailable: live forge skipped; Get/List/VerifyChunk auth unit-tested in dtfs-data"
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

echo "[3/5] forge GetChunk WITHOUT auth headers"
GET_OUT=$(grpcurl -plaintext -max-time 8 -import-path "$PROTO_DIR" -proto data.proto \
      -d "{\"chunk_id\":\"$CID\"}" \
      "$HOST:9500" dtfs.data.DataService/GetChunk 2>&1 || true)
echo "  grpcurl GetChunk: $GET_OUT"
echo "$GET_OUT" | grep -qiE 'PermissionDenied|permission denied' \
    || fail "GetChunk expected PermissionDenied, got: $GET_OUT"

echo "[4/5] forge ListChunks WITHOUT auth headers"
LIST_OUT=$(grpcurl -plaintext -max-time 8 -import-path "$PROTO_DIR" -proto data.proto \
      -d '{"limit":10}' \
      "$HOST:9500" dtfs.data.DataService/ListChunks 2>&1 || true)
echo "  grpcurl ListChunks: $LIST_OUT"
echo "$LIST_OUT" | grep -qiE 'PermissionDenied|permission denied' \
    || fail "ListChunks expected PermissionDenied, got: $LIST_OUT"

echo "[5/5] forge VerifyChunk WITHOUT auth headers"
VER_OUT=$(grpcurl -plaintext -max-time 8 -import-path "$PROTO_DIR" -proto data.proto \
      -d "{\"chunk_id\":\"$CID\"}" \
      "$HOST:9500" dtfs.data.DataService/VerifyChunk 2>&1 || true)
echo "  grpcurl VerifyChunk: $VER_OUT"
echo "$VER_OUT" | grep -qiE 'PermissionDenied|permission denied' \
    || fail "VerifyChunk expected PermissionDenied, got: $VER_OUT"
pass

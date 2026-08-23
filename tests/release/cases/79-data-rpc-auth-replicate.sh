#!/usr/bin/env bash
# LIS-55: unauthenticated ReplicateChunk on a data node MUST be rejected
# with PERMISSION_DENIED when cluster.shared_secret is set. Pre-fix any
# peer that could reach :9500 could coerce outbound PutChunk of known
# chunk ids (exfil/SSRF) without knowing the shared secret.
#
# Forging the RPC needs grpcurl + proto/data.proto. When grpcurl is
# unavailable we KNOWN_ISSUE (helpers unit-tested in dtfs-common +
# dtfs-data server tests); it auto-promotes once grpcurl exists.
source "$(dirname "$0")/../lib/common.sh"

PROTO_DIR="$(cd "$(dirname "$0")/../../../proto" 2>/dev/null && pwd)"
PREFIX=/release/79-data-rpc-auth-replicate-$$/
PATHF="${MOUNT}${PREFIX}f.bin"
APIF="${PATHF#$MOUNT}"

case_cleanup() { client_run "sudo rm -rf ${MOUNT}${PREFIX}" >/dev/null 2>&1 || true; }
trap case_cleanup EXIT

file_exists() {
    mgr_get_any "/api/v1/files/inspect?path=$APIF" \
        | python3 -c "import json,sys;d=json.load(sys.stdin);sys.exit(0 if d.get('chunks') else 1)" 2>/dev/null
}

echo "[1/4] preconditions: shared secret + grpcurl + proto"
SECRET="${SHARED_SECRET:-}"
[ -n "$SECRET" ] || SECRET="$(mgr_shared_secret 2>/dev/null || true)"
[ -n "$SECRET" ] || skip "shared_secret empty; auth check is back-compat-skipped on a no-secret cluster"
command -v grpcurl >/dev/null 2>&1 \
    || known_issue LIS-55 "grpcurl unavailable: forged ReplicateChunk not live-wirable; helper unit-tested (data_rpc_auth)"
[ -n "$PROTO_DIR" ] && [ -f "$PROTO_DIR/data.proto" ] \
    || known_issue LIS-55 "data.proto not found at $PROTO_DIR; cannot resolve the RPC without server reflection"

echo "[2/4] write a file so a real chunk id exists on a data node"
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

echo "[3/4] forge ReplicateChunk WITHOUT auth headers"
OUT=$(grpcurl -plaintext -max-time 8 -import-path "$PROTO_DIR" -proto data.proto \
      -d "{\"chunk_id\":\"$CID\",\"target_address\":\"http://127.0.0.1:19999\",\"target_tier\":\"warm\"}" \
      "$HOST:9500" dtfs.data.DataService/ReplicateChunk 2>&1 || true)
echo "  grpcurl: $OUT"

echo "[4/4] must be PermissionDenied; chunk must remain on source"
echo "$OUT" | grep -qiE 'PermissionDenied|permission denied' \
    || fail "expected PermissionDenied, got: $OUT"
host_has_chunk "$HOST" "$CID" || fail "chunk $CID vanished after auth rejection"
pass

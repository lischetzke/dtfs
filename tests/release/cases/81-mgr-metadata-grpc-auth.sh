#!/usr/bin/env bash
# LIS-56 / Sec HIGH: unauthenticated DeleteFile on manager gRPC MUST be
# rejected with PERMISSION_DENIED when cluster.shared_secret is set.
# Pre-fix anyone who could reach :9401 could mutate namespace metadata
# (Create/Delete/Rename/leases) while REST was already gated (LIS-18).
#
# Forging the RPC needs grpcurl + proto/manager.proto. When grpcurl is
# unavailable we KNOWN_ISSUE (helpers unit-tested in dtfs-common +
# dtfs-manager lis56_mgr_rpc_auth_tests); it auto-promotes once grpcurl exists.
source "$(dirname "$0")/../lib/common.sh"

PROTO_DIR="$(cd "$(dirname "$0")/../../../proto" 2>/dev/null && pwd)"
PREFIX=/release/81-mgr-rpc-auth-$$/
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
    || known_issue LIS-56 "grpcurl unavailable: forged DeleteFile not live-wirable; helper unit-tested (mgr_rpc_auth)"
[ -n "$PROTO_DIR" ] && [ -f "$PROTO_DIR/manager.proto" ] \
    || known_issue LIS-56 "manager.proto not found at $PROTO_DIR; cannot resolve the RPC without server reflection"

echo "[2/4] write a target file that a forged DeleteFile would remove"
client_run "sudo mkdir -p ${MOUNT}${PREFIX} && head -c 4096 /dev/urandom | sudo tee '$PATHF' >/dev/null && sudo sync" \
    || fail "could not stage target file"
wait_for 30 "target file registered" file_exists

echo "[3/4] forge DeleteFile WITHOUT auth headers against a manager"
MGR=""
for h in $(mgrs_list); do MGR="$h"; break; done
[ -n "$MGR" ] || fail "no manager host"
OUT=$(grpcurl -plaintext -max-time 8 -import-path "$PROTO_DIR" -proto manager.proto \
      -d "{\"path\":\"$APIF\"}" \
      "$MGR:9401" dtfs.manager.ManagerService/DeleteFile 2>&1 || true)
echo "  mgr=$MGR grpcurl: $OUT"

echo "[4/4] must be PermissionDenied and file must survive"
echo "$OUT" | grep -qiE 'PermissionDenied|permission denied' \
    || fail "expected PermissionDenied, got: $OUT"
file_exists || fail "file $APIF was deleted despite auth rejection"
pass

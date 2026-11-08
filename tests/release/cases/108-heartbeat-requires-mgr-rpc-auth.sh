#!/usr/bin/env bash
# CR-5-1 (PROTO-1) / LIS-56: unauthenticated Heartbeat on a manager MUST
# be rejected with PERMISSION_DENIED when cluster.shared_secret is set.
# Pre-fix, Heartbeat and ReportChunkStatus were the only two mutating
# ManagerService RPCs not gated by require_mgr_rpc_auth, so any caller
# who knew (or guessed) a node id could poison that node's health status
# or drop replica entries without the cluster secret.
#
# Forging the RPC needs grpcurl + proto/manager.proto. When grpcurl is
# unavailable we KNOWN_ISSUE (the gate is unit-tested in dtfs-manager's
# lis56_mgr_rpc_auth_tests); it auto-promotes once grpcurl exists.
source "$(dirname "$0")/../lib/common.sh"

PROTO_DIR="$(cd "$(dirname "$0")/../../../proto" 2>/dev/null && pwd)"
MGR_GRPC_PORT=${MGR_GRPC_PORT:-9401}

case_cleanup() { :; }
trap case_cleanup EXIT

echo "[1/3] preconditions: shared secret + grpcurl + proto"
SECRET="${SHARED_SECRET:-}"
[ -n "$SECRET" ] || SECRET="$(mgr_shared_secret 2>/dev/null || true)"
[ -n "$SECRET" ] || skip "shared_secret empty; auth check is back-compat-skipped on a no-secret cluster"
command -v grpcurl >/dev/null 2>&1 \
    || known_issue PROTO-1 "grpcurl unavailable: forged Heartbeat not live-wirable; gate unit-tested (lis56_mgr_rpc_auth_tests)"
[ -n "$PROTO_DIR" ] && [ -f "$PROTO_DIR/manager.proto" ] \
    || known_issue PROTO-1 "manager.proto not found at $PROTO_DIR; cannot resolve the RPC without server reflection"

HOST=""
for h in $(mgrs_list); do HOST="$h"; break; done
[ -n "$HOST" ] || fail "no manager host configured"
echo "  manager=$HOST:$MGR_GRPC_PORT"

echo "[2/3] forge Heartbeat WITHOUT auth headers"
OUT=$(grpcurl -plaintext -max-time 8 -import-path "$PROTO_DIR" -proto manager.proto \
      -d '{"node_id":"1","uptime_secs":1}' \
      "$HOST:$MGR_GRPC_PORT" dtfs.manager.ManagerService/Heartbeat 2>&1 || true)
echo "  grpcurl: $OUT"

echo "[3/3] must be PermissionDenied"
echo "$OUT" | grep -qiE 'PermissionDenied|permission denied' \
    || fail "expected PermissionDenied, got: $OUT"
pass

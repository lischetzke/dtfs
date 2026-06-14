#!/usr/bin/env bash
# CO-2-2 [HIGH security]: a follower manager's ForwardClientWrite RPC must
# authenticate the forwarding peer with the cluster shared secret. Pre-fix any
# peer that could reach a follower's gRPC port could drive arbitrary client
# writes (including DeleteFile) with NO authentication, because the follower
# applied/forwarded the embedded ClientRequest unconditionally.
#
# Post-fix: a forged ForwardClientWrite carrying a wrong/missing shared_secret
# is rejected with PERMISSION_DENIED and the target file survives. When the
# cluster runs WITHOUT a shared secret the check is skipped (back-compat) and
# this case SKIPs.
#
# Forging the RPC needs a raw gRPC client (grpcurl) + the manager proto (the
# manager gRPC server has no reflection, so we point grpcurl at proto/). When
# grpcurl is unavailable we KNOWN_ISSUE (the helper is unit-tested in
# dtfs-manager co_2_2_forward_auth_tests); it auto-promotes once grpcurl exists.
source "$(dirname "$0")/../lib/common.sh"
source "$(dirname "$0")/../lib/dc.sh"

PROTO_DIR="$(cd "$(dirname "$0")/../../../proto" 2>/dev/null && pwd)"
PREFIX=/release/64-fwdauth-$$/
PATHF="${MOUNT}${PREFIX}f.bin"
APIF="${PATHF#$MOUNT}"  # mount-relative path the manager namespace uses

CW=$(client_for_dc dc-a); [ -n "$CW" ] || CW="$CLIENT"
[ -n "$CW" ] || skip "no client available"

case_cleanup() { client_run "sudo rm -rf ${MOUNT}${PREFIX}" >/dev/null 2>&1 || true; }
trap case_cleanup EXIT

file_exists() {  # via the cluster (manager namespace), not the harness FS
    mgr_get_any "/api/v1/files/inspect?path=$APIF" \
        | python3 -c "import json,sys;d=json.load(sys.stdin);sys.exit(0 if d.get('chunks') else 1)" 2>/dev/null
}

echo "[1/4] preconditions: shared secret + grpcurl + proto"
[ -n "${SHARED_SECRET:-}" ] || skip "SHARED_SECRET not provided; auth check is back-compat-skipped on a no-secret cluster"
command -v grpcurl >/dev/null 2>&1 \
    || known_issue CO-2-2 "grpcurl unavailable: forged ForwardClientWrite not live-wirable; helper unit-tested (co_2_2_forward_auth_tests)"
[ -n "$PROTO_DIR" ] && [ -f "$PROTO_DIR/manager.proto" ] \
    || known_issue CO-2-2 "manager.proto not found at $PROTO_DIR; cannot resolve the RPC without server reflection"

echo "[2/4] write a target file (via $CW) that a forged DeleteFile would remove"
ssh_run "$CW" "sudo mkdir -p ${MOUNT}${PREFIX} && head -c 4096 /dev/urandom | sudo tee '$PATHF' >/dev/null && sudo sync" \
    || fail "could not stage target file"
wait_for 30 "target file registered" file_exists

echo "[3/4] find a follower + forge ForwardClientWrite(DeleteFile) with a WRONG secret"
TARGET=""
for h in $(mgrs_list); do
    st=$(curl "${CURL_OPTS[@]}" "http://$h:$MGR_PORT/api/v1/raft/state" 2>/dev/null \
         | python3 -c "import json,sys;print(json.load(sys.stdin).get('state',''))" 2>/dev/null)
    [ "$st" = "Follower" ] && { TARGET="$h"; break; }
done
[ -n "$TARGET" ] || known_issue CO-2-2 "no follower manager found to target the forged forward"

REQ_B64=$(python3 -c "import json,base64,sys;print(base64.b64encode(json.dumps({'DeleteFile':{'path':sys.argv[1]}}).encode()).decode())" "$APIF")
OUT=$(grpcurl -plaintext -max-time 8 -import-path "$PROTO_DIR" -proto manager.proto \
      -d "{\"client_request_json\":\"$REQ_B64\",\"shared_secret\":\"WRONG-SECRET\"}" \
      "$TARGET:9401" dtfs.manager.ManagerService/ForwardClientWrite 2>&1 || true)
echo "  grpcurl response: $(echo "$OUT" | head -c 200)"
echo "$OUT" | grep -qi "PermissionDenied\|permission_denied\|shared-secret mismatch\|denied" \
    || fail "forged forward was NOT rejected with PERMISSION_DENIED: $OUT"

echo "[4/4] target file must survive the forged DeleteFile (rejected before any Raft apply)"
file_exists || fail "target file was deleted by the forged forward (auth bypass)"
echo "  forged ForwardClientWrite rejected; file intact"
pass

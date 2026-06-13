#!/usr/bin/env bash
# CO-2-2 [HIGH security]: a follower manager's ForwardClientWrite RPC must
# authenticate the forwarding peer with the cluster shared secret. Pre-fix
# any peer that could reach a follower's gRPC port could drive arbitrary
# client writes (including DeleteFile) with NO authentication, because the
# follower applied/forwarded the embedded ClientRequest unconditionally.
#
# Post-fix: a forged ForwardClientWrite carrying a wrong/missing
# shared_secret is rejected with PERMISSION_DENIED and the target file
# survives. When the cluster runs WITHOUT a shared secret the check is
# skipped (back-compat) and this case SKIPs.
#
# Forging the RPC needs a raw gRPC client (grpcurl) with the manager proto.
# When grpcurl + the descriptor are unavailable in the harness we cannot
# wire the forged call live, so the case reports KNOWN_ISSUE [CO-2-2]
# (the helper-level guarantee is unit-tested in dtfs-manager
# co_2_2_forward_auth_tests). It auto-promotes once a live forge is wired.
source "$(dirname "$0")/../lib/common.sh"

F="$MOUNT/release/64-forward-auth-$$.bin"

case_cleanup() {
    [ -n "$MOUNT" ] && rm -f "$F" 2>/dev/null || true
}
trap case_cleanup EXIT

echo "[1/3] confirm cluster runs WITH a shared secret (else the auth check is intentionally skipped)"
if [ -z "${SHARED_SECRET:-}" ]; then
    skip "SHARED_SECRET not provided to the harness; cannot exercise the auth path (back-compat skip)"
fi

echo "[2/3] write a target file that a forged DeleteFile would try to remove"
if [ -n "$MOUNT" ]; then
    head -c 4096 /dev/urandom > "$F" 2>/dev/null || true
fi

echo "[3/3] attempt a forged ForwardClientWrite(DeleteFile) with a WRONG secret"
if ! command -v grpcurl >/dev/null 2>&1; then
    known_issue CO-2-2 "grpcurl unavailable: forged ForwardClientWrite not live-wirable here; helper guaranteed by dtfs-manager co_2_2_forward_auth_tests (constant-time compare + empty-secret back-compat)"
fi

# A follower (any non-leader manager) is the interesting target.
TARGET=""
for h in $(mgrs_list); do
    st=$(curl "${CURL_OPTS[@]}" "http://$h:$MGR_PORT/api/v1/raft/state" 2>/dev/null \
         | python3 -c "import json,sys;print(json.load(sys.stdin).get('state',''))" 2>/dev/null)
    [ "$st" = "Follower" ] && { TARGET="$h"; break; }
done
[ -n "$TARGET" ] || known_issue CO-2-2 "no follower manager found to target the forged forward"

# Build the JSON ClientRequest body (DeleteFile of our target path) and a
# ForwardWriteRequest carrying a deliberately wrong shared_secret.
REQ=$(python3 -c "import json,sys;print(json.dumps({'DeleteFile':{'path':sys.argv[1]}}))" "$F")
REQ_B64=$(printf '%s' "$REQ" | base64 | tr -d '\n')
OUT=$(grpcurl -plaintext -d "{\"client_request_json\":\"$REQ_B64\",\"shared_secret\":\"WRONG-SECRET\"}" \
      "$TARGET:9401" dtfs.manager.ManagerService/ForwardClientWrite 2>&1 || true)
echo "  grpcurl response: $(echo "$OUT" | head -c 200)"

echo "$OUT" | grep -qi "PermissionDenied\|permission_denied\|denied" \
    || fail "forged forward was NOT rejected with PERMISSION_DENIED: $OUT"

# File must survive (forged DeleteFile rejected before any Raft apply).
[ -n "$MOUNT" ] && { [ -f "$F" ] || fail "target file was deleted by the forged forward"; }

pass

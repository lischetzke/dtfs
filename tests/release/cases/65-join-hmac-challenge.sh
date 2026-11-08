#!/usr/bin/env bash
# 64-join-hmac-challenge.sh — CO-2-3: the two-phase node-join handshake must
# prove possession of cluster.shared_secret on /join/complete and derive
# node_id server-side from the hostname.
#
# Asserts:
#   * a forged /join/complete with NO prior /join/request (and no valid
#     challenge/HMAC) is rejected (success:false),
#   * a /join/complete carrying a real challenge but a BAD HMAC is rejected,
#   * a legitimate two-phase join (request -> complete with
#     HMAC(secret, challenge)) succeeds, and the node is registered under its
#     hostname (node_id derived server-side, not from any client field),
#   * replaying the same challenge is rejected (single-use).
#
# SKIPs on a legacy plaintext cluster (empty shared_secret) — the downgrade is
# intentional and unauthenticated, so there is nothing to assert.
. "$(dirname "$0")/../lib/common.sh"

JOIN_HOST="co23-join-test-$$"

# Test hygiene: the legit join (step 4) registers JOIN_HOST in the cluster
# registry; it never heartbeats again, so without cleanup it lingers as a
# Down phantom and drives cluster health to "degraded" across release runs.
# Decommission it on exit via the leader-gated /decommission endpoint (the
# node holds no chunk replicas, so the integrity guard allows removal).
case_cleanup() {
    local nid
    nid=$(mgr_get_any /api/v1/nodes | python3 -c "
import json,sys
host='$JOIN_HOST'
for n in json.load(sys.stdin):
    if n.get('hostname')==host:
        print(n.get('node_id','')); break" 2>/dev/null)
    [ -n "$nid" ] || return 0
    for h in $(mgrs_list); do
        curl "${CURL_OPTS[@]}" -X POST -H 'content-type: application/json' \
            -d '{"operator_id":"release-suite","reason":"CO-2-3 join test cleanup: remove ephemeral test node"}' \
            "http://$h:$MGR_PORT/api/v1/nodes/$nid/decommission" 2>/dev/null \
            | grep -q '"ok":true' && break
    done
}
trap case_cleanup EXIT

first_mgr=$(mgrs_list | awk '{print $1}')

SECRET=$(ssh_run "$first_mgr" "python3 - <<'EOF'
import tomllib
with open('/etc/dtfs/manager.toml','rb') as f:
    print(tomllib.load(f)['cluster'].get('shared_secret',''))
EOF
")
[ -n "$SECRET" ] || skip "cluster has empty shared_secret (legacy plaintext downgrade); nothing to authenticate"

# HMAC-SHA256(secret, msg) as lowercase hex.
hmac_hex() {
    python3 -c "
import hmac,hashlib,sys
print(hmac.new(sys.argv[1].encode(), sys.argv[2].encode(), hashlib.sha256).hexdigest())
" "$1" "$2"
}

# The phase-1 challenge store is per-manager and in-memory, and phase-2 does a
# local Raft write, so BOTH phases must hit the same manager AND that manager
# must be the leader (else the write forwards and the local challenge check
# would run on the wrong node). Find the leader (retrying while an election
# settles) and pin every join RPC to it.
find_leader() {
    for h in $(mgrs_list); do
        curl "${CURL_OPTS[@]}" "http://$h:$MGR_PORT/api/v1/raft/state" 2>/dev/null \
            | grep -q '"is_leader":true' && { echo "$h"; return 0; }
    done
    return 1
}
JOIN_MGR=""
for _ in $(seq 1 10); do
    JOIN_MGR=$(find_leader) && [ -n "$JOIN_MGR" ] && break
    sleep 2
done
[ -n "$JOIN_MGR" ] || fail "could not locate the leader manager host"
echo "  leader manager: $JOIN_MGR"

post_join() {
    local path="$1" body="$2"
    curl "${CURL_OPTS[@]}" -X POST -H 'content-type: application/json' \
        -d "$body" "http://$JOIN_MGR:$MGR_PORT$path" 2>/dev/null
}

echo "[1/5] forged /join/complete with no prior request is rejected"
FORGED=$(post_join /api/v1/join/complete \
    "{\"challenge\":\"never-issued\",\"completion_hmac_hex\":\"$(hmac_hex "$SECRET" never-issued)\",\"hostname\":\"$JOIN_HOST\",\"grpc_addr\":\"127.0.0.1:9599\"}")
echo "  resp: $(echo "$FORGED" | head -c 160)"
echo "$FORGED" | grep -q '"success":false' || fail "forged complete (no challenge) was not rejected: $FORGED"

echo "[2/5] phase-1 /join/request returns a challenge"
REQ_HMAC=$(hmac_hex "$SECRET" co23-nonce)
REQ=$(post_join /api/v1/join/request \
    "{\"nonce\":\"co23-nonce\",\"hmac_hex\":\"$REQ_HMAC\",\"hostname\":\"$JOIN_HOST\"}")
echo "  resp: $(echo "$REQ" | head -c 160)"
echo "$REQ" | grep -q '"accepted":true' || fail "phase-1 request rejected: $REQ"
CHALLENGE=$(echo "$REQ" | python3 -c 'import json,sys; print(json.load(sys.stdin)["challenge"])')
[ -n "$CHALLENGE" ] || fail "no challenge returned from phase-1"

echo "[3/5] /join/complete with valid challenge but BAD HMAC is rejected"
BAD=$(post_join /api/v1/join/complete \
    "{\"challenge\":\"$CHALLENGE\",\"completion_hmac_hex\":\"$(hmac_hex wrong-secret "$CHALLENGE")\",\"hostname\":\"$JOIN_HOST\",\"grpc_addr\":\"127.0.0.1:9599\"}")
echo "  resp: $(echo "$BAD" | head -c 160)"
echo "$BAD" | grep -q '"success":false' || fail "bad-HMAC complete was not rejected: $BAD"

echo "[4/5] legitimate two-phase join succeeds (fresh challenge)"
# The bad-HMAC attempt above consumed the challenge (single-use), so request a
# fresh one for the legit completion.
REQ2=$(post_join /api/v1/join/request \
    "{\"nonce\":\"co23-nonce2\",\"hmac_hex\":\"$(hmac_hex "$SECRET" co23-nonce2)\",\"hostname\":\"$JOIN_HOST\"}")
CHAL2=$(echo "$REQ2" | python3 -c 'import json,sys; print(json.load(sys.stdin)["challenge"])')
[ -n "$CHAL2" ] || fail "no challenge returned from second phase-1"
OK=$(post_join /api/v1/join/complete \
    "{\"challenge\":\"$CHAL2\",\"completion_hmac_hex\":\"$(hmac_hex "$SECRET" "$CHAL2")\",\"hostname\":\"$JOIN_HOST\",\"grpc_addr\":\"127.0.0.1:9599\"}")
echo "  resp: $(echo "$OK" | head -c 160)"
echo "$OK" | grep -q '"success":true' || fail "legit two-phase join failed: $OK"

# node_id is derived server-side from the hostname (the request body carries
# no node_id at all), so the node must appear keyed by its hostname with a
# stable non-zero id.
node_registered() {
    mgr_get_any /api/v1/nodes | python3 -c "
import json,sys
host=sys.argv[1]
for n in json.load(sys.stdin):
    if n.get('hostname')==host and int(n.get('node_id',0))!=0:
        sys.exit(0)
sys.exit(1)" "$JOIN_HOST"
}
wait_for 20 "node registered under server-derived node_id for $JOIN_HOST" node_registered

echo "[5/5] replaying the consumed challenge is rejected (single-use)"
REPLAY=$(post_join /api/v1/join/complete \
    "{\"challenge\":\"$CHAL2\",\"completion_hmac_hex\":\"$(hmac_hex "$SECRET" "$CHAL2")\",\"hostname\":\"$JOIN_HOST\",\"grpc_addr\":\"127.0.0.1:6666\"}")
echo "  resp: $(echo "$REPLAY" | head -c 160)"
echo "$REPLAY" | grep -q '"success":false' || fail "replayed challenge was not rejected: $REPLAY"

# The replay used a different grpc_addr; confirm it did NOT overwrite the live
# node's grpc_addr (the rejected complete must not touch state).
GRPC=$(mgr_get_any /api/v1/nodes | python3 -c "
import json,sys
host=sys.argv[1]
for n in json.load(sys.stdin):
    if n.get('hostname')==host:
        print(n.get('grpc_addr',''))
        break" "$JOIN_HOST")
assert_eq "127.0.0.1:9599" "$GRPC" "grpc_addr unchanged by replayed/forged complete"

pass

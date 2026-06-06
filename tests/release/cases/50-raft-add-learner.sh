#!/usr/bin/env bash
# 50-raft-add-learner.sh — CF-2: online raft membership change.
#
# Spawns a throwaway dtfs-manager ("cf2-test-learner") on the first
# manager host with its own ports + data dir, then drives the full
# membership lifecycle through the new API:
#   add-learner -> learner replicates FULL state (snapshot/log catch-up,
#   cluster view converges) -> promote to voter -> remove -> gone.
# Also asserts the gates: wrong shared_secret is refused, and a
# follower answers 409 with a leader hint.
. "$(dirname "$0")/../lib/common.sh"

LEARNER_HOST=""
LEARNER_NAME="cf2-test-learner"
LEARNER_DIR="/tmp/cf2-learner"
LEARNER_GRPC_PORT=9411
LEARNER_API_PORT=9412

case_cleanup() {
    if [ -n "$LEARNER_HOST" ]; then
        # Best-effort: drop the learner from membership, then kill it.
        if [ -n "${SECRET:-}" ]; then
            post_leader_raft "/api/v1/raft/remove" \
                "{\"hostname\":\"$LEARNER_NAME\",\"shared_secret\":\"$SECRET\"}" >/dev/null 2>&1
        fi
        # [d]tfs trick: keep the pattern from matching this ssh shell's
        # own cmdline (pkill -f would otherwise kill the cleanup itself
        # before the rm runs).
        ssh_run "$LEARNER_HOST" "sudo pkill -f '[d]tfs-manager --config $LEARNER_DIR.toml' 2>/dev/null; \
                                 sudo rm -rf $LEARNER_DIR $LEARNER_DIR.toml $LEARNER_DIR.log" || true
    fi
}
trap case_cleanup EXIT

# POST a membership change, walking managers until one accepts it (the
# non-leaders answer 409 not-the-leader; mgr_post_leader's forward-error
# grep does not match these endpoints' shape).
post_leader_raft() {
    local path="$1" body="$2"
    for h in $(mgrs_list); do
        local r=$(curl "${CURL_OPTS[@]}" -X POST -H 'content-type: application/json' \
                  -d "$body" "http://$h:$MGR_PORT$path" 2>/dev/null)
        if [ -n "$r" ] && ! echo "$r" | grep -q 'not the leader'; then
            echo "$r"
            return 0
        fi
    done
    return 1
}

first_mgr=$(mgrs_list | awk '{print $1}')
LEARNER_HOST="$first_mgr"

ssh_run "$first_mgr" 'test -x /usr/local/bin/dtfs-manager' \
    || skip "dtfs-manager binary not installed on $first_mgr"

# ── 1. Build the learner's config from the host's own manager.toml ──
SECRET=$(ssh_run "$first_mgr" "python3 - <<'EOF'
import tomllib
with open('/etc/dtfs/manager.toml','rb') as f:
    print(tomllib.load(f)['cluster']['shared_secret'])
EOF
")
[ -n "$SECRET" ] || fail "could not read shared_secret from $first_mgr"

# NOTE: no [[cluster.initial_voters]] in the throwaway's config — with
# an empty list its BR-1 self-register loop only ever talks to its own
# loopback (which has no leader to forward to), so the shared node
# registry is NOT polluted with a stale test-manager entry.
ssh_run "$first_mgr" "python3 - <<'EOF'
import subprocess, tomllib
cfg = tomllib.load(open('/etc/dtfs/manager.toml','rb'))
# grpc.listen_addr is usually the 0.0.0.0 wildcard — derive the routable
# IP from this host's own initial_voters entry (Bug AC identity contract),
# falling back to the first hostname -I address.
me = cfg['node']['hostname']
ip = next((v['addr'].rsplit(':',1)[0]
           for v in cfg['cluster'].get('initial_voters', [])
           if v.get('hostname') == me), None) \
     or subprocess.run(['hostname','-I'], capture_output=True, text=True).stdout.split()[0]
toml = '''[node]
id = \"auto\"
hostname = \"$LEARNER_NAME\"
data_dir = \"$LEARNER_DIR\"

[location]
region = \"eu\"
datacenter = \"dc-test\"
rack = \"r1\"
host = \"$LEARNER_NAME\"

[cluster]
name = \"%s\"
shared_secret = \"%s\"
bootstrap = false

[tls]
mode = \"auto\"

# required sections; every inner field has a serde default
[defaults]
[tiering]
[monitoring]

[raft]
listen_addr = \"%s:9410\"

[grpc]
listen_addr = \"%s:$LEARNER_GRPC_PORT\"

[api]
listen_addr = \"%s:$LEARNER_API_PORT\"
''' % (cfg['cluster'].get('name','dtfs'), cfg['cluster']['shared_secret'], ip, ip, ip)
open('/tmp/cf2-learner.toml.new','w').write(toml)
print(ip)
EOF
" > /tmp/cf2-ip.$$ || fail "config generation failed"
LEARNER_IP=$(tail -1 /tmp/cf2-ip.$$); rm -f /tmp/cf2-ip.$$
[ -n "$LEARNER_IP" ] || fail "could not derive learner IP"

ssh_run "$first_mgr" "sudo mv /tmp/cf2-learner.toml.new $LEARNER_DIR.toml && sudo mkdir -p $LEARNER_DIR && \
    sudo sh -c 'nohup /usr/local/bin/dtfs-manager --config $LEARNER_DIR.toml > $LEARNER_DIR.log 2>&1 &'"

learner_api() { curl "${CURL_OPTS[@]}" "http://$LEARNER_IP:$LEARNER_API_PORT$1" 2>/dev/null; }

learner_up() { learner_api /api/v1/health | grep -q healthy; }
wait_for 30 "test learner manager comes up" learner_up

# Pre-join sanity: empty, uninitialized learner state.
pre=$(learner_api /api/v1/raft/state)
assert_match '"voters":\[\]' "$pre" "pre-join learner voters"

# ── 2. Gates: bad secret refused; follower answers with leader hint ──
bad=$(post_leader_raft "/api/v1/raft/add-learner" \
    "{\"hostname\":\"$LEARNER_NAME\",\"addr\":\"$LEARNER_IP:$LEARNER_GRPC_PORT\",\"shared_secret\":\"wrong\"}") \
    && true
assert_match 'shared_secret mismatch' "${bad:-}" "bad-secret gate"

leader_id=$(mgr_get_any /api/v1/raft/state | python3 -c 'import json,sys; print(json.load(sys.stdin)["current_leader"] or "")')
for h in $(mgrs_list); do
    s=$(curl "${CURL_OPTS[@]}" "http://$h:$MGR_PORT/api/v1/raft/state" 2>/dev/null)
    if ! echo "$s" | grep -q "\"is_leader\":true"; then
        hint=$(curl "${CURL_OPTS[@]}" -X POST -H 'content-type: application/json' \
            -d "{\"hostname\":\"x\",\"shared_secret\":\"$SECRET\"}" \
            "http://$h:$MGR_PORT/api/v1/raft/promote" 2>/dev/null)
        assert_match 'not the leader' "$hint" "follower 409"
        assert_match "$leader_id" "$hint" "leader hint carries leader id"
        break
    fi
done

# ── 3. add-learner -> full state replication ─────────────────────
r=$(post_leader_raft "/api/v1/raft/add-learner" \
    "{\"hostname\":\"$LEARNER_NAME\",\"addr\":\"$LEARNER_IP:$LEARNER_GRPC_PORT\",\"shared_secret\":\"$SECRET\"}")
assert_match '"status":"ok"' "$r" "add-learner response"
nid=$(echo "$r" | python3 -c 'import json,sys; print(json.load(sys.stdin)["node_id"])')

leader_sees_learner() {
    mgr_get_any /api/v1/raft/state | grep -q "\"learners\":\[[^]]*\"$nid\""
}
wait_for 30 "leader lists new learner" leader_sees_learner

learner_synced() {
    local s=$(learner_api /api/v1/raft/state)
    echo "$s" | python3 -c '
import json,sys
try: d=json.load(sys.stdin)
except Exception: sys.exit(1)
ok = d.get("current_leader") and d.get("applied_log_lag")==0 and d.get("last_applied_index",0)>0
sys.exit(0 if ok else 1)'
}
wait_for 60 "learner replicates state (leader known, lag 0)" learner_synced

# Full state replication: the learner's own API now serves the real
# cluster view (this is the production dc1 symptom this exists to fix).
cluster_view_converged() {
    local want=$(mgr_get_any /api/v1/cluster/status | python3 -c 'import json,sys; print(json.load(sys.stdin)["data_nodes"])')
    local got=$(learner_api /api/v1/cluster/status | python3 -c 'import json,sys; print(json.load(sys.stdin)["data_nodes"])' 2>/dev/null)
    [ -n "$got" ] && [ "$got" = "$want" ] && [ "$got" -ge 1 ]
}
wait_for 30 "learner serves the real cluster view" cluster_view_converged

# Voter set must be unchanged by add-learner.
voters_now=$(mgr_get_any /api/v1/raft/state | python3 -c 'import json,sys; print(len(json.load(sys.stdin)["voters"]))')
base_voters=$voters_now

# ── 4. promote -> voter ──────────────────────────────────────────
r=$(post_leader_raft "/api/v1/raft/promote" \
    "{\"hostname\":\"$LEARNER_NAME\",\"shared_secret\":\"$SECRET\"}")
assert_match '"status":"ok"' "$r" "promote response"

is_voter_now() {
    mgr_get_any /api/v1/raft/state | python3 -c "
import json,sys
sys.exit(0 if \"$nid\" in json.load(sys.stdin)['voters'] else 1)"
}
wait_for 30 "learner promoted to voter" is_voter_now

# ── 5. remove -> gone, voter count restored ──────────────────────
r=$(post_leader_raft "/api/v1/raft/remove" \
    "{\"hostname\":\"$LEARNER_NAME\",\"shared_secret\":\"$SECRET\"}")
assert_match '"status":"ok"' "$r" "remove response"

fully_removed() {
    mgr_get_any /api/v1/raft/state | python3 -c "
import json,sys
d=json.load(sys.stdin)
gone = \"$nid\" not in d['voters'] and \"$nid\" not in d.get('learners',[])
sys.exit(0 if gone and len(d['voters'])==$base_voters else 1)"
}
wait_for 30 "node removed, voter set restored" fully_removed

# Audit trail for the three ops. NOTE: the local audit ring returns
# OLDEST-N (CF-4a will flip it) — fetch big enough to cover the whole
# retained ring so the fresh raft.* rows are inside the window.
audit=$(mgr_get_any "/api/v1/audit?limit=5000")
assert_match 'raft.add_learner' "$audit" "audit add_learner"
assert_match 'raft.promote' "$audit" "audit promote"
assert_match 'raft.remove' "$audit" "audit remove"

pass

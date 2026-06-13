#!/usr/bin/env bash
# 64-snapshot-learner-under-shaping.sh — CO-3-6: a learner behind the 40 Mbps
# LELI shape must be able to install a large (>25 MB) snapshot.
#
# Pre-CO-3-6 the snapshot-stream RPC inherited the 5s channel default timeout,
# so a >25 MB snapshot over the 40 Mbps link could never finish in 5s -> the
# RPC timed out, openraft retried forever, the learner never caught up. With a
# per-request, size-scaled timeout the install completes and the learner's
# committed log catches up to the leader.
#
# Needs the shaped 3-DC PVE lab (ansible netshape) + the membership API; SKIPs
# otherwise. NOT in QUICK_CASES (spawns a throwaway manager + applies shaping).
source "$(dirname "$0")/../lib/common.sh"
require_pve
command -v ansible-playbook >/dev/null 2>&1 || skip "ansible not on runner (needed for netshape)"

LEARNER_NAME="co36-test-learner"
LEARNER_DIR="/tmp/co36-learner"
LEARNER_GRPC_PORT=9421
LEARNER_API_PORT=9422
LEARNER_RAFT_PORT=9420
LEARNER_HOST=""
LEARNER_IP=""
LEARNER_NID=""
SECRET=""
SHAPED=0

post_leader_raft() {
    local path="$1" body="$2"
    for h in $(mgrs_list); do
        local r
        r=$(curl "${CURL_OPTS[@]}" -X POST -H 'content-type: application/json' \
            -d "$body" "http://$h:$MGR_PORT$path" 2>/dev/null)
        if [ -n "$r" ] && ! echo "$r" | grep -q 'not the leader'; then
            echo "$r"; return 0
        fi
    done
    return 1
}

case_cleanup() {
    [ "$SHAPED" = "1" ] && ansible-playbook -i "$INV3DC" \
        "$ANSIBLE_DIR/playbooks/revert-shaping.yml" >/dev/null 2>&1 || true
    if [ -n "$LEARNER_HOST" ]; then
        [ -n "$SECRET" ] && post_leader_raft "/api/v1/raft/remove" \
            "{\"hostname\":\"$LEARNER_NAME\",\"shared_secret\":\"$SECRET\"}" >/dev/null 2>&1
        ssh_run "$LEARNER_HOST" "sudo pkill -f '[d]tfs-manager --config $LEARNER_DIR.toml' 2>/dev/null; \
                                 sudo rm -rf $LEARNER_DIR $LEARNER_DIR.toml $LEARNER_DIR.log" || true
    fi
}
trap case_cleanup EXIT

first_mgr=$(mgrs_list | awk '{print $1}')
LEARNER_HOST="$first_mgr"
ssh_run "$first_mgr" 'test -x /usr/local/bin/dtfs-manager' \
    || skip "dtfs-manager binary not installed on $first_mgr"

SECRET=$(ssh_run "$first_mgr" "python3 - <<'EOF'
import tomllib
with open('/etc/dtfs/manager.toml','rb') as f:
    print(tomllib.load(f)['cluster']['shared_secret'])
EOF
")
[ -n "$SECRET" ] || fail "could not read shared_secret from $first_mgr"

echo "[1/5] grow leader state so the snapshot is >25 MB"
# Many small files -> large metadata snapshot. Best-effort; the learner only
# needs a snapshot bigger than what 5s could move over 40 Mbps (~25 MB).
LOCAL=/tmp/co36-src-$$.bin
head -c $((64 * 1024)) /dev/urandom > "$LOCAL"
CW=$(client_for_dc dc-a)
[ -n "$CW" ] || skip "need a dc-a client to seed state"
scp "${SSH_OPTS[@]}" "$LOCAL" "debian@$CW:/tmp/co36-src.bin" >/dev/null 2>&1 || skip "cannot stage seed"
ssh_run "$CW" "for i in \$(seq 1 400); do sudo cp /tmp/co36-src.bin '$MOUNT/release/co36-\$i.bin' 2>/dev/null; done; sudo sync" || true
ssh_run "$CW" "sudo rm -f /tmp/co36-src.bin"; rm -f "$LOCAL"
sleep 5

echo "[2/5] spawn a throwaway learner manager on $first_mgr"
ssh_run "$first_mgr" "python3 - <<'EOF'
import subprocess, tomllib
cfg = tomllib.load(open('/etc/dtfs/manager.toml','rb'))
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
datacenter = \"dc-a\"
rack = \"r1\"
host = \"$LEARNER_NAME\"

[cluster]
name = \"%s\"
shared_secret = \"%s\"
bootstrap = false

[tls]
mode = \"auto\"

[defaults]
[tiering]
[monitoring]

[raft]
listen_addr = \"%s:$LEARNER_RAFT_PORT\"

[grpc]
listen_addr = \"%s:$LEARNER_GRPC_PORT\"

[api]
listen_addr = \"%s:$LEARNER_API_PORT\"
''' % (cfg['cluster'].get('name','dtfs'), cfg['cluster']['shared_secret'], ip, ip, ip)
open('/tmp/co36-learner.toml.new','w').write(toml)
print(ip)
EOF
" > /tmp/co36-ip.$$ || fail "config generation failed"
LEARNER_IP=$(tail -1 /tmp/co36-ip.$$); rm -f /tmp/co36-ip.$$
[ -n "$LEARNER_IP" ] || fail "could not derive learner IP"

ssh_run "$first_mgr" "sudo mv /tmp/co36-learner.toml.new $LEARNER_DIR.toml && sudo mkdir -p $LEARNER_DIR && \
    sudo sh -c 'nohup /usr/local/bin/dtfs-manager --config $LEARNER_DIR.toml > $LEARNER_DIR.log 2>&1 &'"

learner_api() { curl "${CURL_OPTS[@]}" "http://$LEARNER_IP:$LEARNER_API_PORT$1" 2>/dev/null; }
learner_up() { learner_api /api/v1/health | grep -q healthy; }
wait_for 30 "test learner manager comes up" learner_up

echo "[3/5] apply per-DC bandwidth shaping (40 Mbps on dc-a uplink)"
ansible-playbook -i "$INV3DC" "$ANSIBLE_DIR/playbooks/apply-shaping.yml" >/dev/null 2>&1 \
    && SHAPED=1 || skip "shaping apply failed"

echo "[4/5] add-learner -> snapshot install must complete under the shape"
r=$(post_leader_raft "/api/v1/raft/add-learner" \
    "{\"hostname\":\"$LEARNER_NAME\",\"addr\":\"$LEARNER_IP:$LEARNER_GRPC_PORT\",\"shared_secret\":\"$SECRET\"}")
assert_match '"status":"ok"' "$r" "add-learner response"
LEARNER_NID=$(echo "$r" | python3 -c 'import json,sys; print(json.load(sys.stdin)["node_id"])')

# Leader's applied index is the target the learner must reach. Catching up to
# it from an empty state requires installing the snapshot (the snapshot is what
# can't move in 5s over the shape), so this index is direct evidence the
# snapshot RPC completed.
leader_applied() {
    mgr_get_any /api/v1/raft/state \
        | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("last_applied_index") or 0)'
}
TARGET=$(leader_applied)
[ -n "$TARGET" ] && [ "$TARGET" -gt 0 ] || fail "could not read leader last_applied_index"

learner_caught_up() {
    local idx
    idx=$(learner_api /api/v1/raft/state \
        | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("last_applied_index") or 0)' 2>/dev/null)
    [ -n "$idx" ] && [ "$idx" -ge "$TARGET" ]
}
# 180s budget: a >25 MB snapshot at 40 Mbps (~5 MB/s shaped, less under
# contention) needs well over the old 5s default; the size-scaled per-request
# timeout (CO-3-6) lets it land. Pre-fix this never converges.
wait_for 180 "learner installs snapshot + catches up under shaping" learner_caught_up

echo "[5/5] Raft stayed stable under shaping (ports exempt)"
quorum_ok || fail "lost quorum under shaping (Raft/API ports not exempt?)"

echo "STATUS: PASS"

#!/usr/bin/env bash
# LIS-20 / Sec R3: empty shared_secret must not silently admit nodes.
#
# 1) Live manager.toml files must have a non-empty shared_secret, or an
#    explicit allow_insecure_empty_secret=true (lab escape hatch).
# 2) Spawning dtfs-manager with an empty secret and no escape hatch must
#    exit non-zero immediately (config validate fail-closed).
. "$(dirname "$0")/../lib/common.sh"

TMP_CFG="/tmp/lis20-empty-secret.toml"
TMP_LOG="/tmp/lis20-empty-secret.log"

case_cleanup() {
    if [ -n "${PROBE_HOST:-}" ]; then
        ssh_run "$PROBE_HOST" "sudo rm -f $TMP_CFG $TMP_LOG" || true
    fi
}
trap case_cleanup EXIT

# ── 1. Live cluster must not be fail-open ─────────────────────────
for h in $(mgrs_list); do
    st=$(ssh_run "$h" "python3 - <<'EOF'
import tomllib
cfg = tomllib.load(open('/etc/dtfs/manager.toml','rb'))
c = cfg.get('cluster', {})
secret = c.get('shared_secret', '')
allow = bool(c.get('allow_insecure_empty_secret', False))
if secret:
    print('ok')
elif allow:
    print('insecure-lab')
else:
    print('fail-open')
EOF
") || fail "could not read manager.toml on $h"
    case "$st" in
        ok|insecure-lab) ;;
        *) fail "manager $h: empty shared_secret without allow_insecure_empty_secret (got '$st')" ;;
    esac
done

# ── 2. Binary refuse: empty secret, escape hatch off ──────────────
PROBE_HOST=$(mgrs_list | awk '{print $1}')
[ -n "$PROBE_HOST" ] || skip "no manager hosts"
ssh_run "$PROBE_HOST" "test -x /usr/local/bin/dtfs-manager" \
    || skip "dtfs-manager binary not installed on $PROBE_HOST"

ssh_run "$PROBE_HOST" "python3 - <<'EOF'
toml = '''[node]
hostname = \"lis20-probe\"
data_dir = \"/tmp/lis20-probe-data\"

[location]
region = \"eu\"
datacenter = \"dc-test\"
rack = \"r1\"
host = \"lis20-probe\"

[cluster]
name = \"dtfs\"
shared_secret = \"\"
bootstrap = false

[tls]
mode = \"auto\"

[defaults]
[tiering]
[monitoring]

[raft]
listen_addr = \"127.0.0.1:19410\"

[grpc]
listen_addr = \"127.0.0.1:19411\"

[api]
listen_addr = \"127.0.0.1:19412\"
'''
open('/tmp/lis20-empty-secret.toml','w').write(toml)
EOF
" || fail "could not write probe config"

# Expect immediate non-zero exit from validate_shared_secret (before raft).
out=$(ssh_run "$PROBE_HOST" \
    "sudo timeout 8 /usr/local/bin/dtfs-manager --config $TMP_CFG >$TMP_LOG 2>&1; echo EXIT:\$?" \
    || true)
assert_match 'EXIT:[1-9]' "$out" "manager must exit non-zero on empty secret"
log=$(ssh_run "$PROBE_HOST" "sudo cat $TMP_LOG 2>/dev/null" || true)
assert_match 'shared_secret is empty|allow_insecure_empty_secret' "$log" \
    "refuse message must mention empty secret or escape hatch"

pass

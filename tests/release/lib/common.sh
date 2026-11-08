# tests/release/lib/common.sh — shared helpers for the E2E suite.
# Source this from every cases/NN-*.sh.

set -uo pipefail

# ─── Environment + cluster topology ──────────────────────────────
MGRS=${MGRS:-mini-mgr01,mini-mgr02,mini-mgr03}
DATA=${DATA:-mini-data01,mini-data02}
CLIENT=${CLIENT:-mini-client01}
MOUNT=${MOUNT:-/mnt/dtfs}
VOL=${VOL:-default}
MGR_PORT=${MGR_PORT:-9402}
SSH_KEY=${SSH_KEY:-$HOME/.ssh/dtfs-lab/id_ed25519}
SSH_OPTS=(-o StrictHostKeyChecking=no -i "$SSH_KEY" -o ConnectTimeout=5 -o BatchMode=yes)
CURL_OPTS=(-s --max-time 8)

mgrs_list() { echo "$MGRS" | tr ',' ' '; }
data_list() { echo "$DATA" | tr ',' ' '; }

# ─── Result protocol ─────────────────────────────────────────────
# Last line of stdout MUST be STATUS: <kind> [<bug>].
pass()         { echo "STATUS: PASS"; exit 0; }
fail()         { echo "FAIL: $*" >&2; echo "STATUS: FAIL"; exit 1; }
skip()         { echo "SKIP: $*"; echo "STATUS: SKIP"; exit 0; }
known_issue()  { echo "KNOWN_ISSUE: $* (tracked by $2 ${1:-})"; echo "STATUS: KNOWN_ISSUE [$1]"; exit 0; }

# ─── Manager HTTP helpers ────────────────────────────────────────
mgr_get_any() {
    local path="$1"
    for h in $(mgrs_list); do
        local r=$(curl "${CURL_OPTS[@]}" "http://$h:$MGR_PORT$path" 2>/dev/null)
        if [ -n "$r" ]; then echo "$r"; return 0; fi
    done
    return 1
}

mgr_post_leader() {
    local path="$1" body="$2"
    # LIS-18 — mutating REST requires cluster shared_secret when configured.
    local secret="${DTFS_SHARED_SECRET:-}"
    if [ -z "$secret" ]; then
        secret=$(mgr_shared_secret 2>/dev/null || true)
    fi
    local -a auth_hdr=()
    if [ -n "$secret" ]; then
        auth_hdr=(-H "X-Dtfs-Shared-Secret: $secret")
    fi
    for h in $(mgrs_list); do
        local r=$(curl "${CURL_OPTS[@]}" -X POST -H 'content-type: application/json' \
                  "${auth_hdr[@]}" \
                  -d "$body" "http://$h:$MGR_PORT$path" 2>/dev/null)
        # Skip a non-leader: openraft surfaces the forward hint either bare
        # or wrapped ("raft write failed: has to forward request to: ..."),
        # so match the substring anywhere rather than right after "error".
        if echo "$r" | grep -q 'has to forward request'; then
            continue
        fi
        echo "$r"
        return 0
    done
    return 1
}

# Read `[cluster].shared_secret` from the first manager's on-disk config
# (cached for the case lifetime). Empty string when unset / unreadable.
mgr_shared_secret() {
    if [ -n "${_MGR_SHARED_SECRET_CACHED+x}" ]; then
        echo "$_MGR_SHARED_SECRET_CACHED"
        return 0
    fi
    local h
    h=$(mgrs_list | awk '{print $1}')
    _MGR_SHARED_SECRET_CACHED=$(ssh_run "$h" "python3 - <<'EOF'
import tomllib
try:
    with open('/etc/dtfs/manager.toml','rb') as f:
        print(tomllib.load(f).get('cluster',{}).get('shared_secret',''))
except Exception:
    print('')
EOF
" 2>/dev/null | tr -d '\r' || true)
    echo "$_MGR_SHARED_SECRET_CACHED"
}

# LIS-19 / LIS-55 / LIS-83 � build the gRPC metadata a data-plane RPC needs
# when `cluster.shared_secret` is set, as grpcurl `-H` arguments in the global
# array DATA_RPC_HDRS. The canonical MAC message is
#   "dtfs-data-rpc\nv1\n{method}\n{ts}\n{binding}\n"
# hashed HMAC-SHA256 with the cluster secret (dtfs_common::crypto).
# `binding` is empty for PutChunk/ListChunks, the chunk_id for
# DeleteChunk/GetChunk/VerifyChunk, and "chunk_id|target" for ReplicateChunk.
#
# On a cluster with no secret the array is left EMPTY on purpose: the data node
# skips verification entirely there, so an unstamped call is the correct call.
# The stamp stays valid for DATA_RPC_MAX_SKEW_SECS (300s), so one call covers a
# whole page walk.
#   data_rpc_stamp ListChunks ""
#   grpcurl "${DATA_RPC_HDRS[@]}" ...
data_rpc_stamp() {
    local method="$1" binding="${2:-}"
    DATA_RPC_HDRS=()
    local secret="${DTFS_SHARED_SECRET:-}"
    if [ -z "$secret" ]; then
        secret=$(mgr_shared_secret 2>/dev/null || true)
    fi
    [ -n "$secret" ] || return 0
    local out
    out=$(python3 - "$secret" "$method" "$binding" <<'PYSTAMP'
import hashlib, hmac, sys, time
secret, method, binding = sys.argv[1], sys.argv[2], sys.argv[3]
ts = int(time.time())
msg = "dtfs-data-rpc\nv1\n{}\n{}\n{}\n".format(method, ts, binding).encode()
print(ts)
print(hmac.new(secret.encode(), msg, hashlib.sha256).hexdigest())
PYSTAMP
) || return 1
    local ts mac
    ts=$(printf '%s\n' "$out" | sed -n 1p)
    mac=$(printf '%s\n' "$out" | sed -n 2p)
    [ -n "$ts" ] && [ -n "$mac" ] || return 1
    DATA_RPC_HDRS=(-H "x-dtfs-rpc-ts: $ts" -H "x-dtfs-rpc-mac: $mac")
}

# LIS-56 — build the gRPC metadata a mutating ManagerService RPC needs when
# `cluster.shared_secret` is set, as grpcurl `-H` arguments in the global
# array MGR_RPC_HDRS. Same shape as data_rpc_stamp but over the
# `dtfs-mgr-rpc` MAC plane (dtfs_common::crypto::stamp_mgr_rpc_auth /
# require_mgr_rpc_auth). `binding` is the RPC's own binding value (e.g. the
# file path for CreateFile). Empty secret -> empty array (manager skips
# verification, so an unstamped call is correct there too).
#   mgr_rpc_stamp CreateFile "/release/109-foo.bin"
#   grpcurl "${MGR_RPC_HDRS[@]}" ...
mgr_rpc_stamp() {
    local method="$1" binding="${2:-}"
    MGR_RPC_HDRS=()
    local secret="${DTFS_SHARED_SECRET:-}"
    if [ -z "$secret" ]; then
        secret=$(mgr_shared_secret 2>/dev/null || true)
    fi
    [ -n "$secret" ] || return 0
    local out
    out=$(python3 - "$secret" "$method" "$binding" <<'PYSTAMP'
import hashlib, hmac, sys, time
secret, method, binding = sys.argv[1], sys.argv[2], sys.argv[3]
ts = int(time.time())
msg = "dtfs-mgr-rpc\nv1\n{}\n{}\n{}\n".format(method, ts, binding).encode()
print(ts)
print(hmac.new(secret.encode(), msg, hashlib.sha256).hexdigest())
PYSTAMP
) || return 1
    local ts mac
    ts=$(printf '%s\n' "$out" | sed -n 1p)
    mac=$(printf '%s\n' "$out" | sed -n 2p)
    [ -n "$ts" ] && [ -n "$mac" ] || return 1
    MGR_RPC_HDRS=(-H "x-dtfs-rpc-ts: $ts" -H "x-dtfs-rpc-mac: $mac")
}

# ─── Remote-host helpers ─────────────────────────────────────────
ssh_run()    { ssh "${SSH_OPTS[@]}" "debian@$1" "$2"; }
client_run() { ssh_run "$CLIENT" "$1"; }

# ─── Assertion helpers ───────────────────────────────────────────
assert_eq() {
    local want="$1" got="$2" what="${3:-value}"
    if [ "$want" != "$got" ]; then
        fail "$what: expected '$want', got '$got'"
    fi
}
assert_match() {
    local pattern="$1" text="$2" what="${3:-output}"
    if ! echo "$text" | grep -qE "$pattern"; then
        fail "$what: pattern '$pattern' not found in: $text"
    fi
}
assert_ge() {
    local got="$1" want="$2" what="${3:-value}"
    if [ "$got" -lt "$want" ]; then
        fail "$what: $got < $want"
    fi
}

# Poll a check fn (returns 0 on success) up to N seconds.
wait_for() {
    local secs="$1"; shift
    local desc="$1"; shift
    local start=$(date +%s)
    while [ $(( $(date +%s) - start )) -lt "$secs" ]; do
        if "$@"; then return 0; fi
        sleep 2
    done
    fail "wait_for: $desc did not become true in ${secs}s"
}

# Like wait_for but returns 1 on timeout instead of failing the case.
# Use when a not-yet-true condition should drive a SKIP, not a FAIL.
#   wait_for_soft <secs> <cmd...>
wait_for_soft() {
    local secs="$1"; shift
    local start=$(date +%s)
    while [ $(( $(date +%s) - start )) -lt "$secs" ]; do
        if "$@"; then return 0; fi
        sleep 2
    done
    return 1
}

# ─── Domain helpers ──────────────────────────────────────────────

# Return the chunk_ids list for a given file path (newline-separated).
# Uses the BE-helper /api/v1/files/inspect endpoint which exposes the
# full chunk metadata for one path (the bulk list_files omits chunks).
chunk_ids_for_path() {
    local path="$1"
    mgr_get_any "/api/v1/files/inspect?path=$path" | python3 -c "
import json,sys
d=json.load(sys.stdin)
if 'error' in d:
    sys.exit(0)
for c in d.get('chunks',[]):
    print(c.get('chunk_id',''))
"
}

# Count chunk .dat files on a host (across all tier dirs).
chunks_on_host() {
    local host="$1"
    ssh_run "$host" 'sudo find /mnt -name "*.dat" 2>/dev/null | wc -l'
}

# Does host hold a given chunk_id?
host_has_chunk() {
    local host="$1" cid="$2"
    local found=$(ssh_run "$host" "sudo find /mnt -name '$cid.dat' 2>/dev/null | head -1")
    [ -n "$found" ]
}

# sha256 of a file via FUSE client.
sha256_via_fuse() {
    client_run "sudo sha256sum '$1' | awk '{print \$1}'"
}

# ─── Cleanup-on-error ────────────────────────────────────────────
# Cases SHOULD register cleanup via trap "case_cleanup" EXIT and define
# a case_cleanup fn. Suite continues to the next case even if this one
# fails; cleanup runs regardless.

# ─── Chaos / lifecycle env (consumed by lib/pve.sh + lib/lifecycle.sh) ──
# ALLOW_PVE_CHAOS=1   opt-in for hypervisor-destructive cases (else they SKIP)
# PVE_TOKEN=...       Proxmox API token (root@pam!dtfs-test=...); never commit
# INVENTORY_JSON=...  topology map (required by dc.sh)
# KEEP_CLUSTER=1      after a --provision run, leave the VMs up for debugging

# ─── BE-1 — pull in fixture + health + DC/PVE helpers ────────────
# Sourced here so every case gets all libs from one
# `source .../lib/common.sh` line. dc.sh is pure SSH/API (safe always);
# pve.sh only defines functions (inert until a case calls require_pve).
_LIBDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$_LIBDIR/fixtures.sh"
. "$_LIBDIR/health.sh"
. "$_LIBDIR/dc.sh"
. "$_LIBDIR/pve.sh"

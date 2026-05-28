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
    for h in $(mgrs_list); do
        local r=$(curl "${CURL_OPTS[@]}" -X POST -H 'content-type: application/json' \
                  -d "$body" "http://$h:$MGR_PORT$path" 2>/dev/null)
        if echo "$r" | grep -q '"error"[^"]*has to forward request'; then
            continue
        fi
        echo "$r"
        return 0
    done
    return 1
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

# ─── BE-1 — pull in fixture + health helpers ─────────────────────
# Sourced here so every case gets all three libs from one
# `source .../lib/common.sh` line.
_LIBDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$_LIBDIR/fixtures.sh"
. "$_LIBDIR/health.sh"

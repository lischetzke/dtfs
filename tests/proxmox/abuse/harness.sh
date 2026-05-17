#!/usr/bin/env bash
# Common harness for the DTFS abuse-test suite.
# Source this file at the top of each test script.

set -u

# ─── lab topology ────────────────────────────────────────────────────────────
MGR_API="${MGR_API:-http://10.150.0.151:9402}"
MGRS=(mgr-01 mgr-02 mgr-03)
DATA_NODES=(data-01 data-02 data-03 data-04)
CLIENTS=(client-01 client-02)
ALL_HOSTS=("${MGRS[@]}" "${DATA_NODES[@]}" "${CLIENTS[@]}")
MOUNT=/mnt/dtfs

SSH_KEY="${SSH_KEY:-$HOME/.ssh/dtfs-lab/id_ed25519}"
SSH_KH="${SSH_KH:-$HOME/.ssh/dtfs-lab/known_hosts}"
SSH_OPTS=(-i "$SSH_KEY" -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile="$SSH_KH")

# ─── output ─────────────────────────────────────────────────────────────────
COLOR_RESET=$'\e[0m'
COLOR_PASS=$'\e[32m'
COLOR_FAIL=$'\e[31m'
COLOR_WARN=$'\e[33m'
COLOR_INFO=$'\e[36m'

RESULTS_FILE="${RESULTS_FILE:-/tmp/dtfs-abuse-results.tsv}"
[ -f "$RESULTS_FILE" ] || printf "category\ttest\tresult\tduration_s\tdetail\n" > "$RESULTS_FILE"

t_section() { printf "\n%s═══ %s ═══%s\n" "$COLOR_INFO" "$1" "$COLOR_RESET"; }
t_info()    { printf "    %s\n" "$1"; }
t_pass()    { printf "  %sPASS%s %s (%ss)\n" "$COLOR_PASS" "$COLOR_RESET" "$1" "$2"; printf "%s\t%s\tPASS\t%s\t%s\n" "$CATEGORY" "$1" "$2" "${3:-}" >> "$RESULTS_FILE"; }
t_fail()    { printf "  %sFAIL%s %s (%ss): %s\n" "$COLOR_FAIL" "$COLOR_RESET" "$1" "$2" "$3"; printf "%s\t%s\tFAIL\t%s\t%s\n" "$CATEGORY" "$1" "$2" "$3" >> "$RESULTS_FILE"; }
t_warn()    { printf "  %sWARN%s %s: %s\n" "$COLOR_WARN" "$COLOR_RESET" "$1" "$2"; printf "%s\t%s\tWARN\t-\t%s\n" "$CATEGORY" "$1" "$2" >> "$RESULTS_FILE"; }
t_skip()    { printf "  SKIP %s: %s\n" "$1" "$2"; printf "%s\t%s\tSKIP\t-\t%s\n" "$CATEGORY" "$1" "$2" >> "$RESULTS_FILE"; }

# ─── SSH helpers ────────────────────────────────────────────────────────────
# Local SSH ServerAliveInterval keeps connection from dangling forever if peer wedges (see F-20).
sssh() { ssh -o ServerAliveInterval=30 -o ServerAliveCountMax=4 "${SSH_OPTS[@]}" "$1" "${@:2}"; }
sssh_root() { ssh -o ServerAliveInterval=30 -o ServerAliveCountMax=4 "${SSH_OPTS[@]}" "$1" "sudo bash -lc '${2}'"; }
sssh_script() {
    # $1: target host, $2: local script body (heredoc body), runs as root on target
    local host="$1"; shift
    local script
    script=$(cat)
    ssh "${SSH_OPTS[@]}" "$host" "sudo bash" <<<"$script"
}

# ─── API helpers ─────────────────────────────────────────────────────────────
api_get()  { curl -sS --max-time 10 "${MGR_API}$1"; }
api_post() { curl -sS --max-time 10 -X POST -H 'Content-Type: application/json' -d "$2" "${MGR_API}$1"; }

cluster_files()       { api_get /api/v1/files; }
cluster_nodes()       { api_get /api/v1/nodes; }
cluster_status()      { api_get /api/v1/cluster/status; }
healthy_node_count()  { cluster_nodes | python3 -c "import sys,json;print(sum(1 for n in json.load(sys.stdin) if n.get('health')=='Healthy'))"; }
file_chunk_count()    { api_get "/api/v1/files" | python3 -c "import sys,json;d=json.load(sys.stdin);p=sys.argv[1];e=[f for f in d['files'] if f['path']==p];print(e[0]['chunk_count'] if e else 0)" "$1"; }
file_total_size()     { api_get "/api/v1/files" | python3 -c "import sys,json;d=json.load(sys.stdin);p=sys.argv[1];e=[f for f in d['files'] if f['path']==p];print(e[0]['total_size'] if e else -1)" "$1"; }
file_exists()         { [ "$(file_chunk_count "$1")" -gt 0 ] 2>/dev/null; }
chunk_count_on_disk() {
    # Returns total .dat chunk files across all data nodes
    local total=0
    for h in "${DATA_NODES[@]}"; do
        local n
        n=$(sssh "$h" "sudo find /mnt/disk-a /mnt/disk-b -type f -name '*.dat' 2>/dev/null | wc -l" 2>/dev/null || echo 0)
        total=$((total + n))
    done
    echo "$total"
}

# ─── client-side test helpers (run on client-01) ────────────────────────────
client_exec()  { sssh "$1" "sudo bash -c '${2}'"; }
fuse_write()   { sssh "$1" "sudo bash -c 'cp $2 ${MOUNT}/$3'"; }
fuse_read()    { sssh "$1" "sudo bash -c 'md5sum ${MOUNT}/$2 | cut -d\" \" -f1'"; }
fuse_ls()      { sssh "$1" "sudo ls -1 ${MOUNT}/"; }
fuse_unlink()  { sssh "$1" "sudo rm -f ${MOUNT}/$2"; }
fuse_remount() { sssh "$1" "sudo fusermount3 -uz ${MOUNT} 2>/dev/null; sudo systemctl reset-failed dtfs-fuse@mnt-dtfs.service 2>/dev/null; sudo systemctl restart dtfs-fuse@mnt-dtfs.service; sleep 4; sudo mountpoint -q ${MOUNT}"; }

# ─── timing ──────────────────────────────────────────────────────────────────
t_start() { date +%s.%N; }
# Use awk (always present) instead of bc which may be absent.
t_elapsed() { awk -v end="$(date +%s.%N)" -v start="$1" 'BEGIN{printf "%.2f", end - start}'; }

# ─── waits with timeout ──────────────────────────────────────────────────────
wait_until() {
    # wait_until <timeout_secs> <shell-condition-command>
    local deadline=$(( $(date +%s) + $1 ))
    shift
    while [ "$(date +%s)" -lt "$deadline" ]; do
        if eval "$*"; then return 0; fi
        sleep 1
    done
    return 1
}

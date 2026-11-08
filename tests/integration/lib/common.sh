#!/usr/bin/env bash
# DTFS Integration Test Common Library
# Source this file at the top of every test script:
#   source "$(dirname "$0")/lib/common.sh"

# ---------------------------------------------------------------------------
# Environment configuration
# ---------------------------------------------------------------------------

MANAGER_API="${DTFS_MANAGER_API:-http://10.0.1.1:8080}"
MANAGER_FALLBACK_APIS=(
    "http://10.0.1.1:8080"
    "http://10.0.1.2:8080"
    "http://10.0.1.3:8080"
)

TEST_MOUNT="${DTFS_TEST_MOUNT:-/mnt/dtfs}"
LOG_DIR="${DTFS_LOG_DIR:-/tmp/dtfs-test-logs}"
RESULTS_DIR="${DTFS_RESULTS_DIR:-/tmp/dtfs-test-results}"
SSH_KEY="${DTFS_SSH_KEY:-$HOME/.ssh/dtfs_test_key}"
SSH_USER="${DTFS_SSH_USER:-dtfs-admin}"
DTFS_CLI="${DTFS_CLI_BIN:-/usr/local/bin/dtfs-cli}"

# Node addresses
MGR_01="${DTFS_MGR_01:-10.0.1.1}"
MGR_02="${DTFS_MGR_02:-10.0.1.2}"
MGR_03="${DTFS_MGR_03:-10.0.1.3}"
DATA_01="${DTFS_DATA_01:-10.0.2.1}"
DATA_02="${DTFS_DATA_02:-10.0.2.2}"
CLIENT_01="${DTFS_CLIENT_01:-10.0.3.1}"

ALL_MANAGERS=("$MGR_01" "$MGR_02" "$MGR_03")
ALL_DATA_NODES=("$MGR_01" "$MGR_02" "$DATA_01" "$DATA_02")
ALL_NODES=("$MGR_01" "$MGR_02" "$MGR_03" "$DATA_01" "$DATA_02" "$CLIENT_01")

# API health check timeout (seconds)
HEALTH_WAIT_TIMEOUT="${DTFS_HEALTH_TIMEOUT:-120}"
# API call timeout
API_TIMEOUT="${DTFS_API_TIMEOUT:-10}"

# ---------------------------------------------------------------------------
# Color codes
# ---------------------------------------------------------------------------
if [ -t 1 ] && [ "${NO_COLOR:-}" = "" ]; then
    COLOR_RESET="\033[0m"
    COLOR_GREEN="\033[0;32m"
    COLOR_RED="\033[0;31m"
    COLOR_YELLOW="\033[0;33m"
    COLOR_BLUE="\033[0;34m"
    COLOR_CYAN="\033[0;36m"
    COLOR_BOLD="\033[1m"
else
    COLOR_RESET=""
    COLOR_GREEN=""
    COLOR_RED=""
    COLOR_YELLOW=""
    COLOR_BLUE=""
    COLOR_CYAN=""
    COLOR_BOLD=""
fi

# ---------------------------------------------------------------------------
# Pass/fail counters
# ---------------------------------------------------------------------------
PASS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0
TEST_SUITE_NAME="${TEST_SUITE_NAME:-unknown}"

# ---------------------------------------------------------------------------
# Logging functions
# ---------------------------------------------------------------------------

_ts() { date '+%Y-%m-%dT%H:%M:%S'; }

log_info() {
    echo -e "${COLOR_BLUE}[INFO]${COLOR_RESET} $(_ts) $*" >&2
}

log_ok() {
    echo -e "${COLOR_GREEN}[PASS]${COLOR_RESET} $(_ts) $*" >&2
    PASS_COUNT=$((PASS_COUNT + 1))
}

log_fail() {
    echo -e "${COLOR_RED}[FAIL]${COLOR_RESET} $(_ts) $*" >&2
    FAIL_COUNT=$((FAIL_COUNT + 1))
}

log_warn() {
    echo -e "${COLOR_YELLOW}[WARN]${COLOR_RESET} $(_ts) $*" >&2
}

log_skip() {
    echo -e "${COLOR_CYAN}[SKIP]${COLOR_RESET} $(_ts) $*" >&2
    SKIP_COUNT=$((SKIP_COUNT + 1))
}

log_section() {
    echo -e "" >&2
    echo -e "${COLOR_BOLD}${COLOR_BLUE}=== $* ===${COLOR_RESET}" >&2
}

log_debug() {
    if [ "${DTFS_DEBUG:-0}" = "1" ]; then
        echo -e "[DEBUG] $(_ts) $*" >&2
    fi
}

# Print test summary and exit with appropriate code
print_summary() {
    local suite="${1:-$TEST_SUITE_NAME}"
    local total=$((PASS_COUNT + FAIL_COUNT + SKIP_COUNT))
    echo -e "" >&2
    echo -e "${COLOR_BOLD}=== Test Suite: $suite ===${COLOR_RESET}" >&2
    echo -e "  Total:  $total" >&2
    echo -e "  ${COLOR_GREEN}Passed: $PASS_COUNT${COLOR_RESET}" >&2
    echo -e "  ${COLOR_RED}Failed: $FAIL_COUNT${COLOR_RESET}" >&2
    echo -e "  ${COLOR_CYAN}Skipped: $SKIP_COUNT${COLOR_RESET}" >&2
    echo -e "" >&2
    if [ "$FAIL_COUNT" -gt 0 ]; then
        echo -e "${COLOR_RED}RESULT: FAILED${COLOR_RESET}" >&2
        return 1
    else
        echo -e "${COLOR_GREEN}RESULT: PASSED${COLOR_RESET}" >&2
        return 0
    fi
}

# ---------------------------------------------------------------------------
# SSH execution
# ---------------------------------------------------------------------------

# ssh_exec <host> <command>
# Run a shell command on a remote host via SSH. Returns stdout.
ssh_exec() {
    local host="$1"
    shift
    local cmd="$*"
    ssh -i "$SSH_KEY" \
        -o StrictHostKeyChecking=no \
        -o UserKnownHostsFile=/dev/null \
        -o ConnectTimeout=10 \
        -o BatchMode=yes \
        "${SSH_USER}@${host}" \
        "sudo bash -c $(printf '%q' "$cmd")"
}

# ssh_exec_bg <host> <command>
# Run command in background on remote host (fire and forget)
ssh_exec_bg() {
    local host="$1"
    shift
    local cmd="$*"
    ssh -i "$SSH_KEY" \
        -o StrictHostKeyChecking=no \
        -o UserKnownHostsFile=/dev/null \
        -o ConnectTimeout=10 \
        -o BatchMode=yes \
        -f \
        "${SSH_USER}@${host}" \
        "sudo bash -c $(printf '%q' "nohup $cmd &>/dev/null &")"
}

# ssh_exec_no_sudo <host> <command>
# Run without sudo (for read-only checks as dtfs-admin)
ssh_exec_no_sudo() {
    local host="$1"
    shift
    local cmd="$*"
    ssh -i "$SSH_KEY" \
        -o StrictHostKeyChecking=no \
        -o UserKnownHostsFile=/dev/null \
        -o ConnectTimeout=10 \
        -o BatchMode=yes \
        "${SSH_USER}@${host}" \
        "bash -c $(printf '%q' "$cmd")"
}

# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

# api_get <path> [timeout]
# Perform GET against MANAGER_API. Returns JSON body.
api_get() {
    local path="$1"
    local timeout="${2:-$API_TIMEOUT}"
    curl -sf --max-time "$timeout" "${MANAGER_API}${path}"
}

# api_put <path> <json_body> [timeout]
api_put() {
    local path="$1"
    local body="$2"
    local timeout="${3:-$API_TIMEOUT}"
    curl -sf --max-time "$timeout" \
        -X PUT \
        -H 'Content-Type: application/json' \
        -d "$body" \
        "${MANAGER_API}${path}"
}

# api_post <path> <json_body> [timeout]
api_post() {
    local path="$1"
    local body="$2"
    local timeout="${3:-$API_TIMEOUT}"
    curl -sf --max-time "$timeout" \
        -X POST \
        -H 'Content-Type: application/json' \
        -d "$body" \
        "${MANAGER_API}${path}"
}

# api_delete <path> [timeout]
api_delete() {
    local path="$1"
    local timeout="${2:-$API_TIMEOUT}"
    curl -sf --max-time "$timeout" -X DELETE "${MANAGER_API}${path}"
}

# ---------------------------------------------------------------------------
# Cluster wait helpers
# ---------------------------------------------------------------------------

# wait_for_api <url> <timeout_seconds>
# Poll the URL (GET) until it responds with HTTP 200 or timeout.
wait_for_api() {
    local url="$1"
    local timeout="${2:-60}"
    local start
    start=$(date +%s)
    local deadline=$((start + timeout))

    log_info "Waiting for API at $url (timeout: ${timeout}s)"
    while true; do
        local now
        now=$(date +%s)
        if [ "$now" -ge "$deadline" ]; then
            log_fail "Timed out waiting for API at $url after ${timeout}s"
            return 1
        fi
        if curl -sf --max-time 3 "$url" >/dev/null 2>&1; then
            log_debug "API at $url is reachable (took $((now - start))s)"
            return 0
        fi
        sleep 2
    done
}

# wait_for_health <timeout_seconds>
# Wait until cluster reports overall healthy status.
wait_for_health() {
    local timeout="${1:-$HEALTH_WAIT_TIMEOUT}"
    local start
    start=$(date +%s)
    local deadline=$((start + timeout))

    log_info "Waiting for cluster health (timeout: ${timeout}s)"
    while true; do
        local now
        now=$(date +%s)
        if [ "$now" -ge "$deadline" ]; then
            log_fail "Timed out waiting for cluster health after ${timeout}s"
            return 1
        fi

        local health
        health=$(curl -sf --max-time 5 "${MANAGER_API}/api/v1/health" 2>/dev/null || echo '{}')
        local status
        status=$(echo "$health" | jq -r '.status // "unknown"')

        if [ "$status" = "healthy" ]; then
            log_info "Cluster is healthy (took $((now - start))s)"
            return 0
        fi
        log_debug "Cluster status: $status (elapsed: $((now - start))s)"
        sleep 3
    done
}

# wait_for_node_health <node_api_url> <timeout_seconds>
# Wait until a specific manager node API responds healthy.
wait_for_node_health() {
    local node_url="$1"
    local timeout="${2:-60}"
    wait_for_api "${node_url}/api/v1/health" "$timeout"
}

# wait_for_leader <timeout_seconds>
# Wait until a leader is elected. Returns leader node_id.
wait_for_leader() {
    local timeout="${1:-60}"
    local start
    start=$(date +%s)
    local deadline=$((start + timeout))

    while true; do
        local now
        now=$(date +%s)
        if [ "$now" -ge "$deadline" ]; then
            log_fail "Timed out waiting for leader election after ${timeout}s"
            return 1
        fi

        local leader
        leader=$(curl -sf --max-time 5 "${MANAGER_API}/api/v1/cluster/status" 2>/dev/null \
            | jq -r '.raft.leader_id // empty')

        if [ -n "$leader" ] && [ "$leader" != "null" ]; then
            echo "$leader"
            return 0
        fi
        sleep 2
    done
}

# get_leader_node_id
# Returns the current leader node_id or empty string.
get_leader_node_id() {
    curl -sf --max-time 5 "${MANAGER_API}/api/v1/cluster/status" 2>/dev/null \
        | jq -r '.raft.leader_id // empty'
}

# get_cluster_status
# Returns full cluster status JSON.
get_cluster_status() {
    curl -sf --max-time 10 "${MANAGER_API}/api/v1/cluster/status" 2>/dev/null
}

# ---------------------------------------------------------------------------
# Mount helpers
# ---------------------------------------------------------------------------

# mount_dtfs <host> <mountpoint>
# Mount DTFS FUSE on a remote host.
mount_dtfs() {
    local host="$1"
    local mountpoint="${2:-$TEST_MOUNT}"

    log_info "Mounting DTFS at ${host}:${mountpoint}"

    # Start the systemd service (which mounts)
    ssh_exec "$host" "systemctl start 'dtfs-fuse@default.service'"

    # Wait for mount
    local attempts=0
    while [ $attempts -lt 15 ]; do
        if ssh_exec "$host" "mountpoint -q '$mountpoint'" 2>/dev/null; then
            log_info "DTFS mounted at ${host}:${mountpoint}"
            return 0
        fi
        sleep 2
        attempts=$((attempts + 1))
    done

    log_fail "DTFS failed to mount at ${host}:${mountpoint} after 30s"
    return 1
}

# umount_dtfs <host> <mountpoint>
# Unmount DTFS FUSE on a remote host.
umount_dtfs() {
    local host="$1"
    local mountpoint="${2:-$TEST_MOUNT}"

    log_info "Unmounting DTFS at ${host}:${mountpoint}"
    ssh_exec "$host" "systemctl stop 'dtfs-fuse@default.service' || true"
    ssh_exec "$host" "fusermount3 -uz '$mountpoint' 2>/dev/null || true"

    # Verify unmounted
    if ssh_exec "$host" "mountpoint -q '$mountpoint'" 2>/dev/null; then
        log_warn "DTFS still appears mounted at ${host}:${mountpoint} after stop"
        # Lazy unmount as fallback
        ssh_exec "$host" "umount -l '$mountpoint' 2>/dev/null || true"
    fi
}

# ---------------------------------------------------------------------------
# File I/O helpers
# ---------------------------------------------------------------------------

# write_test_file <host> <path> <size_mb>
# Write a test file using dd + /dev/urandom. Prints checksum to stdout.
write_test_file() {
    local host="$1"
    local path="$2"
    local size_mb="$3"

    log_info "Writing ${size_mb}MB test file to ${host}:${path}"
    local cksum
    cksum=$(ssh_exec "$host" \
        "dd if=/dev/urandom bs=1M count=${size_mb} 2>/dev/null | tee '${path}' | md5sum | awk '{print \$1}'")
    echo "$cksum"
}

# read_verify_file <host> <path> <expected_checksum>
# Read back a file and verify its MD5 checksum.
read_verify_file() {
    local host="$1"
    local path="$2"
    local expected_checksum="$3"

    log_info "Verifying checksum of ${host}:${path}"
    local actual
    actual=$(ssh_exec "$host" "md5sum '${path}' | awk '{print \$1}'")

    if [ "$actual" = "$expected_checksum" ]; then
        return 0
    else
        log_debug "Expected checksum: $expected_checksum"
        log_debug "Actual checksum:   $actual"
        return 1
    fi
}

# write_fixed_test_file <host> <path> <size_mb> <seed>
# Write a deterministic test file using a fixed seed. Prints expected checksum.
write_fixed_test_file() {
    local host="$1"
    local path="$2"
    local size_mb="$3"
    local seed="${4:-42}"

    log_info "Writing ${size_mb}MB fixed test file to ${host}:${path} (seed=$seed)"
    # Use openssl for deterministic output based on seed
    local cksum
    cksum=$(ssh_exec "$host" \
        "openssl enc -aes-256-ctr -pass pass:seed${seed} -nosalt \
         </dev/zero 2>/dev/null | dd bs=1M count=${size_mb} 2>/dev/null \
         | tee '${path}' | md5sum | awk '{print \$1}'")
    echo "$cksum"
}

# ---------------------------------------------------------------------------
# Replication verification helpers
# ---------------------------------------------------------------------------

# get_file_chunks <file_path>
# Get chunk IDs for a file via the manager API. Prints one chunk_id per line.
get_file_chunks() {
    local file_path="$1"
    # URL-encode the path
    local encoded_path
    encoded_path=$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1]))" "$file_path" 2>/dev/null \
        || echo "$file_path" | sed 's|/|%2F|g')
    api_get "/api/v1/files/${encoded_path}/chunks" \
        | jq -r '.[] | .chunk_id'
}

# check_replicas <chunk_id> <expected_count>
# Verify a chunk has exactly expected_count replicas via the API.
# Returns 0 if count matches, 1 otherwise.
check_replicas() {
    local chunk_id="$1"
    local expected_count="$2"

    local replica_info
    replica_info=$(api_get "/api/v1/chunks/${chunk_id}/replicas")
    if [ $? -ne 0 ]; then
        log_debug "Failed to get replica info for chunk $chunk_id"
        return 1
    fi

    local actual_count
    actual_count=$(echo "$replica_info" | jq 'length')

    if [ "$actual_count" -eq "$expected_count" ]; then
        return 0
    else
        log_debug "Chunk $chunk_id: expected $expected_count replicas, got $actual_count"
        return 1
    fi
}

# wait_for_replication <chunk_id> <expected_count> <timeout_seconds>
# Wait until a chunk reaches the expected replica count.
wait_for_replication() {
    local chunk_id="$1"
    local expected_count="$2"
    local timeout="${3:-60}"
    local start
    start=$(date +%s)
    local deadline=$((start + timeout))

    while true; do
        local now
        now=$(date +%s)
        if [ "$now" -ge "$deadline" ]; then
            local actual
            actual=$(api_get "/api/v1/chunks/${chunk_id}/replicas" | jq 'length' 2>/dev/null || echo 0)
            log_fail "Timed out waiting for $expected_count replicas of chunk $chunk_id (got $actual)"
            return 1
        fi
        if check_replicas "$chunk_id" "$expected_count"; then
            return 0
        fi
        sleep 3
    done
}

# get_chunk_nodes <chunk_id>
# Return list of node hostnames that hold a replica of this chunk.
get_chunk_nodes() {
    local chunk_id="$1"
    api_get "/api/v1/chunks/${chunk_id}/replicas" \
        | jq -r '.[].node_hostname'
}

# ---------------------------------------------------------------------------
# Network fault injection
# ---------------------------------------------------------------------------

# partition_network <host> <target_hosts_csv>
# Block all traffic from <host> to each of the target hosts using iptables.
# target_hosts_csv: comma-separated list of IPs, e.g. "10.0.1.2,10.0.1.3"
partition_network() {
    local host="$1"
    local target_hosts_csv="$2"

    log_info "Partitioning $host from: $target_hosts_csv"
    IFS=',' read -ra targets <<< "$target_hosts_csv"
    for target in "${targets[@]}"; do
        ssh_exec "$host" "iptables -I INPUT -s '$target' -j DROP" || true
        ssh_exec "$host" "iptables -I OUTPUT -d '$target' -j DROP" || true
        log_debug "Dropped traffic between $host and $target"
    done
}

# restore_network <host>
# Remove DTFS-specific iptables DROP rules added by partition_network.
restore_network() {
    local host="$1"
    log_info "Restoring network on $host"

    # Flush DROP rules added for test purposes (INPUT and OUTPUT)
    # We remove rules by iterating — safest approach without custom chains
    local i
    for i in 1 2 3 4 5; do
        ssh_exec "$host" "
            iptables -L INPUT --line-numbers -n 2>/dev/null |
            awk '/DROP/{print \$1}' | sort -rn |
            while read n; do iptables -D INPUT \$n 2>/dev/null; done
        " || true
        ssh_exec "$host" "
            iptables -L OUTPUT --line-numbers -n 2>/dev/null |
            awk '/DROP/{print \$1}' | sort -rn |
            while read n; do iptables -D OUTPUT \$n 2>/dev/null; done
        " || true
    done

    log_debug "Network restored on $host"
}

# add_latency <host> <interface> <latency_ms> <jitter_ms>
# Add artificial network latency using tc netem.
add_latency() {
    local host="$1"
    local iface="${2:-eth0}"
    local delay_ms="${3:-100}"
    local jitter_ms="${4:-10}"

    log_info "Adding ${delay_ms}ms latency (jitter: ${jitter_ms}ms) on $host:$iface"
    ssh_exec "$host" "tc qdisc add dev '$iface' root netem delay ${delay_ms}ms ${jitter_ms}ms || \
                      tc qdisc change dev '$iface' root netem delay ${delay_ms}ms ${jitter_ms}ms"
}

# remove_latency <host> <interface>
remove_latency() {
    local host="$1"
    local iface="${2:-eth0}"
    log_info "Removing tc netem on $host:$iface"
    ssh_exec "$host" "tc qdisc del dev '$iface' root 2>/dev/null || true"
}

# ---------------------------------------------------------------------------
# Service fault injection
# ---------------------------------------------------------------------------

# crash_service <host> <service_name>
# Kill a service with SIGKILL (simulates hard crash / OOM kill).
crash_service() {
    local host="$1"
    local service="$2"

    log_info "Crashing service $service on $host (SIGKILL)"
    # Get PID from systemd, then kill -9
    ssh_exec "$host" "
        pid=\$(systemctl show -p MainPID --value '$service' 2>/dev/null)
        if [ -n \"\$pid\" ] && [ \"\$pid\" != '0' ]; then
            kill -9 \$pid
            echo \"Killed $service PID \$pid\"
        else
            echo \"$service not running or PID unknown\"
            exit 1
        fi
    "
}

# start_service <host> <service_name>
start_service() {
    local host="$1"
    local service="$2"
    log_info "Starting service $service on $host"
    ssh_exec "$host" "systemctl start '$service'"
}

# stop_service <host> <service_name>
stop_service() {
    local host="$1"
    local service="$2"
    log_info "Stopping service $service on $host (SIGTERM)"
    ssh_exec "$host" "systemctl stop '$service'"
}

# restart_service <host> <service_name>
restart_service() {
    local host="$1"
    local service="$2"
    log_info "Restarting service $service on $host"
    ssh_exec "$host" "systemctl restart '$service'"
}

# service_is_running <host> <service_name>
# Returns 0 if the service is active, 1 otherwise.
service_is_running() {
    local host="$1"
    local service="$2"
    ssh_exec "$host" "systemctl is-active '$service' >/dev/null 2>&1"
}

# ---------------------------------------------------------------------------
# Disk fault injection
# ---------------------------------------------------------------------------

# fail_disk <host> <mountpoint>
# Simulate disk failure by unmounting the path.
fail_disk() {
    local host="$1"
    local mountpoint="$2"
    log_info "Simulating disk failure on $host:$mountpoint (unmounting)"
    ssh_exec "$host" "umount -f '$mountpoint' 2>/dev/null || umount -l '$mountpoint'"
}

# restore_disk <host> <mountpoint>
# Re-mount a previously unmounted disk path.
restore_disk() {
    local host="$1"
    local mountpoint="$2"
    log_info "Restoring disk on $host:$mountpoint (remounting from fstab)"
    ssh_exec "$host" "mount '$mountpoint'"
}

# corrupt_chunk_file <host> <chunk_path>
# Corrupt the first byte of a chunk file with a null byte.
corrupt_chunk_file() {
    local host="$1"
    local chunk_path="$2"
    log_info "Corrupting chunk file $host:$chunk_path"
    ssh_exec "$host" "dd if=/dev/zero of='$chunk_path' bs=1 count=1 conv=notrunc 2>/dev/null"
}

# find_chunk_file <host> <chunk_id>
# Locate the actual chunk file on disk given a chunk ID.
find_chunk_file() {
    local host="$1"
    local chunk_id="$2"
    # Chunks are stored under /mnt/disk-{a,b}/chunks/XX/YY/<chunk_id>
    ssh_exec "$host" "find /mnt/disk-a /mnt/disk-b -name '${chunk_id}' 2>/dev/null | head -1"
}

# ---------------------------------------------------------------------------
# Assert helpers
# ---------------------------------------------------------------------------

# assert_eq <actual> <expected> <message>
assert_eq() {
    local actual="$1"
    local expected="$2"
    local message="${3:-assert_eq}"
    if [ "$actual" = "$expected" ]; then
        log_ok "$message"
        return 0
    else
        log_fail "$message — expected='$expected' actual='$actual'"
        return 1
    fi
}

# assert_ne <actual> <unexpected> <message>
assert_ne() {
    local actual="$1"
    local unexpected="$2"
    local message="${3:-assert_ne}"
    if [ "$actual" != "$unexpected" ]; then
        log_ok "$message"
        return 0
    else
        log_fail "$message — value should not be '$unexpected'"
        return 1
    fi
}

# assert_contains <haystack> <needle> <message>
assert_contains() {
    local haystack="$1"
    local needle="$2"
    local message="${3:-assert_contains}"
    if echo "$haystack" | grep -qF "$needle"; then
        log_ok "$message"
        return 0
    else
        log_fail "$message — '$needle' not found in output"
        return 1
    fi
}

# assert_not_contains <haystack> <needle> <message>
assert_not_contains() {
    local haystack="$1"
    local needle="$2"
    local message="${3:-assert_not_contains}"
    if ! echo "$haystack" | grep -qF "$needle"; then
        log_ok "$message"
        return 0
    else
        log_fail "$message — '$needle' unexpectedly found in output"
        return 1
    fi
}

# assert_gt <actual> <min> <message>
assert_gt() {
    local actual="$1"
    local min="$2"
    local message="${3:-assert_gt}"
    if [ "$actual" -gt "$min" ] 2>/dev/null || \
       awk "BEGIN{exit !($actual > $min)}" 2>/dev/null; then
        log_ok "$message ($actual > $min)"
        return 0
    else
        log_fail "$message — expected $actual > $min"
        return 1
    fi
}

# assert_zero_exit <command_exit_code> <message>
assert_zero_exit() {
    local rc="$1"
    local message="${2:-assert_zero_exit}"
    if [ "$rc" -eq 0 ]; then
        log_ok "$message"
        return 0
    else
        log_fail "$message — command exited with code $rc"
        return 1
    fi
}

# assert_nonzero_exit <command_exit_code> <message>
assert_nonzero_exit() {
    local rc="$1"
    local message="${2:-assert_nonzero_exit}"
    if [ "$rc" -ne 0 ]; then
        log_ok "$message (exit code: $rc)"
        return 0
    else
        log_fail "$message — expected non-zero exit but got 0"
        return 1
    fi
}

# ---------------------------------------------------------------------------
# Maintenance mode
# ---------------------------------------------------------------------------

# set_maintenance <node_id> <true|false> [reason]
set_maintenance() {
    local node_id="$1"
    local enabled="$2"
    local reason="${3:-test}"
    api_put "/api/v1/nodes/${node_id}/maintenance" \
        "{\"enabled\": ${enabled}, \"reason\": \"${reason}\"}"
}

# ---------------------------------------------------------------------------
# DTFS CLI wrapper (runs on client-01 via SSH)
# ---------------------------------------------------------------------------

# dtfs_cli <args...>
# Run dtfs-cli on client-01 with the manager API configured.
dtfs_cli() {
    ssh_exec "$CLIENT_01" \
        "DTFS_MANAGER='${MANAGER_API}' ${DTFS_CLI} $*"
}

# dtfs_cli_local <args...>
# Run dtfs-cli locally (assumes it's installed on the test runner).
dtfs_cli_local() {
    DTFS_MANAGER="${MANAGER_API}" "${DTFS_CLI}" "$@"
}

# ---------------------------------------------------------------------------
# Setup / teardown helpers
# ---------------------------------------------------------------------------

# setup_log_dir
# Create the test log directory.
setup_log_dir() {
    mkdir -p "$LOG_DIR"
    log_info "Log directory: $LOG_DIR"
}

# cleanup_test_files <host> <path_prefix>
# Remove test files/dirs created during tests.
cleanup_test_files() {
    local host="$1"
    local path_prefix="$2"
    log_info "Cleaning up test files at ${host}:${path_prefix}"
    ssh_exec "$host" "rm -rf '${path_prefix}'" 2>/dev/null || true
}

# generate_unique_path <prefix>
# Generate a unique path for test files.
generate_unique_path() {
    local prefix="${1:-test}"
    echo "${TEST_MOUNT}/${prefix}-$(hostname -s)-$$-$(date +%s)"
}

# ---------------------------------------------------------------------------
# Initialization (run on source)
# ---------------------------------------------------------------------------

setup_log_dir

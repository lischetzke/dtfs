#!/usr/bin/env bash
# DTFS Integration Test Master Runner
# Usage: ./run_all.sh [options]
#   --skip-chaos      Skip chaos/fault injection tests (03-08)
#   --skip-perf       Skip performance-sensitive tests (09-10)
#   --only=<pattern>  Only run test scripts matching pattern (e.g. "01,02")
#   --stop-on-fail    Stop after first failing test script
#   --no-color        Disable color output
#   --timeout=<s>     Per-test script timeout in seconds (default: 600)

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
SKIP_CHAOS=0
SKIP_PERF=0
ONLY_PATTERN=""
STOP_ON_FAIL=0
TEST_TIMEOUT="${DTFS_TEST_TIMEOUT:-600}"

for arg in "$@"; do
    case "$arg" in
        --skip-chaos)     SKIP_CHAOS=1 ;;
        --skip-perf)      SKIP_PERF=1 ;;
        --only=*)         ONLY_PATTERN="${arg#--only=}" ;;
        --stop-on-fail)   STOP_ON_FAIL=1 ;;
        --no-color)       export NO_COLOR=1 ;;
        --timeout=*)      TEST_TIMEOUT="${arg#--timeout=}" ;;
        --help|-h)
            echo "Usage: $0 [--skip-chaos] [--skip-perf] [--only=01,02] [--stop-on-fail] [--timeout=600]"
            exit 0
            ;;
        *)
            echo "Unknown argument: $arg" >&2
            exit 1
            ;;
    esac
done

# ---------------------------------------------------------------------------
# Test script registry
# ---------------------------------------------------------------------------
# Format: "script_file:category:description"
declare -a TEST_REGISTRY=(
    "01_basic_io.sh:basic:Basic I/O operations"
    "02_replication.sh:basic:Replication verification"
    "03_network_disconnect.sh:chaos:Network partition handling"
    "04_service_crash.sh:chaos:Service crash and recovery"
    "05_disk_failure.sh:chaos:Disk failure and re-replication"
    "06_reconnect.sh:chaos:Node reconnect scenarios"
    "07_election_quorum.sh:chaos:Leader election and quorum"
    "08_client_reconnect.sh:chaos:FUSE client reconnect"
    "09_tiering.sh:perf:Data tiering"
    "10_scrub.sh:perf:Scrub and integrity check"
    "11_feature_validation.sh:basic:Comprehensive feature validation"
)

# ---------------------------------------------------------------------------
# Results tracking
# ---------------------------------------------------------------------------
declare -A TEST_RESULTS    # test_name -> PASS|FAIL|SKIP|TIMEOUT
declare -A TEST_DURATIONS  # test_name -> duration in seconds
OVERALL_PASS=0
OVERALL_FAIL=0
OVERALL_SKIP=0

# ---------------------------------------------------------------------------
# Pre-flight checks
# ---------------------------------------------------------------------------

log_section "Pre-flight Checks"

# Verify manager API is reachable
log_info "Checking DTFS manager API at $MANAGER_API"
if ! wait_for_api "${MANAGER_API}/api/v1/health" 30; then
    log_fail "Manager API at $MANAGER_API is not reachable. Is the cluster running?"
    exit 1
fi
log_ok "Manager API reachable"

# Verify cluster is healthy before starting
log_info "Verifying cluster health"
CLUSTER_STATUS=$(get_cluster_status 2>/dev/null || echo '{}')
CLUSTER_HEALTH=$(echo "$CLUSTER_STATUS" | jq -r '.status // "unknown"')
if [ "$CLUSTER_HEALTH" != "healthy" ]; then
    log_warn "Cluster status is '$CLUSTER_HEALTH' (not 'healthy') — tests may fail"
fi

# Verify SSH to all nodes
log_info "Verifying SSH connectivity to all nodes"
SSH_FAIL=0
for host in "${ALL_NODES[@]}"; do
    if ssh_exec "$host" "echo ok" >/dev/null 2>&1; then
        log_debug "SSH OK: $host"
    else
        log_warn "SSH FAIL: $host (some tests may be skipped)"
        SSH_FAIL=$((SSH_FAIL + 1))
    fi
done
if [ "$SSH_FAIL" -eq 0 ]; then
    log_ok "SSH connectivity verified for all nodes"
else
    log_warn "SSH connectivity issues on $SSH_FAIL node(s)"
fi

# Verify FUSE mount on client-01
log_info "Checking FUSE mount on client-01"
if ssh_exec "$CLIENT_01" "mountpoint -q '$TEST_MOUNT'" 2>/dev/null; then
    log_ok "DTFS FUSE mounted at client-01:$TEST_MOUNT"
else
    log_warn "DTFS FUSE not mounted — attempting to mount"
    mount_dtfs "$CLIENT_01" "$TEST_MOUNT" || log_warn "Mount failed — some tests may fail"
fi

# ---------------------------------------------------------------------------
# Determine which tests to run
# ---------------------------------------------------------------------------

should_run() {
    local script="$1"
    local category="$2"
    local basename="${script%.sh}"

    # Category filters
    if [ "$SKIP_CHAOS" -eq 1 ] && [ "$category" = "chaos" ]; then
        return 1
    fi
    if [ "$SKIP_PERF" -eq 1 ] && [ "$category" = "perf" ]; then
        return 1
    fi

    # Pattern filter
    if [ -n "$ONLY_PATTERN" ]; then
        IFS=',' read -ra patterns <<< "$ONLY_PATTERN"
        for p in "${patterns[@]}"; do
            if [[ "$basename" == *"$p"* ]]; then
                return 0
            fi
        done
        return 1
    fi

    return 0
}

# ---------------------------------------------------------------------------
# Run tests
# ---------------------------------------------------------------------------

log_section "Running Integration Tests"
echo ""

for entry in "${TEST_REGISTRY[@]}"; do
    IFS=':' read -r script category description <<< "$entry"
    script_path="$SCRIPT_DIR/$script"
    test_name="${script%.sh}"

    # Check if script exists
    if [ ! -f "$script_path" ]; then
        log_warn "Test script not found: $script_path (skipping)"
        TEST_RESULTS["$test_name"]="SKIP"
        TEST_DURATIONS["$test_name"]=0
        OVERALL_SKIP=$((OVERALL_SKIP + 1))
        continue
    fi

    # Check if should run
    if ! should_run "$script" "$category"; then
        log_skip "Skipping $test_name ($category)"
        TEST_RESULTS["$test_name"]="SKIP"
        TEST_DURATIONS["$test_name"]=0
        OVERALL_SKIP=$((OVERALL_SKIP + 1))
        continue
    fi

    log_section "Running: $test_name — $description"

    # Capture start time
    t_start=$(date +%s)

    # Run with timeout, capture exit code
    set +e
    timeout "$TEST_TIMEOUT" bash "$script_path" 2>&1
    rc=$?
    set -e

    t_end=$(date +%s)
    duration=$((t_end - t_start))
    TEST_DURATIONS["$test_name"]=$duration

    if [ "$rc" -eq 0 ]; then
        TEST_RESULTS["$test_name"]="PASS"
        OVERALL_PASS=$((OVERALL_PASS + 1))
        echo -e "${COLOR_GREEN}[PASS]${COLOR_RESET} $test_name (${duration}s)"
    elif [ "$rc" -eq 124 ]; then
        TEST_RESULTS["$test_name"]="TIMEOUT"
        OVERALL_FAIL=$((OVERALL_FAIL + 1))
        echo -e "${COLOR_RED}[TIMEOUT]${COLOR_RESET} $test_name (${TEST_TIMEOUT}s)"
    else
        TEST_RESULTS["$test_name"]="FAIL"
        OVERALL_FAIL=$((OVERALL_FAIL + 1))
        echo -e "${COLOR_RED}[FAIL]${COLOR_RESET} $test_name (${duration}s, exit=$rc)"
    fi

    if [ "$STOP_ON_FAIL" -eq 1 ] && [ "$rc" -ne 0 ]; then
        log_warn "Stopping after first failure (--stop-on-fail)"
        break
    fi

    # Brief pause between tests (let cluster settle)
    sleep 2
done

# ---------------------------------------------------------------------------
# Summary table
# ---------------------------------------------------------------------------

echo ""
echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║                   DTFS Integration Test Results                  ║"
echo "╠═══════════════════════════════════╦══════════╦═══════════════════╣"
printf "║ %-35s ║ %-8s ║ %s\n" "Test" "Result" "Duration (s)  ║"
echo "╠═══════════════════════════════════╬══════════╬═══════════════════╣"

for entry in "${TEST_REGISTRY[@]}"; do
    IFS=':' read -r script category description <<< "$entry"
    test_name="${script%.sh}"
    result="${TEST_RESULTS[$test_name]:-N/A}"
    dur="${TEST_DURATIONS[$test_name]:-0}"

    case "$result" in
        PASS)    color="$COLOR_GREEN" ;;
        FAIL)    color="$COLOR_RED" ;;
        TIMEOUT) color="$COLOR_RED" ;;
        SKIP)    color="$COLOR_CYAN" ;;
        *)       color="$COLOR_RESET" ;;
    esac

    printf "║ %-35s ║ ${color}%-8s${COLOR_RESET} ║ %-17s ║\n" \
        "$test_name" "$result" "${dur}s"
done

echo "╠═══════════════════════════════════╩══════════╩═══════════════════╣"
printf "║ %-30s %s %-4s │ %s %-4s │ %s %-4s          ║\n" \
    "Total: $((OVERALL_PASS + OVERALL_FAIL + OVERALL_SKIP))" \
    "${COLOR_GREEN}PASS:" "$OVERALL_PASS" \
    "${COLOR_RED}FAIL:" "$OVERALL_FAIL${COLOR_RESET}" \
    "${COLOR_CYAN}SKIP:" "$OVERALL_SKIP${COLOR_RESET}"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""

# Write results to file
RESULTS_FILE="$RESULTS_DIR/integration-$(date +%Y%m%d_%H%M%S).json"
mkdir -p "$RESULTS_DIR"
{
    echo '{'
    echo "  \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\","
    echo "  \"cluster_api\": \"$MANAGER_API\","
    echo "  \"summary\": {"
    echo "    \"pass\": $OVERALL_PASS,"
    echo "    \"fail\": $OVERALL_FAIL,"
    echo "    \"skip\": $OVERALL_SKIP"
    echo "  },"
    echo "  \"tests\": {"
    for entry in "${TEST_REGISTRY[@]}"; do
        IFS=':' read -r script category description <<< "$entry"
        test_name="${script%.sh}"
        result="${TEST_RESULTS[$test_name]:-N/A}"
        dur="${TEST_DURATIONS[$test_name]:-0}"
        echo "    \"$test_name\": {\"result\": \"$result\", \"duration_s\": $dur, \"category\": \"$category\"},"
    done | sed '$ s/,$//'
    echo "  }"
    echo '}'
} > "$RESULTS_FILE"
log_info "Results written to: $RESULTS_FILE"

# Exit with failure if any test failed
if [ "$OVERALL_FAIL" -gt 0 ]; then
    exit 1
fi
exit 0

#!/usr/bin/env bash
# DTFS Performance Test Suite Master Runner
# Runs all benchmark scripts in sequence and prints a summary table at the end.

set -euo pipefail
source "$(dirname "$0")/lib/common.sh"

MANAGER_API="${MANAGER_API:-http://10.0.1.1:9402}"
SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=10"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Export the timestamp so all sub-scripts share the same results dir
export DTFS_PERF_TS="$_PERF_RUN_TS"
export DTFS_RESULTS_DIR="$RESULTS_DIR"

BENCHMARKS=(
    "bench_sequential_write.sh"
    "bench_sequential_read.sh"
    "bench_random_read.sh"
    "bench_random_write.sh"
    "bench_cross_dc.sh"
)

PASS_SUITES=()
FAIL_SUITES=()

log_section "DTFS Performance Test Suite"
log_info "Run timestamp : $_PERF_RUN_TS"
log_info "Results dir   : $RESULTS_DIR"
log_info "Manager API   : $MANAGER_API"
log_info "FUSE mount    : $TEST_MOUNT"
echo ""

# ---------------------------------------------------------------------------
# Run each benchmark script
# ---------------------------------------------------------------------------
for bench in "${BENCHMARKS[@]}"; do
    bench_path="${SCRIPT_DIR}/${bench}"
    if [ ! -f "$bench_path" ]; then
        log_warn "Benchmark script not found: $bench_path — skipping"
        continue
    fi

    log_section "Running: $bench"
    BENCH_RC=0
    bash "$bench_path" || BENCH_RC=$?

    if [ $BENCH_RC -eq 0 ]; then
        PASS_SUITES+=("$bench")
        log_ok "Suite PASSED: $bench"
    else
        FAIL_SUITES+=("$bench")
        log_fail "Suite FAILED: $bench (rc=$BENCH_RC)"
    fi
    echo ""
done

# ---------------------------------------------------------------------------
# Summary table
# ---------------------------------------------------------------------------
echo ""
echo "============================================================"
echo "  DTFS Performance Benchmark Summary — ${_PERF_RUN_TS}"
echo "============================================================"
printf "  %-45s %s\n" "BENCHMARK" "STATUS"
printf "  %-45s %s\n" "---------" "------"

for bench in "${PASS_SUITES[@]}"; do
    printf "  ${COLOR_GREEN}%-45s PASS${COLOR_RESET}\n" "$bench"
done
for bench in "${FAIL_SUITES[@]}"; do
    printf "  ${COLOR_RED}%-45s FAIL${COLOR_RESET}\n" "$bench"
done

echo ""
echo "  Passed: ${#PASS_SUITES[@]}   Failed: ${#FAIL_SUITES[@]}"
echo ""

# ---------------------------------------------------------------------------
# Print numeric results from summary JSON
# ---------------------------------------------------------------------------
if [ -f "$SUMMARY_JSON" ] && command -v jq >/dev/null 2>&1; then
    echo "------------------------------------------------------------"
    echo "  Metric Results"
    echo "------------------------------------------------------------"
    printf "  %-45s %12s %s\n" "METRIC" "VALUE" "UNIT"
    printf "  %-45s %12s %s\n" "------" "-----" "----"
    jq -r '.results[] | "  \(.suite)/\(.metric)|\(.value)|\(.unit)"' \
        "$SUMMARY_JSON" 2>/dev/null | \
        while IFS='|' read -r metric value unit; do
            printf "  %-45s %12s %s\n" "$metric" "$value" "$unit"
        done
    echo ""
fi

echo "  Full results: $RESULTS_DIR"
echo "============================================================"
echo ""

if [ "${#FAIL_SUITES[@]}" -gt 0 ]; then
    exit 1
fi
exit 0

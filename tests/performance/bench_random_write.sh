#!/usr/bin/env bash
# DTFS Performance Benchmark: Random Write
# Measures random write IOPS and latency over the FUSE mount using fio.
# - 4 KB blocks, sync=1 (to test write safety / durability)
# - 128 KB blocks
# sync=1 forces fsync after each write, measuring durable write throughput.

set -euo pipefail
PERF_SUITE_NAME="bench_random_write"
source "$(dirname "$0")/lib/common.sh"

MANAGER_API="${MANAGER_API:-http://10.0.1.1:9402}"
SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=10"

MGR1="${MGR1:-mgr-01}"
MGR2="${MGR2:-mgr-02}"
MGR3="${MGR3:-mgr-03}"
DATA1="${DATA1:-data-01}"
DATA2="${DATA2:-data-02}"
CLIENT1="${CLIENT1:-client-01}"

MOUNT="${TEST_MOUNT:-/mnt/dtfs}"
BENCH_DIR="${MOUNT}/perf-randwrite-$$"
WORKING_SET_SIZE="8g"

perf_header "Random Write"

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
log_section "Setup: creating benchmark directory"
ssh_exec "$CLIENT_01" "mkdir -p '${BENCH_DIR}'"

# Pre-allocate a file so random writes don't also extend the file
log_info "Pre-allocating ${WORKING_SET_SIZE} working set with fallocate"
ssh_exec "$CLIENT_01" \
    "fallocate -l ${WORKING_SET_SIZE} '${BENCH_DIR}/workingset.dat' 2>/dev/null || \
     dd if=/dev/zero bs=4M count=2048 2>/dev/null > '${BENCH_DIR}/workingset.dat'"

# ---------------------------------------------------------------------------
# Run 1: Random write — 4 KB bs, sync=1 (durable), 4 jobs, QD=8
# ---------------------------------------------------------------------------
log_section "Run 1: Random write — 4 KB bs, sync=1 (durable), 4 jobs, QD=8, 60s"

FIO_JSON_4K_SYNC=$(ssh_exec "$CLIENT_01" \
    "fio \
        --name=randwrite_4k_sync \
        --filename='${BENCH_DIR}/workingset.dat' \
        --rw=randwrite \
        --bs=4k \
        --direct=1 \
        --sync=1 \
        --numjobs=4 \
        --iodepth=8 \
        --ioengine=libaio \
        --group_reporting \
        --runtime=60 \
        --time_based=1 \
        --output-format=json \
        --lat_percentiles=1 \
        --clat_percentiles=1 \
        --percentile_list=50:99:99.9 \
        2>/dev/null" \
    || echo '{}')

IOPS_4K_SYNC=$(parse_fio_iops "$FIO_JSON_4K_SYNC")
LAT_P50_4K_SYNC=$(parse_fio_lat_p50 "$FIO_JSON_4K_SYNC")
LAT_P99_4K_SYNC=$(parse_fio_lat_p99 "$FIO_JSON_4K_SYNC")
BW_4K_SYNC=$(parse_fio_bw "$FIO_JSON_4K_SYNC")

log_info "Run 1: IOPS=${IOPS_4K_SYNC} BW=${BW_4K_SYNC} MiB/s lat_p50=${LAT_P50_4K_SYNC}us lat_p99=${LAT_P99_4K_SYNC}us"
perf_result "rand_write_4k_sync_qd8_4jobs_iops"    "$IOPS_4K_SYNC"    "IOPS"
perf_result "rand_write_4k_sync_qd8_4jobs_bw"      "$BW_4K_SYNC"      "MiB/s"
perf_result "rand_write_4k_sync_qd8_4jobs_lat_p50" "$LAT_P50_4K_SYNC" "us"
perf_result "rand_write_4k_sync_qd8_4jobs_lat_p99" "$LAT_P99_4K_SYNC" "us"

echo "$FIO_JSON_4K_SYNC" > "${RESULTS_DIR}/randwrite_4k_sync_raw.json" 2>/dev/null || true

# ---------------------------------------------------------------------------
# Run 2: Random write — 4 KB bs, async (no sync), 4 jobs, QD=32 (throughput)
# ---------------------------------------------------------------------------
log_section "Run 2: Random write — 4 KB bs, async, 4 jobs, QD=32, 60s"

FIO_JSON_4K_ASYNC=$(ssh_exec "$CLIENT_01" \
    "fio \
        --name=randwrite_4k_async \
        --filename='${BENCH_DIR}/workingset.dat' \
        --rw=randwrite \
        --bs=4k \
        --direct=1 \
        --sync=0 \
        --numjobs=4 \
        --iodepth=32 \
        --ioengine=libaio \
        --group_reporting \
        --runtime=60 \
        --time_based=1 \
        --output-format=json \
        --lat_percentiles=1 \
        --clat_percentiles=1 \
        --percentile_list=50:99 \
        2>/dev/null" \
    || echo '{}')

IOPS_4K_ASYNC=$(parse_fio_iops "$FIO_JSON_4K_ASYNC")
LAT_P50_4K_ASYNC=$(parse_fio_lat_p50 "$FIO_JSON_4K_ASYNC")
LAT_P99_4K_ASYNC=$(parse_fio_lat_p99 "$FIO_JSON_4K_ASYNC")
BW_4K_ASYNC=$(parse_fio_bw "$FIO_JSON_4K_ASYNC")

log_info "Run 2: IOPS=${IOPS_4K_ASYNC} BW=${BW_4K_ASYNC} MiB/s lat_p50=${LAT_P50_4K_ASYNC}us lat_p99=${LAT_P99_4K_ASYNC}us"
perf_result "rand_write_4k_async_qd32_4jobs_iops"    "$IOPS_4K_ASYNC"    "IOPS"
perf_result "rand_write_4k_async_qd32_4jobs_bw"      "$BW_4K_ASYNC"      "MiB/s"
perf_result "rand_write_4k_async_qd32_4jobs_lat_p50" "$LAT_P50_4K_ASYNC" "us"
perf_result "rand_write_4k_async_qd32_4jobs_lat_p99" "$LAT_P99_4K_ASYNC" "us"

echo "$FIO_JSON_4K_ASYNC" > "${RESULTS_DIR}/randwrite_4k_async_raw.json" 2>/dev/null || true

# ---------------------------------------------------------------------------
# Run 3: Random write — 128 KB bs, sync=1, 4 jobs, QD=8
# ---------------------------------------------------------------------------
log_section "Run 3: Random write — 128 KB bs, sync=1, 4 jobs, QD=8, 60s"

FIO_JSON_128K=$(ssh_exec "$CLIENT_01" \
    "fio \
        --name=randwrite_128k_sync \
        --filename='${BENCH_DIR}/workingset.dat' \
        --rw=randwrite \
        --bs=128k \
        --direct=1 \
        --sync=1 \
        --numjobs=4 \
        --iodepth=8 \
        --ioengine=libaio \
        --group_reporting \
        --runtime=60 \
        --time_based=1 \
        --output-format=json \
        --lat_percentiles=1 \
        --clat_percentiles=1 \
        --percentile_list=50:99 \
        2>/dev/null" \
    || echo '{}')

IOPS_128K=$(parse_fio_iops "$FIO_JSON_128K")
LAT_P50_128K=$(parse_fio_lat_p50 "$FIO_JSON_128K")
LAT_P99_128K=$(parse_fio_lat_p99 "$FIO_JSON_128K")
BW_128K=$(parse_fio_bw "$FIO_JSON_128K")

log_info "Run 3: IOPS=${IOPS_128K} BW=${BW_128K} MiB/s lat_p50=${LAT_P50_128K}us lat_p99=${LAT_P99_128K}us"
perf_result "rand_write_128k_sync_qd8_4jobs_iops"    "$IOPS_128K"    "IOPS"
perf_result "rand_write_128k_sync_qd8_4jobs_bw"      "$BW_128K"      "MiB/s"
perf_result "rand_write_128k_sync_qd8_4jobs_lat_p50" "$LAT_P50_128K" "us"
perf_result "rand_write_128k_sync_qd8_4jobs_lat_p99" "$LAT_P99_128K" "us"

echo "$FIO_JSON_128K" > "${RESULTS_DIR}/randwrite_128k_raw.json" 2>/dev/null || true

# ---------------------------------------------------------------------------
# Run 4: Random write — single-threaded latency baseline (4 KB, sync=1, QD=1)
# ---------------------------------------------------------------------------
log_section "Run 4: Random write — 4 KB bs, sync=1, 1 job, QD=1 (latency baseline, 30s)"

FIO_JSON_BASELINE=$(ssh_exec "$CLIENT_01" \
    "fio \
        --name=randwrite_4k_baseline \
        --filename='${BENCH_DIR}/workingset.dat' \
        --rw=randwrite \
        --bs=4k \
        --direct=1 \
        --sync=1 \
        --numjobs=1 \
        --iodepth=1 \
        --ioengine=libaio \
        --runtime=30 \
        --time_based=1 \
        --output-format=json \
        --lat_percentiles=1 \
        --clat_percentiles=1 \
        --percentile_list=50:99 \
        2>/dev/null" \
    || echo '{}')

IOPS_BASE=$(parse_fio_iops "$FIO_JSON_BASELINE")
LAT_P50_BASE=$(parse_fio_lat_p50 "$FIO_JSON_BASELINE")
LAT_P99_BASE=$(parse_fio_lat_p99 "$FIO_JSON_BASELINE")

log_info "Run 4 (baseline): IOPS=${IOPS_BASE} lat_p50=${LAT_P50_BASE}us lat_p99=${LAT_P99_BASE}us"
perf_result "rand_write_4k_sync_qd1_1job_iops"    "$IOPS_BASE"    "IOPS"
perf_result "rand_write_4k_sync_qd1_1job_lat_p50" "$LAT_P50_BASE" "us"
perf_result "rand_write_4k_sync_qd1_1job_lat_p99" "$LAT_P99_BASE" "us"

echo "$FIO_JSON_BASELINE" > "${RESULTS_DIR}/randwrite_baseline_raw.json" 2>/dev/null || true

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
log_section "Random Write Summary"
printf "  %-55s %12s %s\n" "Metric" "Value" "Unit"
printf "  %-55s %12s %s\n" "------" "-----" "----"
printf "  %-55s %12s %s\n" "4 KB  sync QD=8  4-job IOPS (durable)"  "$IOPS_4K_SYNC"    "IOPS"
printf "  %-55s %12s %s\n" "4 KB  sync QD=8  4-job lat p99"          "$LAT_P99_4K_SYNC" "us"
printf "  %-55s %12s %s\n" "4 KB  async QD=32 4-job IOPS"            "$IOPS_4K_ASYNC"   "IOPS"
printf "  %-55s %12s %s\n" "4 KB  async QD=32 4-job BW"              "$BW_4K_ASYNC"     "MiB/s"
printf "  %-55s %12s %s\n" "128KB sync QD=8  4-job IOPS"             "$IOPS_128K"       "IOPS"
printf "  %-55s %12s %s\n" "128KB sync QD=8  4-job BW"               "$BW_128K"         "MiB/s"
printf "  %-55s %12s %s\n" "4 KB  sync QD=1  1-job IOPS (baseline)"  "$IOPS_BASE"       "IOPS"
printf "  %-55s %12s %s\n" "4 KB  sync QD=1  1-job lat p50 (baseline)" "$LAT_P50_BASE"  "us"
echo ""

# ---------------------------------------------------------------------------
# Teardown
# ---------------------------------------------------------------------------
log_info "Cleaning up benchmark files"
ssh_exec "$CLIENT_01" "rm -rf '${BENCH_DIR}'" 2>/dev/null || true

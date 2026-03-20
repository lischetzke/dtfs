#!/usr/bin/env bash
# DTFS Performance Benchmark: Random Read IOPS
# Measures random read IOPS and latency over the FUSE mount using fio.
# - 4 KB blocksize, queue depth 32, 4 jobs
# - 128 KB blocksize, queue depth 16, 4 jobs

set -euo pipefail
PERF_SUITE_NAME="bench_random_read"
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
BENCH_DIR="${MOUNT}/perf-randread-$$"
# Use a 16 GB working set so data spans multiple chunks
WORKING_SET_SIZE="16g"

perf_header "Random Read IOPS"

# ---------------------------------------------------------------------------
# Setup: pre-write working set so reads hit actual DTFS data
# ---------------------------------------------------------------------------
log_section "Setup: pre-writing ${WORKING_SET_SIZE} working set"
ssh_exec "$CLIENT_01" "mkdir -p '${BENCH_DIR}'"
log_info "Writing working set — this may take several minutes"
ssh_exec "$CLIENT_01" \
    "fio \
        --name=workingset_write \
        --filename='${BENCH_DIR}/workingset.dat' \
        --rw=write \
        --bs=4m \
        --direct=1 \
        --numjobs=1 \
        --size=${WORKING_SET_SIZE} \
        --iodepth=8 \
        --ioengine=libaio \
        --output-format=json \
        --time_based=0 \
        2>/dev/null" \
    >/dev/null 2>&1 || log_warn "Working set write completed with warnings"

# Drop page cache so reads are truly random from DTFS
ssh_exec "$CLIENT_01" "sync && echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true"
sleep 2

# ---------------------------------------------------------------------------
# Run 1: Random read — 4 KB bs, queue depth 32, 4 jobs
# ---------------------------------------------------------------------------
log_section "Run 1: Random read — 4 KB bs, QD=32, 4 jobs (60s runtime)"

FIO_JSON_4K=$(ssh_exec "$CLIENT_01" \
    "fio \
        --name=randread_4k \
        --filename='${BENCH_DIR}/workingset.dat' \
        --rw=randread \
        --bs=4k \
        --direct=1 \
        --numjobs=4 \
        --iodepth=32 \
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

IOPS_4K=$(parse_fio_iops "$FIO_JSON_4K")
LAT_P50_4K=$(parse_fio_lat_p50 "$FIO_JSON_4K")
LAT_P99_4K=$(parse_fio_lat_p99 "$FIO_JSON_4K")
BW_4K=$(parse_fio_bw "$FIO_JSON_4K")

log_info "Run 1: IOPS=${IOPS_4K} BW=${BW_4K} MiB/s lat_p50=${LAT_P50_4K}us lat_p99=${LAT_P99_4K}us"
perf_result "rand_read_4k_qd32_4jobs_iops"   "$IOPS_4K"    "IOPS"
perf_result "rand_read_4k_qd32_4jobs_bw"     "$BW_4K"      "MiB/s"
perf_result "rand_read_4k_qd32_4jobs_lat_p50" "$LAT_P50_4K" "us"
perf_result "rand_read_4k_qd32_4jobs_lat_p99" "$LAT_P99_4K" "us"

echo "$FIO_JSON_4K" > "${RESULTS_DIR}/randread_4k_raw.json" 2>/dev/null || true

# Drop cache between runs
ssh_exec "$CLIENT_01" "sync && echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true"

# ---------------------------------------------------------------------------
# Run 2: Random read — 128 KB bs, queue depth 16, 4 jobs
# ---------------------------------------------------------------------------
log_section "Run 2: Random read — 128 KB bs, QD=16, 4 jobs (60s runtime)"

FIO_JSON_128K=$(ssh_exec "$CLIENT_01" \
    "fio \
        --name=randread_128k \
        --filename='${BENCH_DIR}/workingset.dat' \
        --rw=randread \
        --bs=128k \
        --direct=1 \
        --numjobs=4 \
        --iodepth=16 \
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

IOPS_128K=$(parse_fio_iops "$FIO_JSON_128K")
LAT_P50_128K=$(parse_fio_lat_p50 "$FIO_JSON_128K")
LAT_P99_128K=$(parse_fio_lat_p99 "$FIO_JSON_128K")
BW_128K=$(parse_fio_bw "$FIO_JSON_128K")

log_info "Run 2: IOPS=${IOPS_128K} BW=${BW_128K} MiB/s lat_p50=${LAT_P50_128K}us lat_p99=${LAT_P99_128K}us"
perf_result "rand_read_128k_qd16_4jobs_iops"    "$IOPS_128K"    "IOPS"
perf_result "rand_read_128k_qd16_4jobs_bw"      "$BW_128K"      "MiB/s"
perf_result "rand_read_128k_qd16_4jobs_lat_p50" "$LAT_P50_128K" "us"
perf_result "rand_read_128k_qd16_4jobs_lat_p99" "$LAT_P99_128K" "us"

echo "$FIO_JSON_128K" > "${RESULTS_DIR}/randread_128k_raw.json" 2>/dev/null || true

# ---------------------------------------------------------------------------
# Run 3: Random read — 4 KB bs, single job, QD=1 (latency-sensitive baseline)
# ---------------------------------------------------------------------------
log_section "Run 3: Random read — 4 KB bs, QD=1, 1 job (latency baseline, 30s)"

FIO_JSON_SYNC=$(ssh_exec "$CLIENT_01" \
    "fio \
        --name=randread_4k_sync \
        --filename='${BENCH_DIR}/workingset.dat' \
        --rw=randread \
        --bs=4k \
        --direct=1 \
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

IOPS_SYNC=$(parse_fio_iops "$FIO_JSON_SYNC")
LAT_P50_SYNC=$(parse_fio_lat_p50 "$FIO_JSON_SYNC")
LAT_P99_SYNC=$(parse_fio_lat_p99 "$FIO_JSON_SYNC")

log_info "Run 3 (sync): IOPS=${IOPS_SYNC} lat_p50=${LAT_P50_SYNC}us lat_p99=${LAT_P99_SYNC}us"
perf_result "rand_read_4k_qd1_1job_iops"    "$IOPS_SYNC"    "IOPS"
perf_result "rand_read_4k_qd1_1job_lat_p50" "$LAT_P50_SYNC" "us"
perf_result "rand_read_4k_qd1_1job_lat_p99" "$LAT_P99_SYNC" "us"

echo "$FIO_JSON_SYNC" > "${RESULTS_DIR}/randread_4k_sync_raw.json" 2>/dev/null || true

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
log_section "Random Read Summary"
printf "  %-50s %12s %s\n" "Metric" "Value" "Unit"
printf "  %-50s %12s %s\n" "------" "-----" "----"
printf "  %-50s %12s %s\n" "4 KB  QD=32 4-job IOPS"   "$IOPS_4K"    "IOPS"
printf "  %-50s %12s %s\n" "4 KB  QD=32 4-job lat p50" "$LAT_P50_4K" "us"
printf "  %-50s %12s %s\n" "4 KB  QD=32 4-job lat p99" "$LAT_P99_4K" "us"
printf "  %-50s %12s %s\n" "128KB QD=16 4-job IOPS"    "$IOPS_128K"  "IOPS"
printf "  %-50s %12s %s\n" "128KB QD=16 4-job BW"      "$BW_128K"    "MiB/s"
printf "  %-50s %12s %s\n" "128KB QD=16 4-job lat p99" "$LAT_P99_128K" "us"
printf "  %-50s %12s %s\n" "4 KB  QD=1  1-job IOPS (sync baseline)" "$IOPS_SYNC" "IOPS"
printf "  %-50s %12s %s\n" "4 KB  QD=1  1-job lat p50 (sync)"       "$LAT_P50_SYNC" "us"
echo ""

# ---------------------------------------------------------------------------
# Teardown
# ---------------------------------------------------------------------------
log_info "Cleaning up benchmark files"
ssh_exec "$CLIENT_01" "rm -rf '${BENCH_DIR}'" 2>/dev/null || true

#!/usr/bin/env bash
# DTFS Performance Benchmark: Sequential Write
# Measures sequential write throughput over the FUSE mount using fio.
# - 1 job, 4 GB, 128 MB block size
# - 4 jobs, 1 GB each, 128 MB block size

set -euo pipefail
PERF_SUITE_NAME="bench_sequential_write"
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
BENCH_DIR="${MOUNT}/perf-seqwrite-$$"
FIO_TIMEOUT=600  # 10 minutes max per fio run

perf_header "Sequential Write"

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
log_info "Creating benchmark directory on $CLIENT_01"
ssh_exec "$CLIENT_01" "mkdir -p '${BENCH_DIR}'"

# ---------------------------------------------------------------------------
# Run 1: 1 job × 4 GB, 128 MB blocksize, direct I/O
# ---------------------------------------------------------------------------
log_section "Run 1: Sequential write — 1 job × 4 GB, bs=128M, direct=1"

FIO_JSON_1=$(ssh_exec "$CLIENT_01" \
    "fio \
        --name=seqwrite_1j \
        --filename='${BENCH_DIR}/seqwrite_1j.dat' \
        --rw=write \
        --bs=128m \
        --direct=1 \
        --numjobs=1 \
        --size=4g \
        --iodepth=1 \
        --ioengine=libaio \
        --group_reporting \
        --output-format=json \
        --time_based=0 \
        2>/dev/null" \
    || echo '{}')

BW_1J=$(parse_fio_bw "$FIO_JSON_1")
log_info "Run 1 bandwidth: ${BW_1J} MiB/s"
perf_result "seq_write_1job_4gb_bs128m" "$BW_1J" "MiB/s"

# Save raw fio output
echo "$FIO_JSON_1" > "${RESULTS_DIR}/seqwrite_1job_raw.json" 2>/dev/null || true

# ---------------------------------------------------------------------------
# Run 2: 4 jobs × 1 GB each, 128 MB blocksize, direct I/O
# ---------------------------------------------------------------------------
log_section "Run 2: Sequential write — 4 jobs × 1 GB, bs=128M, direct=1"

FIO_JSON_4=$(ssh_exec "$CLIENT_01" \
    "fio \
        --name=seqwrite_4j \
        --filename='${BENCH_DIR}/seqwrite_4j_\$\$JOBNUM\$\$.dat' \
        --rw=write \
        --bs=128m \
        --direct=1 \
        --numjobs=4 \
        --size=1g \
        --iodepth=1 \
        --ioengine=libaio \
        --group_reporting \
        --output-format=json \
        --time_based=0 \
        2>/dev/null" \
    || echo '{}')

BW_4J=$(parse_fio_bw "$FIO_JSON_4")
log_info "Run 2 bandwidth: ${BW_4J} MiB/s"
perf_result "seq_write_4jobs_4gb_bs128m" "$BW_4J" "MiB/s"

echo "$FIO_JSON_4" > "${RESULTS_DIR}/seqwrite_4jobs_raw.json" 2>/dev/null || true

# ---------------------------------------------------------------------------
# Run 3: 1 job × 4 GB, 1 MB blocksize (smaller blocks = more realistic)
# ---------------------------------------------------------------------------
log_section "Run 3: Sequential write — 1 job × 4 GB, bs=1M, direct=1"

FIO_JSON_1M=$(ssh_exec "$CLIENT_01" \
    "fio \
        --name=seqwrite_1j_1m \
        --filename='${BENCH_DIR}/seqwrite_1j_1m.dat' \
        --rw=write \
        --bs=1m \
        --direct=1 \
        --numjobs=1 \
        --size=4g \
        --iodepth=4 \
        --ioengine=libaio \
        --group_reporting \
        --output-format=json \
        --time_based=0 \
        2>/dev/null" \
    || echo '{}')

BW_1M=$(parse_fio_bw "$FIO_JSON_1M")
log_info "Run 3 bandwidth: ${BW_1M} MiB/s"
perf_result "seq_write_1job_4gb_bs1m" "$BW_1M" "MiB/s"

echo "$FIO_JSON_1M" > "${RESULTS_DIR}/seqwrite_1job_1m_raw.json" 2>/dev/null || true

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
log_section "Sequential Write Summary"
printf "  %-45s %12s %s\n" "Metric" "Value" "Unit"
printf "  %-45s %12s %s\n" "------" "-----" "----"
printf "  %-45s %12s %s\n" "1 job  × 4 GB  bs=128M" "${BW_1J}"  "MiB/s"
printf "  %-45s %12s %s\n" "4 jobs × 1 GB  bs=128M" "${BW_4J}"  "MiB/s"
printf "  %-45s %12s %s\n" "1 job  × 4 GB  bs=1M"   "${BW_1M}"  "MiB/s"
echo ""

# ---------------------------------------------------------------------------
# Teardown
# ---------------------------------------------------------------------------
log_info "Cleaning up benchmark files"
ssh_exec "$CLIENT_01" "rm -rf '${BENCH_DIR}'" 2>/dev/null || true

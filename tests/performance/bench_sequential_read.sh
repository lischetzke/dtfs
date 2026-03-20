#!/usr/bin/env bash
# DTFS Performance Benchmark: Sequential Read
# Measures sequential read throughput over the FUSE mount using fio.
# Files are written first to ensure data exists, then read back.
# - 1 job, 4 GB, 128 MB block size
# - 4 concurrent jobs, 1 GB each

set -euo pipefail
PERF_SUITE_NAME="bench_sequential_read"
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
BENCH_DIR="${MOUNT}/perf-seqread-$$"

perf_header "Sequential Read"

# ---------------------------------------------------------------------------
# Setup: write data first so reads have something to read
# ---------------------------------------------------------------------------
log_section "Setup: pre-writing test data for read benchmarks"
ssh_exec "$CLIENT_01" "mkdir -p '${BENCH_DIR}'"

log_info "Pre-writing 4 GB file for single-job read test"
ssh_exec "$CLIENT_01" \
    "dd if=/dev/urandom bs=128M count=32 2>/dev/null > '${BENCH_DIR}/read_1j.dat'"

log_info "Pre-writing 4 × 1 GB files for multi-job read test"
for i in 1 2 3 4; do
    ssh_exec "$CLIENT_01" \
        "dd if=/dev/urandom bs=128M count=8 2>/dev/null > '${BENCH_DIR}/read_4j_${i}.dat'"
done

# Drop page cache on client to ensure reads come from DTFS, not local cache
log_info "Dropping page cache on $CLIENT_01"
ssh_exec "$CLIENT_01" "sync && echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true"
sleep 2

# ---------------------------------------------------------------------------
# Run 1: 1 job × 4 GB, 128 MB blocksize, direct I/O
# ---------------------------------------------------------------------------
log_section "Run 1: Sequential read — 1 job × 4 GB, bs=128M, direct=1"

FIO_JSON_1J=$(ssh_exec "$CLIENT_01" \
    "fio \
        --name=seqread_1j \
        --filename='${BENCH_DIR}/read_1j.dat' \
        --rw=read \
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

BW_1J=$(parse_fio_bw "$FIO_JSON_1J")
log_info "Run 1 bandwidth: ${BW_1J} MiB/s"
perf_result "seq_read_1job_4gb_bs128m" "$BW_1J" "MiB/s"
echo "$FIO_JSON_1J" > "${RESULTS_DIR}/seqread_1job_raw.json" 2>/dev/null || true

# Drop cache again between runs
ssh_exec "$CLIENT_01" "sync && echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true"

# ---------------------------------------------------------------------------
# Run 2: 4 concurrent jobs × 1 GB each, 128 MB blocksize, direct I/O
# ---------------------------------------------------------------------------
log_section "Run 2: Sequential read — 4 jobs × 1 GB, bs=128M, direct=1"

FIO_JSON_4J=$(ssh_exec "$CLIENT_01" \
    "fio \
        --name=seqread_4j_1 --filename='${BENCH_DIR}/read_4j_1.dat' --rw=read --bs=128m --direct=1 --numjobs=1 --size=1g --iodepth=1 --ioengine=libaio \
        --name=seqread_4j_2 --filename='${BENCH_DIR}/read_4j_2.dat' --rw=read --bs=128m --direct=1 --numjobs=1 --size=1g --iodepth=1 --ioengine=libaio \
        --name=seqread_4j_3 --filename='${BENCH_DIR}/read_4j_3.dat' --rw=read --bs=128m --direct=1 --numjobs=1 --size=1g --iodepth=1 --ioengine=libaio \
        --name=seqread_4j_4 --filename='${BENCH_DIR}/read_4j_4.dat' --rw=read --bs=128m --direct=1 --numjobs=1 --size=1g --iodepth=1 --ioengine=libaio \
        --group_reporting \
        --output-format=json \
        --time_based=0 \
        2>/dev/null" \
    || echo '{}')

BW_4J=$(parse_fio_bw "$FIO_JSON_4J")
log_info "Run 2 bandwidth: ${BW_4J} MiB/s"
perf_result "seq_read_4jobs_4gb_bs128m" "$BW_4J" "MiB/s"
echo "$FIO_JSON_4J" > "${RESULTS_DIR}/seqread_4jobs_raw.json" 2>/dev/null || true

# Drop cache again
ssh_exec "$CLIENT_01" "sync && echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true"

# ---------------------------------------------------------------------------
# Run 3: 1 job × 4 GB, 1 MB blocksize
# ---------------------------------------------------------------------------
log_section "Run 3: Sequential read — 1 job × 4 GB, bs=1M, direct=1"

FIO_JSON_1M=$(ssh_exec "$CLIENT_01" \
    "fio \
        --name=seqread_1j_1m \
        --filename='${BENCH_DIR}/read_1j.dat' \
        --rw=read \
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
perf_result "seq_read_1job_4gb_bs1m" "$BW_1M" "MiB/s"
echo "$FIO_JSON_1M" > "${RESULTS_DIR}/seqread_1job_1m_raw.json" 2>/dev/null || true

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
log_section "Sequential Read Summary"
printf "  %-45s %12s %s\n" "Metric" "Value" "Unit"
printf "  %-45s %12s %s\n" "------" "-----" "----"
printf "  %-45s %12s %s\n" "1 job  × 4 GB  bs=128M" "${BW_1J}" "MiB/s"
printf "  %-45s %12s %s\n" "4 jobs × 4 GB  bs=128M" "${BW_4J}" "MiB/s"
printf "  %-45s %12s %s\n" "1 job  × 4 GB  bs=1M"   "${BW_1M}" "MiB/s"
echo ""

# ---------------------------------------------------------------------------
# Teardown
# ---------------------------------------------------------------------------
log_info "Cleaning up benchmark files"
ssh_exec "$CLIENT_01" "rm -rf '${BENCH_DIR}'" 2>/dev/null || true

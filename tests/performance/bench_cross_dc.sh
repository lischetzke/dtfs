#!/usr/bin/env bash
# DTFS Performance Benchmark: Cross-DC Performance
# Measures raw network bandwidth between DCs using iperf3, and file I/O
# latency/throughput when writing from one DC and reading from another.

set -euo pipefail
PERF_SUITE_NAME="bench_cross_dc"
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
BENCH_DIR="${MOUNT}/perf-crossdc-$$"

# Remote DC client — if not configured, this suite is skipped
REMOTE_CLIENT="${DTFS_REMOTE_CLIENT:-}"
IPERF3_SERVER="${DTFS_IPERF3_SERVER:-}"    # IP of remote-DC iperf3 server
IPERF3_PORT="${DTFS_IPERF3_PORT:-5201}"

perf_header "Cross-DC Performance"

# ---------------------------------------------------------------------------
# Check whether cross-DC targets are configured
# ---------------------------------------------------------------------------
if [ -z "$REMOTE_CLIENT" ] && [ -z "$IPERF3_SERVER" ]; then
    log_warn "No cross-DC targets configured."
    log_warn "Set DTFS_REMOTE_CLIENT and/or DTFS_IPERF3_SERVER to enable cross-DC benchmarks."
    log_warn "Skipping cross-DC benchmark suite."
    perf_result "cross_dc_skipped" "1" "bool"
    exit 0
fi

# ---------------------------------------------------------------------------
# TC-01: iperf3 — raw network bandwidth between DCs
# ---------------------------------------------------------------------------
log_section "TC-01: iperf3 raw network bandwidth (DC to DC)"

if [ -z "$IPERF3_SERVER" ]; then
    log_warn "TC-01: DTFS_IPERF3_SERVER not set — skipping iperf3 test"
    perf_result "cross_dc_iperf3_bw_mbps" "N/A" "Mbps"
else
    # Start iperf3 server on remote host (if not already running)
    log_info "Starting iperf3 server on $IPERF3_SERVER (port $IPERF3_PORT)"
    ssh_exec "$IPERF3_SERVER" \
        "pkill iperf3 2>/dev/null; nohup iperf3 -s -p ${IPERF3_PORT} -D 2>/dev/null; sleep 1" \
        2>/dev/null || true
    sleep 2

    # Run iperf3 client from CLIENT_01 to IPERF3_SERVER
    log_info "Running iperf3: $CLIENT_01 -> $IPERF3_SERVER (10s, TCP)"
    IPERF_JSON=$(ssh_exec "$CLIENT_01" \
        "iperf3 -c '${IPERF3_SERVER}' -p ${IPERF3_PORT} -t 10 -J 2>/dev/null" \
        || echo '{}')

    # Parse bandwidth (Mbps)
    IPERF_BW=$(python3 - <<PYEOF 2>/dev/null || echo "0"
import json, sys
try:
    data = json.loads('''$IPERF_JSON''')
    bps = data["end"]["sum_received"]["bits_per_second"]
    print(f"{bps / 1e6:.1f}")
except Exception:
    print("0")
PYEOF
)
    log_info "TC-01: iperf3 bandwidth: ${IPERF_BW} Mbps"
    perf_result "cross_dc_iperf3_bw_mbps" "$IPERF_BW" "Mbps"
    echo "$IPERF_JSON" > "${RESULTS_DIR}/crossdc_iperf3_raw.json" 2>/dev/null || true

    # RTT latency (ping)
    log_info "Measuring RTT: $CLIENT_01 -> $IPERF3_SERVER"
    RTT=$(ssh_exec "$CLIENT_01" \
        "ping -c 10 -q '${IPERF3_SERVER}' 2>/dev/null | awk -F'/' '/rtt/{print \$5}'" \
        || echo "N/A")
    log_info "TC-01: Round-trip latency: ${RTT} ms (avg)"
    perf_result "cross_dc_rtt_avg_ms" "$RTT" "ms"

    # Cleanup iperf3 server
    ssh_exec "$IPERF3_SERVER" "pkill iperf3 2>/dev/null || true" 2>/dev/null || true
fi

# ---------------------------------------------------------------------------
# TC-02: Write file from local DC client, read from remote DC client
# ---------------------------------------------------------------------------
log_section "TC-02: Write from $CLIENT_01, read from $REMOTE_CLIENT (cross-DC read)"

if [ -z "$REMOTE_CLIENT" ]; then
    log_warn "TC-02: DTFS_REMOTE_CLIENT not set — skipping cross-DC file I/O test"
    perf_result "cross_dc_write_bw_mib_s" "N/A" "MiB/s"
    perf_result "cross_dc_read_bw_mib_s"  "N/A" "MiB/s"
else
    ssh_exec "$CLIENT_01" "mkdir -p '${BENCH_DIR}'"

    # Write a 1 GB file from the local DC client and measure throughput
    log_info "Writing 1 GB file on $CLIENT_01 (local DC write)"
    WRITE_START=$(date +%s%N)
    ssh_exec "$CLIENT_01" \
        "dd if=/dev/urandom bs=128M count=8 2>/dev/null > '${BENCH_DIR}/crossdc_test.dat'" \
        2>/dev/null || true
    WRITE_END=$(date +%s%N)
    WRITE_ELAPSED_NS=$((WRITE_END - WRITE_START))
    WRITE_BW=$(python3 -c "print(f'{(1024 / ($WRITE_ELAPSED_NS / 1e9)):.2f}')" 2>/dev/null || echo "0")
    log_info "TC-02: Local write throughput: ${WRITE_BW} MiB/s"
    perf_result "cross_dc_write_bw_mib_s" "$WRITE_BW" "MiB/s"

    # Ensure the remote client also has the FUSE mount
    log_info "Verifying FUSE mount on remote client: $REMOTE_CLIENT"
    REMOTE_MOUNTED=$(ssh_exec "$REMOTE_CLIENT" \
        "mountpoint -q '${MOUNT}' && echo yes || echo no" 2>/dev/null || echo "no")
    if [ "$REMOTE_MOUNTED" != "yes" ]; then
        log_warn "TC-02: FUSE is not mounted on $REMOTE_CLIENT — attempting to start dtfs-fuse"
        ssh_exec "$REMOTE_CLIENT" \
            "systemctl start 'dtfs-fuse@default.service'" 2>/dev/null || true
        sleep 5
        REMOTE_MOUNTED=$(ssh_exec "$REMOTE_CLIENT" \
            "mountpoint -q '${MOUNT}' && echo yes || echo no" 2>/dev/null || echo "no")
    fi

    if [ "$REMOTE_MOUNTED" != "yes" ]; then
        log_warn "TC-02: FUSE not available on $REMOTE_CLIENT — skipping cross-DC read"
        perf_result "cross_dc_read_bw_mib_s" "N/A" "MiB/s"
    else
        # Drop page cache on remote client
        ssh_exec "$REMOTE_CLIENT" \
            "sync && echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true" 2>/dev/null || true
        sleep 1

        # Read the file from the remote DC client
        log_info "Reading ${BENCH_DIR}/crossdc_test.dat from $REMOTE_CLIENT (remote DC read)"
        READ_START=$(date +%s%N)
        ssh_exec "$REMOTE_CLIENT" \
            "dd if='${BENCH_DIR}/crossdc_test.dat' bs=128M 2>/dev/null | md5sum >/dev/null" \
            2>/dev/null || true
        READ_END=$(date +%s%N)
        READ_ELAPSED_NS=$((READ_END - READ_START))
        READ_BW=$(python3 -c "print(f'{(1024 / ($READ_ELAPSED_NS / 1e9)):.2f}')" 2>/dev/null || echo "0")
        log_info "TC-02: Cross-DC read throughput: ${READ_BW} MiB/s"
        perf_result "cross_dc_read_bw_mib_s" "$READ_BW" "MiB/s"

        # fio latency test: small random reads from remote client
        log_info "Measuring cross-DC random read latency via fio (4KB, QD=1, 30s)"
        FIO_LATENCY_JSON=$(ssh_exec "$REMOTE_CLIENT" \
            "fio \
                --name=crossdc_randread \
                --filename='${BENCH_DIR}/crossdc_test.dat' \
                --rw=randread \
                --bs=4k \
                --direct=1 \
                --numjobs=1 \
                --iodepth=1 \
                --ioengine=libaio \
                --runtime=30 \
                --time_based=1 \
                --output-format=json \
                --percentile_list=50:99 \
                2>/dev/null" \
            || echo '{}')

        CROSS_DC_LAT_P50=$(parse_fio_lat_p50 "$FIO_LATENCY_JSON")
        CROSS_DC_LAT_P99=$(parse_fio_lat_p99 "$FIO_LATENCY_JSON")
        log_info "Cross-DC fio lat_p50=${CROSS_DC_LAT_P50}us lat_p99=${CROSS_DC_LAT_P99}us"
        perf_result "cross_dc_fio_randread_lat_p50_us" "$CROSS_DC_LAT_P50" "us"
        perf_result "cross_dc_fio_randread_lat_p99_us" "$CROSS_DC_LAT_P99" "us"

        echo "$FIO_LATENCY_JSON" > "${RESULTS_DIR}/crossdc_fio_raw.json" 2>/dev/null || true
    fi

    # ---------------------------------------------------------------------------
    # TC-03: iperf3 multi-stream (if iperf3 server available)
    # ---------------------------------------------------------------------------
    if [ -n "$IPERF3_SERVER" ]; then
        log_section "TC-03: iperf3 parallel streams (4 streams, 10s)"
        ssh_exec "$IPERF3_SERVER" \
            "pkill iperf3 2>/dev/null; nohup iperf3 -s -p ${IPERF3_PORT} -D 2>/dev/null; sleep 1" \
            2>/dev/null || true
        sleep 2

        IPERF_PAR_JSON=$(ssh_exec "$CLIENT_01" \
            "iperf3 -c '${IPERF3_SERVER}' -p ${IPERF3_PORT} -t 10 -P 4 -J 2>/dev/null" \
            || echo '{}')

        IPERF_PAR_BW=$(python3 - <<PYEOF 2>/dev/null || echo "0"
import json
try:
    data = json.loads('''$IPERF_PAR_JSON''')
    bps = data["end"]["sum_received"]["bits_per_second"]
    print(f"{bps / 1e6:.1f}")
except Exception:
    print("0")
PYEOF
)
        log_info "TC-03: iperf3 4-stream bandwidth: ${IPERF_PAR_BW} Mbps"
        perf_result "cross_dc_iperf3_4stream_bw_mbps" "$IPERF_PAR_BW" "Mbps"
        echo "$IPERF_PAR_JSON" > "${RESULTS_DIR}/crossdc_iperf3_4stream_raw.json" 2>/dev/null || true

        ssh_exec "$IPERF3_SERVER" "pkill iperf3 2>/dev/null || true" 2>/dev/null || true
    fi
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
log_section "Cross-DC Summary"
printf "  %-50s %12s %s\n" "Metric" "Value" "Unit"
printf "  %-50s %12s %s\n" "------" "-----" "----"

if [ -n "$IPERF3_SERVER" ]; then
    printf "  %-50s %12s %s\n" "iperf3 single-stream bandwidth" "${IPERF_BW:-N/A}" "Mbps"
    printf "  %-50s %12s %s\n" "iperf3 4-stream bandwidth"     "${IPERF_PAR_BW:-N/A}" "Mbps"
    printf "  %-50s %12s %s\n" "RTT average latency"          "${RTT:-N/A}" "ms"
fi
if [ -n "$REMOTE_CLIENT" ]; then
    printf "  %-50s %12s %s\n" "Local DC write throughput"     "${WRITE_BW:-N/A}" "MiB/s"
    printf "  %-50s %12s %s\n" "Cross-DC read throughput"      "${READ_BW:-N/A}" "MiB/s"
    printf "  %-50s %12s %s\n" "Cross-DC fio lat p50"          "${CROSS_DC_LAT_P50:-N/A}" "us"
    printf "  %-50s %12s %s\n" "Cross-DC fio lat p99"          "${CROSS_DC_LAT_P99:-N/A}" "us"
fi
echo ""

# ---------------------------------------------------------------------------
# Teardown
# ---------------------------------------------------------------------------
log_info "Cleaning up benchmark files"
ssh_exec "$CLIENT_01" "rm -rf '${BENCH_DIR}'" 2>/dev/null || true
if [ -n "$REMOTE_CLIENT" ]; then
    ssh_exec "$REMOTE_CLIENT" "rm -rf '${BENCH_DIR}'" 2>/dev/null || true
fi

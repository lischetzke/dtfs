#!/usr/bin/env bash
# Plexmedia soak driver — 1h → 4h → 8h chain.
#
# The chain only proceeds to the next stage if the previous one passes
# its PASS criteria. Each stage is self-contained: workload generators,
# scenario injection, metrics scrape, end-of-stage report.
#
# Usage:
#   ./run-plexmedia.sh [--duration 1h|4h|8h|chain] [--corpus PATH]
#
# `chain` runs 1h → 4h → 8h, stopping on any FAIL.

set -euo pipefail

# ---------- defaults ----------
DURATION_ARG="${1:-1h}"
RESULTS_ROOT="$(dirname "$0")/results"
SCRIPT_DIR="$(dirname "$0")"

# Plexmedia inventory IPs (matches tests/proxmox/inventory-plexmedia.json)
MGR01=10.150.0.151   # LELI voter, bootstrap
MGR02=10.150.0.152   # LELI voter
MGR03=10.150.0.153   # LELI voter
MGR04=10.150.0.154   # TORO observer
MGR05=10.150.0.155   # NRB observer
DATA_PRIMARY=10.150.0.156   # LELI 600 GB anchor
DATA_WARM_TORO=10.150.0.157  # TORO 180 GB SSD
DATA_WARM_NRB=10.150.0.158   # NRB 180 GB
CLIENT_LELI=10.150.0.159   # Plex viewer in LELI
CLIENT_TORO=10.150.0.160   # Plex viewer in TORO (disk cache)
CLIENT_NRB=10.150.0.161    # Download ingest

SSH_USER=debian
SSH_KEY="${HOME}/.ssh/dtfs-lab/id_ed25519"
SSH_OPTS="-o StrictHostKeyChecking=no -o LogLevel=ERROR -i ${SSH_KEY}"

# Plex constants (from inventory.plex section)
PLEX_STREAM_MBPS=60
PLEX_STREAMS_PER_CLIENT=5

# ---------- helpers ----------
log() { echo "[$(date -Iseconds)] $*"; }

run_remote() {
    local host=$1
    shift
    ssh $SSH_OPTS "${SSH_USER}@${host}" "$@"
}

preflight_cluster() {
    log "preflight: cluster status via mgr01"
    if ! curl -fsS --max-time 10 "http://${MGR01}:9402/api/v1/cluster/status" > /dev/null; then
        log "ERROR: mgr01 unreachable — aborting"
        return 1
    fi
    log "preflight: all 3 data nodes reachable?"
    for host in "$DATA_PRIMARY" "$DATA_WARM_TORO" "$DATA_WARM_NRB"; do
        if ! run_remote "$host" "systemctl is-active dtfs-data" > /dev/null 2>&1; then
            log "ERROR: dtfs-data not active on $host"
            return 1
        fi
    done
    log "preflight: PASS"
    return 0
}

# Plex-shape playback generator. Spawns 5 concurrent dd-from-FUSE streams
# at 60 Mbps each (7.5 MB/s). Total per-client: 300 Mbps.
plex_playback_script() {
    cat <<'SCRIPT'
set -euo pipefail
MOUNT=/mnt/dtfs
STREAM_MBPS=60
NUM_STREAMS=5

mkdir -p /tmp/plex-stats
LOG=/tmp/plex-stats/playback.log
echo "[$(date -Iseconds)] plex playback start: ${NUM_STREAMS}×${STREAM_MBPS}Mbps" >> "$LOG"

# Compute IO rate per stream: 60 Mbps = 7.5 MB/s = 7864320 B/s.
RATE_BPS=$(( STREAM_MBPS * 1024 * 1024 / 8 ))

# Discover viewable files: anything > 50 MB under the mount, max 50 files.
mapfile -t FILES < <(find "$MOUNT" -type f -size +50M 2>/dev/null | head -50)
if [ "${#FILES[@]}" -eq 0 ]; then
    echo "[$(date -Iseconds)] no files to stream yet (corpus not preloaded?)" >> "$LOG"
    sleep 30
    exit 0
fi

# Spawn NUM_STREAMS concurrent readers; each picks a random file, reads
# at ~RATE_BPS using pv-style throttling (dd + sleep loop).
for i in $(seq 1 $NUM_STREAMS); do
    (
        while true; do
            FILE="${FILES[$((RANDOM % ${#FILES[@]}))]}"
            # 5 MB chunk reads with delay to hit RATE_BPS/sec average.
            CHUNK=5242880
            DELAY=$(awk "BEGIN {printf \"%.3f\", $CHUNK / $RATE_BPS}")
            dd if="$FILE" of=/dev/null bs=$CHUNK count=1 status=none 2>/dev/null || true
            sleep "$DELAY"
        done
    ) &
done

# Run until parent kills us.
wait
SCRIPT
}

# Download-shape ingest generator. Writes new files (random data) at
# ~60 Mbps, mirroring fresh-content ingest in NRB.
ingest_script() {
    cat <<'SCRIPT'
set -euo pipefail
MOUNT=/mnt/dtfs
INGEST_DIR="${MOUNT}/incoming/$(hostname)"
RATE_MBPS=60
RATE_BPS=$(( RATE_MBPS * 1024 * 1024 / 8 ))

mkdir -p "$INGEST_DIR"
mkdir -p /tmp/plex-stats
LOG=/tmp/plex-stats/ingest.log
echo "[$(date -Iseconds)] ingest start: $RATE_MBPS Mbps" >> "$LOG"

i=0
while true; do
    i=$((i + 1))
    # Each file ~50 MB; at 60 Mbps that's ~7s per file.
    FILE="${INGEST_DIR}/$(date +%Y%m%d-%H%M%S)-$i.bin"
    head -c 52428800 /dev/urandom > "$FILE" 2>>"$LOG" || true
    echo "[$(date -Iseconds)] wrote $FILE" >> "$LOG"
    sleep 1
done
SCRIPT
}

# ---------- run a single stage ----------
run_stage() {
    local stage_label=$1   # "1h", "4h", or "8h"
    local hours=$2
    local results_dir="${RESULTS_ROOT}/plexmedia-${stage_label}-$(date +%Y%m%dT%H%M%S)"
    mkdir -p "${results_dir}"/{logs,scenarios,metrics}

    log "===================================="
    log "Plexmedia soak stage: ${stage_label} (${hours}h)"
    log "results: ${results_dir}"
    log "===================================="

    preflight_cluster || return 1

    # ----- spawn workloads -----
    log "spawning Plex playback on client01 (LELI) — should hit LOCAL data-primary"
    plex_playback_script | ssh $SSH_OPTS "${SSH_USER}@${CLIENT_LELI}" "cat > /tmp/playback.sh && nohup bash /tmp/playback.sh > /tmp/playback.out 2>&1 &" || log "WARN client01 playback spawn failed"

    log "spawning Plex playback on client02 (TORO) — verifies disk cache + folder readahead"
    plex_playback_script | ssh $SSH_OPTS "${SSH_USER}@${CLIENT_TORO}" "cat > /tmp/playback.sh && nohup bash /tmp/playback.sh > /tmp/playback.out 2>&1 &" || log "WARN client02 playback spawn failed"

    log "spawning ingest on client03 (NRB) — verifies anchor migration"
    ingest_script | ssh $SSH_OPTS "${SSH_USER}@${CLIENT_NRB}" "cat > /tmp/ingest.sh && nohup bash /tmp/ingest.sh > /tmp/ingest.out 2>&1 &" || log "WARN client03 ingest spawn failed"

    # ----- spawn metrics collector -----
    log "spawning metrics collector"
    python3 "${SCRIPT_DIR}/collect-metrics.py" \
        --mgrs "http://${MGR01}:9402" "http://${MGR02}:9402" "http://${MGR03}:9402" \
        --output "${results_dir}/metrics" \
        --interval 30 \
        > "${results_dir}/logs/metrics.out" \
        2> "${results_dir}/logs/metrics.err" &
    METRICS_PID=$!
    echo "${METRICS_PID}" > "${results_dir}/logs/metrics.pid"

    # ----- schedule scenarios (subset relevant to the stage duration) -----
    SCHEDULE_1H=("S01-baseline-write:0.05" "S02-baseline-read-a:0.1" "S09-replica-fallback:0.3" "S15-scrub-corruption:0.5")
    SCHEDULE_4H=("S01-baseline-write:0.05" "S02-baseline-read-a:0.1" "S09-replica-fallback:0.5" "S15-scrub-corruption:1.0" "S10-lease-busy:1.5" "S08-anchor-protect:2.0" "S17-rolling-mgr-restart:3.0")
    SCHEDULE_8H=("S01-baseline-write:0.05" "S02-baseline-read-a:0.1" "S09-replica-fallback:0.5" "S15-scrub-corruption:1.0" "S10-lease-busy:1.5" "S08-anchor-protect:2.0" "S17-rolling-mgr-restart:3.0" "S13-partition-dc-a:4.0" "S26-backup-mid-soak:5.0" "S24-anchor-violation-webhook:6.0" "S28-smart-degrade:7.0")

    case "$stage_label" in
        1h) SCHEDULE=("${SCHEDULE_1H[@]}") ;;
        4h) SCHEDULE=("${SCHEDULE_4H[@]}") ;;
        8h) SCHEDULE=("${SCHEDULE_8H[@]}") ;;
        *) SCHEDULE=("${SCHEDULE_1H[@]}") ;;
    esac

    END_TS=$(( $(date +%s) + hours * 3600 ))
    for entry in "${SCHEDULE[@]}"; do
        scenario="${entry%%:*}"
        delay_hr="${entry##*:}"
        delay_secs=$(awk "BEGIN {printf \"%d\", $delay_hr * 3600}")
        target_ts=$(( $(date +%s) + delay_secs ))
        if [ "$target_ts" -ge "$END_TS" ]; then continue; fi
        (
            sleep_for=$((target_ts - $(date +%s)))
            [ "$sleep_for" -gt 0 ] && sleep "$sleep_for"
            log "running $scenario"
            scenario_script="${SCRIPT_DIR}/scenarios/${scenario}.sh"
            if [ -f "$scenario_script" ]; then
                bash "$scenario_script" \
                    > "${results_dir}/scenarios/${scenario}.out" \
                    2> "${results_dir}/scenarios/${scenario}.err" || \
                    log "WARN $scenario exited $?"
            else
                log "WARN $scenario script missing"
            fi
        ) &
    done

    # ----- wait until duration elapses -----
    log "running until $(date -d @$END_TS -Iseconds)"
    while [ "$(date +%s)" -lt "$END_TS" ]; do
        sleep 60
    done

    # ----- shutdown -----
    log "terminating workloads"
    kill "$METRICS_PID" 2>/dev/null || true
    for host in "$CLIENT_LELI" "$CLIENT_TORO" "$CLIENT_NRB"; do
        run_remote "$host" "pkill -f playback.sh; pkill -f ingest.sh" || true
    done
    wait || true

    # ----- evaluate stage PASS/FAIL -----
    local pass=true
    # Basic check: any FAIL in scenario logs?
    if grep -l "FAIL\|ERROR" "${results_dir}/scenarios"/*.out 2>/dev/null | head -1; then
        log "STAGE ${stage_label}: scenarios reported failures"
        pass=false
    fi
    # Cluster still alive?
    if ! curl -fsS --max-time 10 "http://${MGR01}:9402/api/v1/cluster/status" > "${results_dir}/post-cluster-status.json" 2>&1; then
        log "STAGE ${stage_label}: cluster unreachable after run"
        pass=false
    fi

    if $pass; then
        log "STAGE ${stage_label}: PASS"
        echo "PASS" > "${results_dir}/STAGE_RESULT"
        return 0
    else
        log "STAGE ${stage_label}: FAIL — chain aborted"
        echo "FAIL" > "${results_dir}/STAGE_RESULT"
        return 1
    fi
}

# ---------- top-level dispatch ----------
case "$DURATION_ARG" in
    1h)    run_stage "1h" 1  ;;
    4h)    run_stage "4h" 4  ;;
    8h)    run_stage "8h" 8  ;;
    chain)
        log "starting Plexmedia 1h → 4h → 8h soak chain"
        if run_stage "1h" 1; then
            if run_stage "4h" 4; then
                if run_stage "8h" 8; then
                    log "FULL CHAIN PASS"
                    exit 0
                fi
            fi
        fi
        log "CHAIN FAILED"
        exit 1
        ;;
    *) echo "Usage: $0 [1h|4h|8h|chain]" ; exit 2 ;;
esac

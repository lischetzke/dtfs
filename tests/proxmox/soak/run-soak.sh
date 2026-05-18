#!/usr/bin/env bash
# Master soak driver for the 3-DC prod-readiness campaign.
#
# Runs in three concurrent loops:
#   1. Workload generators on each client (background, persistent)
#   2. Scenario injection on a schedule (S13 partition, S15 corrupt, etc.)
#   3. Metrics scraper writing to ./results/<ts>/metrics-*.jsonl
#
# Final phase computes BLAKE3 over the source corpus + FUSE-mounted view
# and writes REPORT.md.

set -euo pipefail

# ---------- defaults ----------
DURATION_HOURS=48
CORPUS_DIR=""
RESULTS_DIR="$(dirname "$0")/results/$(date +%Y%m%dT%H%M%S)"
MGR_LEADER="${MGR_LEADER:-http://10.150.0.151:9402}"

# Hosts (matches inventory-3dc.json)
CLIENT_A=10.150.0.160
CLIENT_B=10.150.0.161
CLIENT_C=10.150.0.162
DATA_B1=10.150.0.156
DATA_A1=10.150.0.154
MGR_01=10.150.0.151
MGR_02=10.150.0.152
SSH_USER=debian
SSH_OPTS="-o StrictHostKeyChecking=no -o LogLevel=ERROR"

# ---------- args ----------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --duration) DURATION_HOURS="${2%h}" ; shift 2 ;;
        --corpus)   CORPUS_DIR="$2"        ; shift 2 ;;
        --results-dir) RESULTS_DIR="$2"    ; shift 2 ;;
        --mgr)      MGR_LEADER="$2"        ; shift 2 ;;
        -h|--help)
            echo "Usage: $0 [--duration NUMh] [--corpus PATH] [--results-dir PATH]"
            exit 0 ;;
        *) echo "unknown arg: $1" >&2; exit 1 ;;
    esac
done

mkdir -p "$RESULTS_DIR"/{logs,metrics,scenarios}
echo "[soak] results dir: $RESULTS_DIR"
echo "[soak] duration   : ${DURATION_HOURS}h"
echo "[soak] corpus     : ${CORPUS_DIR:-none}"
echo "[soak] mgr leader : $MGR_LEADER"

# ---------- preflight ----------
echo "[soak] preflight: cluster status"
if ! curl -fsS "$MGR_LEADER/api/v1/cluster/status" > "$RESULTS_DIR/logs/preflight-status.json"; then
    echo "[soak] ERROR: cluster unreachable at $MGR_LEADER" >&2
    exit 1
fi

# ---------- spawn workloads ----------
spawn_workload() {
    local host=$1
    local label=$2
    local script=$3
    nohup ssh $SSH_OPTS "$SSH_USER@$host" "bash -s" \
        > "$RESULTS_DIR/logs/$label.out" \
        2> "$RESULTS_DIR/logs/$label.err" \
        < "$(dirname "$0")/workloads/$script" &
    echo $! > "$RESULTS_DIR/logs/$label.pid"
    echo "[soak] spawned $label on $host (pid $(cat "$RESULTS_DIR/logs/$label.pid"))"
}

spawn_workload "$CLIENT_A" "wl-client-a" playback-client-a.sh
spawn_workload "$CLIENT_B" "wl-client-b" playback-client-b.sh
spawn_workload "$CLIENT_C" "wl-client-c" playback-client-c.sh

# ---------- spawn metrics collector ----------
python3 "$(dirname "$0")/collect-metrics.py" \
    --mgrs http://${MGR_01}:9402 http://${MGR_02}:9402 \
    --output "$RESULTS_DIR/metrics" \
    --interval 30 \
    > "$RESULTS_DIR/logs/metrics-collector.out" \
    2> "$RESULTS_DIR/logs/metrics-collector.err" &
echo $! > "$RESULTS_DIR/logs/metrics-collector.pid"
echo "[soak] spawned metrics collector"

# ---------- schedule scenarios ----------
SCENARIO_DIR="$(dirname "$0")/scenarios"
END_TS=$(( $(date +%s) + DURATION_HOURS * 3600 ))

# Schedule (relative hours, scenario name)
SCHEDULE=(
    "0:S01-baseline-write"
    "0.5:S02-baseline-read-a"
    "0.5:S03-baseline-read-c"
    "1:S04-folder-readahead"
    "2:S06-tier-promote"
    "4:S09-replica-fallback"
    "6:S10-lease-busy"
    "8:S13-partition-dc-a"
    "12:S24-anchor-violation-webhook"
    "16:S14-disk-full"
    "20:S15-scrub-corruption"
    "24:S26-backup-mid-soak"
    "27:S25-graceful-decommission"
    "30:S17-rolling-mgr-restart"
    "33:S27-cert-rotation"
    "36:S16-clock-skew"
    "39:S28-smart-degrade"
    "44:S08-anchor-protect"
    "46:S20-cross-dc-streaming"
    "47:S07-tier-demote"
)

for entry in "${SCHEDULE[@]}"; do
    delay_hr="${entry%%:*}"
    scenario="${entry#*:}"
    delay_secs=$(awk "BEGIN {printf \"%d\", $delay_hr * 3600}")
    target_ts=$(( $(date +%s) - 0 + delay_secs ))
    if [ "$target_ts" -ge "$END_TS" ]; then
        continue
    fi
    (
        now=$(date +%s)
        sleep_for=$((target_ts - now))
        if [ "$sleep_for" -gt 0 ]; then sleep "$sleep_for"; fi
        echo "[soak] $(date -Iseconds) running $scenario"
        if [ -f "$SCENARIO_DIR/${scenario}.sh" ]; then
            bash "$SCENARIO_DIR/${scenario}.sh" \
                > "$RESULTS_DIR/scenarios/${scenario}.out" \
                2> "$RESULTS_DIR/scenarios/${scenario}.err" || \
                echo "[soak] WARN $scenario exited $?"
        else
            echo "[soak] WARN $scenario: script missing"
        fi
    ) &
done

# ---------- main wait ----------
NOW=$(date +%s)
echo "[soak] running until $(date -d @$END_TS -Iseconds)"
while [ "$NOW" -lt "$END_TS" ]; do
    sleep 60
    NOW=$(date +%s)
done

# ---------- shutdown ----------
echo "[soak] terminating workloads"
for label in wl-client-a wl-client-b wl-client-c metrics-collector; do
    pid_file="$RESULTS_DIR/logs/$label.pid"
    if [ -f "$pid_file" ]; then
        kill "$(cat "$pid_file")" 2>/dev/null || true
    fi
done
wait

# ---------- post-soak ----------
echo "[soak] running post-soak verify"
if [ -n "$CORPUS_DIR" ]; then
    bash "$SCENARIO_DIR/post-verify-blake3.sh" "$CORPUS_DIR" "$RESULTS_DIR" \
        > "$RESULTS_DIR/scenarios/post-verify-blake3.out" \
        2> "$RESULTS_DIR/scenarios/post-verify-blake3.err" || \
        echo "[soak] WARN blake3 verify failed"
fi

echo "[soak] rendering report"
python3 "$(dirname "$0")/render-report.py" \
    --results-dir "$RESULTS_DIR" \
    --output "$RESULTS_DIR/REPORT.md" || \
    echo "[soak] WARN report render failed (see logs)"

echo "[soak] DONE. Report: $RESULTS_DIR/REPORT.md"

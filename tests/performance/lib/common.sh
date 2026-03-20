#!/usr/bin/env bash
# DTFS Performance Test Common Library
# Source this file at the top of every benchmark script:
#   source "$(dirname "$0")/lib/common.sh"

# ---------------------------------------------------------------------------
# Environment configuration
# ---------------------------------------------------------------------------
MANAGER_API="${MANAGER_API:-http://10.0.1.1:9402}"
TEST_MOUNT="${DTFS_TEST_MOUNT:-/mnt/dtfs}"
FIO_BIN="${FIO_BIN:-fio}"
IPERF3_BIN="${IPERF3_BIN:-iperf3}"
DTFS_CLI="${DTFS_CLI_BIN:-/usr/local/bin/dtfs-cli}"

MGR_01="${DTFS_MGR_01:-10.0.1.1}"
MGR_02="${DTFS_MGR_02:-10.0.1.2}"
MGR_03="${DTFS_MGR_03:-10.0.1.3}"
DATA_01="${DTFS_DATA_01:-10.0.2.1}"
DATA_02="${DTFS_DATA_02:-10.0.2.2}"
CLIENT_01="${DTFS_CLIENT_01:-10.0.3.1}"
CLIENT_02="${DTFS_CLIENT_02:-10.0.3.2}"

SSH_KEY="${DTFS_SSH_KEY:-$HOME/.ssh/dtfs_test_key}"
SSH_USER="${DTFS_SSH_USER:-dtfs-admin}"

# ---------------------------------------------------------------------------
# Results directory — timestamped per run
# ---------------------------------------------------------------------------
_PERF_RUN_TS="${DTFS_PERF_TS:-$(date '+%Y-%m-%d_%H-%M')}"
RESULTS_DIR="${DTFS_RESULTS_DIR:-$(dirname "$0")/results/${_PERF_RUN_TS}}"
mkdir -p "$RESULTS_DIR"

SUMMARY_JSON="${RESULTS_DIR}/summary.json"
SUITE_NAME="${PERF_SUITE_NAME:-unknown}"

# Initialise the summary JSON file if it does not exist
if [ ! -f "$SUMMARY_JSON" ]; then
    echo '{"run_timestamp":"'"$_PERF_RUN_TS"'","results":[]}' > "$SUMMARY_JSON"
fi

# ---------------------------------------------------------------------------
# Color codes (same as integration lib)
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
    COLOR_RESET=""; COLOR_GREEN=""; COLOR_RED=""
    COLOR_YELLOW=""; COLOR_BLUE=""; COLOR_CYAN=""; COLOR_BOLD=""
fi

_ts() { date '+%Y-%m-%dT%H:%M:%S'; }

log_info()  { echo -e "${COLOR_BLUE}[INFO]${COLOR_RESET}  $(_ts) $*" >&2; }
log_warn()  { echo -e "${COLOR_YELLOW}[WARN]${COLOR_RESET}  $(_ts) $*" >&2; }
log_ok()    { echo -e "${COLOR_GREEN}[PASS]${COLOR_RESET}  $(_ts) $*" >&2; }
log_fail()  { echo -e "${COLOR_RED}[FAIL]${COLOR_RESET}  $(_ts) $*" >&2; }
log_debug() {
    [ "${DTFS_DEBUG:-0}" = "1" ] && echo "[DEBUG] $(_ts) $*" >&2 || true
}
log_section() {
    echo -e "" >&2
    echo -e "${COLOR_BOLD}${COLOR_CYAN}=== $* ===${COLOR_RESET}" >&2
}

# ---------------------------------------------------------------------------
# SSH helper (mirrors integration lib)
# ---------------------------------------------------------------------------
ssh_exec() {
    local host="$1"; shift
    local cmd="$*"
    ssh -i "$SSH_KEY" \
        -o StrictHostKeyChecking=no \
        -o UserKnownHostsFile=/dev/null \
        -o ConnectTimeout=10 \
        -o BatchMode=yes \
        "${SSH_USER}@${host}" \
        "sudo bash -c $(printf '%q' "$cmd")"
}

# ---------------------------------------------------------------------------
# perf_header <suite_name>
# Print a header banner for a benchmark suite.
# ---------------------------------------------------------------------------
perf_header() {
    local name="$1"
    SUITE_NAME="$name"
    echo -e "" >&2
    echo -e "${COLOR_BOLD}${COLOR_BLUE}############################################${COLOR_RESET}" >&2
    echo -e "${COLOR_BOLD}${COLOR_BLUE}  DTFS Performance Benchmark: ${name}${COLOR_RESET}" >&2
    echo -e "${COLOR_BOLD}${COLOR_BLUE}  Results dir: ${RESULTS_DIR}${COLOR_RESET}" >&2
    echo -e "${COLOR_BOLD}${COLOR_BLUE}############################################${COLOR_RESET}" >&2
    echo -e "" >&2
}

# ---------------------------------------------------------------------------
# perf_result <metric> <value> <unit>
# Record a single metric result to stdout and to the RESULTS_DIR JSON file.
# ---------------------------------------------------------------------------
perf_result() {
    local metric="$1"
    local value="$2"
    local unit="$3"

    printf "  %-45s %12s %s\n" "$metric" "$value" "$unit" >&1

    # Append to per-suite JSON file
    local suite_json="${RESULTS_DIR}/${SUITE_NAME}.json"
    if [ ! -f "$suite_json" ]; then
        echo '{"suite":"'"$SUITE_NAME"'","timestamp":"'"$_PERF_RUN_TS"'","metrics":[]}' > "$suite_json"
    fi

    # Use python3 for JSON manipulation (jq in-place editing is messy)
    python3 - <<PYEOF 2>/dev/null || true
import json, sys

path = "$suite_json"
with open(path) as f:
    data = json.load(f)

data.setdefault("metrics", []).append({
    "metric": "$metric",
    "value":  "$value",
    "unit":   "$unit",
    "ts":     "$(_ts)"
})

with open(path, "w") as f:
    json.dump(data, f, indent=2)
PYEOF

    # Also update global summary
    python3 - <<PYEOF 2>/dev/null || true
import json, sys

path = "$SUMMARY_JSON"
try:
    with open(path) as f:
        data = json.load(f)
except Exception:
    data = {"run_timestamp": "$_PERF_RUN_TS", "results": []}

data.setdefault("results", []).append({
    "suite":  "$SUITE_NAME",
    "metric": "$metric",
    "value":  "$value",
    "unit":   "$unit"
})

with open(path, "w") as f:
    json.dump(data, f, indent=2)
PYEOF
}

# ---------------------------------------------------------------------------
# run_fio <args...>
# Run fio with the given arguments. Outputs fio JSON to stdout.
# Example: run_fio --name=seqwrite --filename=/mnt/dtfs/test.dat ...
# ---------------------------------------------------------------------------
run_fio() {
    "$FIO_BIN" --output-format=json "$@"
}

# ---------------------------------------------------------------------------
# run_fio_remote <host> <args...>
# Run fio on a remote host, return fio JSON output.
# ---------------------------------------------------------------------------
run_fio_remote() {
    local host="$1"; shift
    ssh_exec "$host" "fio --output-format=json $*"
}

# ---------------------------------------------------------------------------
# parse_fio_bw <fio_json>
# Extract aggregate read+write bandwidth in MiB/s from fio JSON output.
# ---------------------------------------------------------------------------
parse_fio_bw() {
    local fio_json="$1"
    # fio JSON: jobs[0].read.bw + jobs[0].write.bw (in KiB/s → convert to MiB/s)
    python3 - <<PYEOF 2>/dev/null || echo "0"
import json, sys

try:
    data = json.loads('''$fio_json''')
    total_bw_kib = 0
    for job in data.get("jobs", []):
        total_bw_kib += job.get("read",  {}).get("bw", 0)
        total_bw_kib += job.get("write", {}).get("bw", 0)
    mib = total_bw_kib / 1024.0
    print(f"{mib:.2f}")
except Exception as e:
    print("0")
PYEOF
}

# ---------------------------------------------------------------------------
# parse_fio_iops <fio_json>
# Extract aggregate IOPS (read + write) from fio JSON output.
# ---------------------------------------------------------------------------
parse_fio_iops() {
    local fio_json="$1"
    python3 - <<PYEOF 2>/dev/null || echo "0"
import json, sys

try:
    data = json.loads('''$fio_json''')
    total_iops = 0
    for job in data.get("jobs", []):
        total_iops += job.get("read",  {}).get("iops", 0)
        total_iops += job.get("write", {}).get("iops", 0)
    print(f"{total_iops:.0f}")
except Exception:
    print("0")
PYEOF
}

# ---------------------------------------------------------------------------
# parse_fio_lat_p50 <fio_json>
# Extract p50 latency in microseconds from fio JSON (prefers read, then write).
# ---------------------------------------------------------------------------
parse_fio_lat_p50() {
    local fio_json="$1"
    python3 - <<PYEOF 2>/dev/null || echo "0"
import json, sys

try:
    data = json.loads('''$fio_json''')
    job = data.get("jobs", [{}])[0]
    # Try read lat first, then write
    for rw in ("read", "write"):
        lat = job.get(rw, {}).get("clat_ns", {}).get("percentile", {}).get("50.000000", 0)
        if lat:
            print(f"{lat/1000:.1f}")  # ns -> us
            break
    else:
        print("0")
except Exception:
    print("0")
PYEOF
}

# ---------------------------------------------------------------------------
# parse_fio_lat_p99 <fio_json>
# Extract p99 latency in microseconds from fio JSON.
# ---------------------------------------------------------------------------
parse_fio_lat_p99() {
    local fio_json="$1"
    python3 - <<PYEOF 2>/dev/null || echo "0"
import json, sys

try:
    data = json.loads('''$fio_json''')
    job = data.get("jobs", [{}])[0]
    for rw in ("read", "write"):
        lat = job.get(rw, {}).get("clat_ns", {}).get("percentile", {}).get("99.000000", 0)
        if lat:
            print(f"{lat/1000:.1f}")  # ns -> us
            break
    else:
        print("0")
except Exception:
    print("0")
PYEOF
}

# ---------------------------------------------------------------------------
# Initialization
# ---------------------------------------------------------------------------
log_info "Performance lib loaded. Results dir: $RESULTS_DIR"

#!/usr/bin/env bash
# Smoke test driver — runs the critical scenarios in ~10 minutes against
# the deployed 3-DC lab. Not a substitute for the 48h soak, but proves
# the P0 fixes work in a real cluster.

set -uo pipefail

RESULTS_DIR="${RESULTS_DIR:-/tmp/dtfs-smoke-$(date +%Y%m%dT%H%M)}"
mkdir -p "$RESULTS_DIR/scenarios" "$RESULTS_DIR/logs"

MGR="http://10.150.0.151:9402"
export MGR CLIENT_A=10.150.0.160 CLIENT_B=10.150.0.161 CLIENT_C=10.150.0.162
export DATA_B1=10.150.0.156 DATA_A1=10.150.0.154 SSH_USER=debian
SCEN_DIR="$(dirname "$0")/scenarios"

run() {
    local name=$1
    echo ""
    echo "=== running $name ==="
    bash "$SCEN_DIR/${name}.sh" \
        > "$RESULTS_DIR/scenarios/${name}.out" \
        2> "$RESULTS_DIR/scenarios/${name}.err"
    local rc=$?
    local verdict=$(grep -m1 -oE "verdict=(pass|fail|warn)" "$RESULTS_DIR/scenarios/${name}.out" || echo "verdict=unknown")
    echo "  $name → $verdict (rc=$rc)"
}

echo "=== preflight ==="
curl -fsS "$MGR/healthz" || { echo "FAIL: /healthz unreachable"; exit 1; }
curl -fsS "$MGR/readyz" || { echo "FAIL: /readyz unreachable"; exit 1; }
echo "  cluster live"
curl -fsS "$MGR/api/v1/nodes" | python3 -c "import sys, json; nodes=json.load(sys.stdin); print(f'  {len(nodes)} data nodes registered')" 2>/dev/null || true

bash "$SCEN_DIR/../bootstrap-cluster.sh" > "$RESULTS_DIR/logs/bootstrap.out" 2>&1 || \
    echo "  bootstrap warnings; continuing"

run S01-baseline-write
run S02-baseline-read-a
run S03-baseline-read-c
run S09-replica-fallback
run S10-lease-busy
run S17-rolling-mgr-restart

echo ""
echo "=== smoke complete ==="
echo "results: $RESULTS_DIR"

# Summary
echo ""
echo "=== verdicts ==="
for f in "$RESULTS_DIR"/scenarios/*.out; do
    name=$(basename "$f" .out)
    v=$(grep -m1 -oE "verdict=(pass|fail|warn)" "$f" || echo "verdict=unknown")
    echo "  $name → $v"
done

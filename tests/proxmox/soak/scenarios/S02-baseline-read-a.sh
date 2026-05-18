#!/usr/bin/env bash
# S02 baseline read from client-a (DSL DC) — should sustain ~40 Mbps egress.
set -uo pipefail
CLIENT_A="${CLIENT_A:-10.150.0.160}"
SSH_USER="${SSH_USER:-debian}"
SSH_OPTS="-o StrictHostKeyChecking=no -o LogLevel=ERROR"
FILE=/mnt/dtfs/S01-baseline-1g.dat

echo "=== S02 baseline read from DC-A ==="
ts0=$(date +%s)
ssh $SSH_OPTS "$SSH_USER@$CLIENT_A" "cat $FILE > /dev/null"
rc=$?
ts1=$(date +%s)
dur=$((ts1 - ts0))
echo "duration: ${dur}s for 1 GB"
if [ $rc -ne 0 ]; then
    echo "verdict=fail: read errored"
    exit 1
fi
echo "verdict=pass: read complete (rc=$rc, dur=${dur}s)"

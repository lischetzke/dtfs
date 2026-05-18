#!/usr/bin/env bash
# S03 baseline read from client-c (1G symmetric) — should sustain ~100 MB/s.
set -uo pipefail
CLIENT_C="${CLIENT_C:-10.150.0.162}"
SSH_USER="${SSH_USER:-debian}"
SSH_OPTS="-o StrictHostKeyChecking=no -o LogLevel=ERROR"
FILE=/mnt/dtfs/S01-baseline-1g.dat

echo "=== S03 baseline read from DC-C ==="
ts0=$(date +%s)
ssh $SSH_OPTS "$SSH_USER@$CLIENT_C" "cat $FILE > /dev/null"
rc=$?
ts1=$(date +%s)
dur=$((ts1 - ts0))
echo "duration: ${dur}s for 1 GB"
if [ $rc -ne 0 ]; then
    echo "verdict=fail: read errored"
    exit 1
fi
echo "verdict=pass: read complete (rc=$rc, dur=${dur}s)"

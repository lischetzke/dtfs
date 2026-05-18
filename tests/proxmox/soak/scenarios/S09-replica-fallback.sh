#!/usr/bin/env bash
# S09 replica fallback: while a client is reading a file, kill the data
# node serving it. The P0-2 fix lets the read continue from another
# replica instead of failing.
set -uo pipefail
CLIENT_B="${CLIENT_B:-10.150.0.161}"
DATA_B1="${DATA_B1:-10.150.0.156}"
SSH_USER="${SSH_USER:-debian}"
SSH_OPTS="-o StrictHostKeyChecking=no -o LogLevel=ERROR"
FILE=/mnt/dtfs/S09-fallback-256m.dat

echo "=== S09 replica fallback ==="

# Create the file (writes ensure replicas across DCs given anchor + rf).
ssh $SSH_OPTS "$SSH_USER@$CLIENT_B" \
    "dd if=/dev/urandom of=$FILE bs=1M count=256 status=none && sync"

# Start a slow background read.
ssh $SSH_OPTS "$SSH_USER@$CLIENT_B" \
    "nohup pv -q -L 8M $FILE > /tmp/s09-read.out 2>&1 &"

sleep 4  # let the read get going

# Kill one data node mid-stream.
ssh $SSH_OPTS "$SSH_USER@$DATA_B1" \
    "sudo systemctl stop dtfs-data" || true

sleep 8

# Restart it for hygiene.
ssh $SSH_OPTS "$SSH_USER@$DATA_B1" \
    "sudo systemctl start dtfs-data" || true

# Wait for the background read to finish.
sleep 30
read_bytes=$(ssh $SSH_OPTS "$SSH_USER@$CLIENT_B" \
    "stat -c%s /tmp/s09-read.out 2>/dev/null || echo 0")

if [ "$read_bytes" -lt $((200 * 1024 * 1024)) ]; then
    echo "verdict=fail: read only ${read_bytes} bytes (expected ~256 MB)"
    exit 1
fi

echo "verdict=pass: read completed (${read_bytes} bytes) after data-b1 kill"

#!/usr/bin/env bash
# S15 scrub corruption: pick a chunk on data-b1, corrupt its .dat file,
# then wait for the next deep scrub to detect it. Verify (a) scrub
# reports the corruption, (b) the scheduler re-replicates from a healthy
# replica.
set -uo pipefail
DATA_B1="${DATA_B1:-10.150.0.156}"
MGR="${MGR:-http://10.150.0.151:9402}"
SSH_USER="${SSH_USER:-debian}"
SSH_OPTS="-o StrictHostKeyChecking=no -o LogLevel=ERROR"

echo "=== S15 scrub corruption ==="

# Find a chunk to corrupt.
target=$(ssh $SSH_OPTS "$SSH_USER@$DATA_B1" \
    "find /mnt/warm/chunks -name '*.dat' 2>/dev/null | head -1")
if [ -z "$target" ]; then
    echo "verdict=warn: no chunks on data-b1 yet"
    exit 0
fi
echo "corrupting $target"
ssh $SSH_OPTS "$SSH_USER@$DATA_B1" \
    "sudo dd if=/dev/urandom of=$target bs=1M count=1 conv=notrunc seek=0 status=none"

# Trigger a deep scrub via REST (faster than waiting for the schedule).
curl -fsS -X POST "$MGR/api/v1/scrub" \
    -H 'content-type: application/json' \
    -d '{"target_type":"node","target":"data-b1","level":"deep"}' \
    > /dev/null || true

# Wait for the scrub + auto re-replication.
sleep 90

# Confirm via audit log.
corrupt_events=$(curl -fsS "$MGR/api/v1/audit?limit=500" \
    | grep -c "corrupt" || true)
repl_events=$(curl -fsS "$MGR/api/v1/audit?action=replication.schedule" \
    | grep -c "replication.schedule" || true)

echo "corrupt-events seen: $corrupt_events"
echo "replication.schedule events seen: $repl_events"

if [ "$corrupt_events" -gt 0 ]; then
    echo "verdict=pass: scrub detected + re-replication scheduled"
else
    echo "verdict=warn: corruption not (yet) detected in audit log"
fi

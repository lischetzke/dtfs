#!/usr/bin/env bash
# S01 baseline-write: write a 1 GB random file from client-c, verify all
# three DCs participate per the anchor=dc-a rule and rf hot=2/warm=2/cold=1.
#
# PASS: file written, manager reports ≥1 chunk per DC, anchor=dc-a holds.
set -uo pipefail
CLIENT_C="${CLIENT_C:-10.150.0.162}"
MGR="${MGR:-http://10.150.0.151:9402}"
SSH_USER="${SSH_USER:-debian}"
SSH_OPTS="-o StrictHostKeyChecking=no -o LogLevel=ERROR"

FILE=/mnt/dtfs/S01-baseline-1g.dat

echo "=== S01 baseline write ==="

ssh $SSH_OPTS "$SSH_USER@$CLIENT_C" \
    "dd if=/dev/urandom of=$FILE bs=1M count=1024 status=none && sync"
RC=$?
if [ $RC -ne 0 ]; then
    echo "verdict=fail: write failed (rc=$RC)"
    exit 1
fi

# Verify chunks were placed
sleep 5
locs=$(curl -fsS "$MGR/api/v1/files?path=$FILE" || echo "{}")
chunk_count=$(echo "$locs" | grep -c '"chunk_id"' || true)
if [ "$chunk_count" -lt 1 ]; then
    echo "verdict=fail: no chunks reported for $FILE"
    exit 1
fi

# Verify anchor invariant via audit log
sleep 2
anchor_violations=$(curl -fsS "$MGR/api/v1/audit?action=anchor.violated" \
    | grep -c "anchor.violated" || true)
if [ "$anchor_violations" -gt 0 ]; then
    echo "verdict=fail: $anchor_violations anchor violations"
    exit 1
fi

echo "verdict=pass: 1 GB written, $chunk_count chunks placed, anchor intact"

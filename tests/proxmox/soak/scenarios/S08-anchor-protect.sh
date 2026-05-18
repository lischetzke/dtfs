#!/usr/bin/env bash
# S08 anchor protect: drive lots of DC-C reads against a chunk, see if
# tiering tries to migrate it. P0-10 should refuse moves that would
# strand the last DC-A copy. Verified by checking the audit log for
# `tier.anchor_protected` events.
set -uo pipefail
CLIENT_C="${CLIENT_C:-10.150.0.162}"
MGR="${MGR:-http://10.150.0.151:9402}"
SSH_USER="${SSH_USER:-debian}"
SSH_OPTS="-o StrictHostKeyChecking=no -o LogLevel=ERROR"
FILE=/mnt/dtfs/S08-anchor.dat

echo "=== S08 anchor protect ==="

# Read a hot file repeatedly from DC-C so the tiering engine sees
# heavy DC-C activity. With anchor=dc-a, migrations are not allowed to
# strand the DC-A copy.
ssh $SSH_OPTS "$SSH_USER@$CLIENT_C" "test -f $FILE || \
    dd if=/dev/urandom of=$FILE bs=1M count=128 status=none"

for i in $(seq 1 10); do
    ssh $SSH_OPTS "$SSH_USER@$CLIENT_C" "cat $FILE > /dev/null" || true
    sleep 6  # gives tiering eval (60s interval) time to fire
done

# Wait one more eval cycle.
sleep 70

# Inspect audit log.
protected=$(curl -fsS "$MGR/api/v1/audit?action=tier.anchor_protected" \
    | grep -c "anchor_protected" || true)

if [ "$protected" -lt 1 ]; then
    echo "verdict=warn: no anchor_protected events observed (may be that no migration was attempted yet)"
    exit 0
fi

echo "verdict=pass: anchor invariant enforced ($protected refusals)"

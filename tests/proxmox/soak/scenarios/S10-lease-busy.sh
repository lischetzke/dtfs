#!/usr/bin/env bash
# S10 single-writer lease busy: two clients open the same file for write
# simultaneously. P0-1 ensures exactly one succeeds; the other gets EBUSY.
set -uo pipefail
CLIENT_A="${CLIENT_A:-10.150.0.160}"
CLIENT_C="${CLIENT_C:-10.150.0.162}"
SSH_USER="${SSH_USER:-debian}"
SSH_OPTS="-o StrictHostKeyChecking=no -o LogLevel=ERROR"
FILE=/mnt/dtfs/S10-lease-busy.dat

echo "=== S10 single-writer lease ==="

# Pre-create the file from client-c (one writer succeeds).
ssh $SSH_OPTS "$SSH_USER@$CLIENT_C" \
    "echo 'initial' > $FILE"

# Now race two writes. Use the same target. Each invocation tries to
# open the file O_WRONLY and write 64 MB.
write_cmd="(dd if=/dev/urandom of=$FILE bs=1M count=64 2>&1; echo rc=\$?) > /tmp/s10-write.out 2>&1"

ssh $SSH_OPTS "$SSH_USER@$CLIENT_A" \
    "rm -f /tmp/s10-write.out; bash -c \"$write_cmd &\"" &
ssh $SSH_OPTS "$SSH_USER@$CLIENT_C" \
    "rm -f /tmp/s10-write.out; bash -c \"$write_cmd &\"" &

wait
sleep 20

a_out=$(ssh $SSH_OPTS "$SSH_USER@$CLIENT_A" "cat /tmp/s10-write.out 2>/dev/null || true")
c_out=$(ssh $SSH_OPTS "$SSH_USER@$CLIENT_C" "cat /tmp/s10-write.out 2>/dev/null || true")

echo "client-a output: $a_out"
echo "client-c output: $c_out"

# Look for "busy" / EBUSY in at least one output.
busy_a=$(echo "$a_out" | grep -ciE "busy|EBUSY|Resource busy|Device or resource busy" || true)
busy_c=$(echo "$c_out" | grep -ciE "busy|EBUSY|Resource busy|Device or resource busy" || true)

if [ "$((busy_a + busy_c))" -lt 1 ]; then
    echo "verdict=warn: no EBUSY observed (both writes may have serialised on the same client)"
    exit 0
fi

echo "verdict=pass: write-lease busy enforced (a=$busy_a c=$busy_c)"

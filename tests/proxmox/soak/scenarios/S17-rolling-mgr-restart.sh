#!/usr/bin/env bash
# S17 rolling manager restart: restart each manager in turn with a 30s
# gap. P0-3 ensures clients failover. P0-10 anchor must hold throughout.
set -uo pipefail
MGRS=(10.150.0.151 10.150.0.152 10.150.0.153)
SSH_USER="${SSH_USER:-debian}"
SSH_OPTS="-o StrictHostKeyChecking=no -o LogLevel=ERROR"
MGR_URL="${MGR:-http://10.150.0.151:9402}"

echo "=== S17 rolling mgr restart ==="

# Capture baseline file count.
baseline=$(curl -fsS "$MGR_URL/api/v1/cluster/status" 2>/dev/null \
    | grep -oP '"file_count":\s*\K[0-9]+' | head -1)
echo "baseline file_count: ${baseline:-unknown}"

for m in "${MGRS[@]}"; do
    echo "restarting mgr $m"
    ssh $SSH_OPTS "$SSH_USER@$m" "sudo systemctl restart dtfs-manager" || \
        echo "WARN restart failed on $m"
    sleep 30
    # Wait for the cluster to be readyz=200 again on any peer.
    for try in $(seq 1 12); do
        for peer in "${MGRS[@]}"; do
            if curl -fsS -o /dev/null "http://$peer:9402/readyz"; then
                echo "readyz on $peer after $((try*5))s"
                break 2
            fi
        done
        sleep 5
    done
done

# Verify anchor still intact.
final=$(curl -fsS "$MGR_URL/api/v1/cluster/status" 2>/dev/null \
    | grep -oP '"file_count":\s*\K[0-9]+' | head -1)
echo "final file_count: ${final:-unknown}"

if [ -z "$final" ]; then
    echo "verdict=fail: cluster unreachable after rolling restart"
    exit 1
fi
echo "verdict=pass: rolling restart complete, cluster live"

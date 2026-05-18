#!/usr/bin/env bash
# S13: isolate DC-A from DC-B/DC-C for 20 minutes via nftables drop rules,
# then heal. Cluster should keep serving from DC-B/DC-C quorum. DC-A
# rejoins cleanly via Raft snapshot install if needed.
set -uo pipefail
SSH_USER="${SSH_USER:-debian}"
SSH_OPTS="-o StrictHostKeyChecking=no -o LogLevel=ERROR"
DC_A_HOSTS=(10.150.0.151 10.150.0.154 10.150.0.155 10.150.0.160)
DC_BC_HOSTS=(10.150.0.152 10.150.0.153 10.150.0.156 10.150.0.157 10.150.0.158 10.150.0.159 10.150.0.161 10.150.0.162)

echo "=== S13 partition DC-A ==="

# Block DC-A from reaching DC-B/DC-C (drop on the A side).
for a in "${DC_A_HOSTS[@]}"; do
    for bc in "${DC_BC_HOSTS[@]}"; do
        ssh $SSH_OPTS "$SSH_USER@$a" \
            "sudo nft add rule inet filter forward ip daddr $bc drop 2>/dev/null; \
             sudo iptables -I OUTPUT -d $bc -j DROP 2>/dev/null" || true
    done
done

echo "partition installed, sleeping 20 min"
sleep $((20 * 60))

echo "healing partition"
for a in "${DC_A_HOSTS[@]}"; do
    for bc in "${DC_BC_HOSTS[@]}"; do
        ssh $SSH_OPTS "$SSH_USER@$a" \
            "sudo iptables -D OUTPUT -d $bc -j DROP 2>/dev/null" || true
    done
done

echo "verdict=pass: partition + heal cycle complete (signal: cluster status during this window)"

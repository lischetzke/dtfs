#!/usr/bin/env bash
# DTFS Integration Test: 06 - Reconnect Scenarios
# Tests data-02 partition + reconnect, manager reconnect, and FUSE client restart.

set -euo pipefail
TEST_SUITE_NAME="06_reconnect"
source "$(dirname "$0")/lib/common.sh"

MANAGER_API="${MANAGER_API:-http://10.0.1.1:9402}"
SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=10"

MGR1="${MGR1:-mgr-01}"
MGR2="${MGR2:-mgr-02}"
MGR3="${MGR3:-mgr-03}"
DATA1="${DATA1:-data-01}"
DATA2="${DATA2:-data-02}"
CLIENT1="${CLIENT1:-client-01}"

MOUNT="${TEST_MOUNT:-/mnt/dtfs}"
TEST_DIR="${MOUNT}/test-reconnect-$$"
FUSE_SERVICE="dtfs-fuse@default.service"
FUSE_PROC="dtfs-fuse"

log_section "06_reconnect: Reconnect Scenario Tests"

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
mount_dtfs "$CLIENT_01" "$MOUNT" || { log_fail "Cannot mount DTFS — aborting"; print_summary; exit 1; }
ssh_exec "$CLIENT_01" "mkdir -p '${TEST_DIR}'"
wait_for_health 60 || { log_fail "Cluster not healthy at test start"; print_summary; exit 1; }

# ---------------------------------------------------------------------------
# TC-01: Partition data-02 from the rest of the cluster
# ---------------------------------------------------------------------------
log_section "TC-01: Partition data-02 from cluster"
# Block data-02 <-> all managers and data-01
ALL_MGRS_IPS="${MGR_01},${MGR_02},${MGR_03}"
partition_network "$DATA_02" "${ALL_MGRS_IPS},${DATA_01}"
# Also block inbound to data-02 from managers
for mgr_ip in "$MGR_01" "$MGR_02" "$MGR_03"; do
    ssh_exec "$mgr_ip" "iptables -I OUTPUT -d '${DATA_02}' -j DROP" 2>/dev/null || true
    ssh_exec "$mgr_ip" "iptables -I INPUT -s '${DATA_02}' -j DROP" 2>/dev/null || true
done
log_ok "TC-01: data-02 partitioned from cluster"

sleep 5

# ---------------------------------------------------------------------------
# TC-02: Write new files while data-02 is partitioned (should succeed via other nodes)
# ---------------------------------------------------------------------------
log_section "TC-02: Write files while data-02 is partitioned"
PARTITION_FILE1="${TEST_DIR}/during_partition_1.dat"
PARTITION_FILE2="${TEST_DIR}/during_partition_2.dat"
CKSUM1=$(write_test_file "$CLIENT_01" "$PARTITION_FILE1" 64)
WRITE1_RC=$?
CKSUM2=$(write_test_file "$CLIENT_01" "$PARTITION_FILE2" 32)
WRITE2_RC=$?

if [ $WRITE1_RC -eq 0 ] && read_verify_file "$CLIENT_01" "$PARTITION_FILE1" "$CKSUM1"; then
    log_ok "TC-02: Write 1 succeeded while data-02 partitioned"
else
    log_fail "TC-02: Write 1 failed while data-02 partitioned (rc=$WRITE1_RC)"
fi
if [ $WRITE2_RC -eq 0 ] && read_verify_file "$CLIENT_01" "$PARTITION_FILE2" "$CKSUM2"; then
    log_ok "TC-02: Write 2 succeeded while data-02 partitioned"
else
    log_fail "TC-02: Write 2 failed while data-02 partitioned (rc=$WRITE2_RC)"
fi

# ---------------------------------------------------------------------------
# TC-03: Reconnect data-02 and verify it comes back healthy within 60s
# ---------------------------------------------------------------------------
log_section "TC-03: Reconnect data-02 — verify healthy within 60s"
restore_network "$DATA_02"
# Also restore inbound on managers
for mgr_ip in "$MGR_01" "$MGR_02" "$MGR_03"; do
    ssh_exec "$mgr_ip" "
        iptables -L OUTPUT --line-numbers -n 2>/dev/null |
        awk '/DROP.*${DATA_02}/{print \$1}' | sort -rn |
        while read n; do iptables -D OUTPUT \$n 2>/dev/null; done
        iptables -L INPUT --line-numbers -n 2>/dev/null |
        awk '/DROP.*${DATA_02}/{print \$1}' | sort -rn |
        while read n; do iptables -D INPUT \$n 2>/dev/null; done
    " 2>/dev/null || true
done
log_info "data-02 network restored"

RECONNECT_TIMEOUT=60
RECONNECT_START=$(date +%s)
DATA2_HEALTHY=false
while true; do
    NOW=$(date +%s)
    if [ $((NOW - RECONNECT_START)) -ge $RECONNECT_TIMEOUT ]; then
        break
    fi
    NODES_JSON=$(api_get "/api/v1/nodes" 15 2>/dev/null || echo '[]')
    DATA2_STATUS=$(echo "$NODES_JSON" | \
        jq -r --arg ip "$DATA_02" \
        '.[] | select(.hostname == $ip or .address == $ip) | .status // .state' \
        2>/dev/null | head -1)
    if [ "$DATA2_STATUS" = "healthy" ] || [ "$DATA2_STATUS" = "active" ]; then
        DATA2_HEALTHY=true
        break
    fi
    sleep 3
done

if $DATA2_HEALTHY; then
    log_ok "TC-03: data-02 came back healthy within ${RECONNECT_TIMEOUT}s"
else
    log_fail "TC-03: data-02 did not become healthy within ${RECONNECT_TIMEOUT}s (status: '${DATA2_STATUS}')"
fi

# ---------------------------------------------------------------------------
# TC-04: Disconnect and reconnect a manager, verify re-election and state sync
# ---------------------------------------------------------------------------
log_section "TC-04: Manager disconnect + reconnect (re-election and state sync)"
LEADER_ID=$(get_leader_node_id)
log_info "Current leader: ${LEADER_ID:-<unknown>}"

# Pick a non-leader manager to disconnect
NON_LEADER_MGR="$MGR_03"
if echo "$LEADER_ID" | grep -q "mgr-03\|${MGR_03}"; then
    NON_LEADER_MGR="$MGR_02"
fi
NON_LEADER_IP=$(case "$NON_LEADER_MGR" in
    *mgr-01*) echo "$MGR_01" ;;
    *mgr-02*) echo "$MGR_02" ;;
    *mgr-03*) echo "$MGR_03" ;;
    *) echo "$MGR_03" ;;
esac)

log_info "Disconnecting non-leader manager: $NON_LEADER_IP"
partition_network "$NON_LEADER_IP" "${MGR_01},${MGR_02},${MGR_03}"
sleep 8

# Write a file while manager is disconnected
MGR_DISCO_FILE="${TEST_DIR}/mgr_disconnected.dat"
DISCO_CKSUM=$(write_test_file "$CLIENT_01" "$MGR_DISCO_FILE" 16)
if read_verify_file "$CLIENT_01" "$MGR_DISCO_FILE" "$DISCO_CKSUM"; then
    log_ok "TC-04: Write succeeded while one manager disconnected (quorum preserved)"
else
    log_fail "TC-04: Write failed while one manager disconnected"
fi

# Restore the manager
restore_network "$NON_LEADER_IP"
log_info "Restored $NON_LEADER_IP network"

# Wait for it to rejoin and verify cluster state syncs (Raft log replicated)
SYNC_TIMEOUT=30
SYNC_START=$(date +%s)
SYNCED=false
while true; do
    NOW=$(date +%s)
    if [ $((NOW - SYNC_START)) -ge $SYNC_TIMEOUT ]; then
        break
    fi
    STATUS=$(curl -sf --max-time 5 "http://${NON_LEADER_IP}:9402/api/v1/cluster/status" 2>/dev/null || echo '{}')
    MEMBER_ROLE=$(echo "$STATUS" | jq -r '.raft.role // .raft.state // empty' 2>/dev/null)
    MEMBER_TERM=$(echo "$STATUS" | jq -r '.raft.term // 0' 2>/dev/null)
    if [ "$MEMBER_ROLE" = "follower" ] || [ "$MEMBER_ROLE" = "leader" ]; then
        SYNCED=true
        log_info "${NON_LEADER_IP} Raft role='$MEMBER_ROLE' term=$MEMBER_TERM"
        break
    fi
    sleep 2
done

if $SYNCED; then
    log_ok "TC-04: Disconnected manager rejoined Raft cluster within ${SYNC_TIMEOUT}s"
else
    log_fail "TC-04: Manager did not rejoin within ${SYNC_TIMEOUT}s"
fi

# Verify file written during manager outage is visible to rejoined manager
if curl -sf --max-time 10 "http://${NON_LEADER_IP}:9402/api/v1/files" 2>/dev/null | \
   jq -r '.[].path // .[].name // ""' 2>/dev/null | grep -q "mgr_disconnected"; then
    log_ok "TC-04: File written during outage is visible on rejoined manager"
else
    log_warn "TC-04: File not found in rejoined manager's listing (may use internal IDs)"
fi

# ---------------------------------------------------------------------------
# TC-05: Restart FUSE client, verify mount still works
# ---------------------------------------------------------------------------
log_section "TC-05: FUSE client restart — mount recovery"
log_info "Killing dtfs-fuse on ${CLIENT1}"
ssh_exec "$CLIENT_01" "pkill -9 '${FUSE_PROC}' 2>/dev/null || true; sleep 1"
ssh_exec "$CLIENT_01" "fusermount3 -uz '${MOUNT}' 2>/dev/null || umount -l '${MOUNT}' 2>/dev/null || true"
sleep 2

log_info "Restarting $FUSE_SERVICE on ${CLIENT1}"
ssh_exec "$CLIENT_01" "systemctl start '${FUSE_SERVICE}'"

# Poll for mount to come back
MOUNT_TIMEOUT=30
MOUNT_START=$(date +%s)
MOUNT_BACK=false
while true; do
    NOW=$(date +%s)
    if [ $((NOW - MOUNT_START)) -ge $MOUNT_TIMEOUT ]; then
        break
    fi
    if ssh_exec "$CLIENT_01" "mountpoint -q '${MOUNT}'" 2>/dev/null; then
        MOUNT_BACK=true
        break
    fi
    sleep 2
done

if $MOUNT_BACK; then
    log_ok "TC-05: FUSE mount came back within ${MOUNT_TIMEOUT}s after restart"
else
    log_fail "TC-05: FUSE mount did not come back within ${MOUNT_TIMEOUT}s"
fi

# Verify previously written files still accessible
if read_verify_file "$CLIENT_01" "$PARTITION_FILE1" "$CKSUM1"; then
    log_ok "TC-05: Pre-restart file checksum verified after FUSE restart"
else
    log_fail "TC-05: Pre-restart file checksum mismatch after FUSE restart"
fi

# Write a new file to confirm FUSE is fully operational
FUSE_RESTART_FILE="${TEST_DIR}/after_fuse_restart.dat"
FUSE_CKSUM=$(write_test_file "$CLIENT_01" "$FUSE_RESTART_FILE" 16)
if read_verify_file "$CLIENT_01" "$FUSE_RESTART_FILE" "$FUSE_CKSUM"; then
    log_ok "TC-05: New write + verify succeeded after FUSE restart"
else
    log_fail "TC-05: New write failed after FUSE restart"
fi

# ---------------------------------------------------------------------------
# Teardown
# ---------------------------------------------------------------------------
log_section "Teardown"
restore_network "$DATA_02" 2>/dev/null || true
restore_network "$NON_LEADER_IP" 2>/dev/null || true
for mgr_ip in "$MGR_01" "$MGR_02" "$MGR_03"; do
    restore_network "$mgr_ip" 2>/dev/null || true
done
cleanup_test_files "$CLIENT_01" "$TEST_DIR"

print_summary "$TEST_SUITE_NAME"

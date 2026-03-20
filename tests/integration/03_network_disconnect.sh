#!/usr/bin/env bash
# DTFS Integration Test: 03 - Network Partition Tests
# Tests that the cluster maintains quorum with one manager partitioned,
# rejects writes when no quorum side exists, and recovers after partition heal.

set -euo pipefail
TEST_SUITE_NAME="03_network_disconnect"
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
TEST_DIR="${MOUNT}/test-net-partition-$$"

log_section "03_network_disconnect: Network Partition Tests"

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
mount_dtfs "$CLIENT_01" "$MOUNT" || { log_fail "Cannot mount DTFS — aborting"; print_summary; exit 1; }
ssh_exec "$CLIENT_01" "mkdir -p '${TEST_DIR}'"

# Verify cluster is healthy before starting
wait_for_health 60 || { log_fail "Cluster not healthy at test start — aborting"; print_summary; exit 1; }

# ---------------------------------------------------------------------------
# TC-01: Baseline write succeeds before any partition
# ---------------------------------------------------------------------------
log_section "TC-01: Baseline write (pre-partition)"
BASELINE_FILE="${TEST_DIR}/baseline.dat"
BASELINE_CKSUM=$(write_test_file "$CLIENT_01" "$BASELINE_FILE" 32)
if read_verify_file "$CLIENT_01" "$BASELINE_FILE" "$BASELINE_CKSUM"; then
    log_ok "TC-01: Baseline write + verify succeeded"
else
    log_fail "TC-01: Baseline write failed"
fi

# ---------------------------------------------------------------------------
# TC-02: Partition mgr-03 from rest of cluster; 2/3 managers = quorum survives
# ---------------------------------------------------------------------------
log_section "TC-02: Single manager partition (mgr-03 isolated)"
log_info "Partitioning ${MGR3} from ${MGR1} and ${MGR2}"

# Block mgr-03 <-> mgr-01 and mgr-03 <-> mgr-02 using iptables
partition_network "$MGR_03" "${MGR_01},${MGR_02}"
partition_network "$MGR_01" "${MGR_03}"
partition_network "$MGR_02" "${MGR_03}"

sleep 5  # Allow partition to propagate

# Remaining 2 managers should maintain quorum
log_info "Verifying writes still succeed with 2/3 managers (quorum)"
PARTITION_FILE="${TEST_DIR}/during_partition.dat"
PARTITION_CKSUM=$(write_test_file "$CLIENT_01" "$PARTITION_FILE" 32)
WRITE_RC=$?
if [ $WRITE_RC -eq 0 ] && read_verify_file "$CLIENT_01" "$PARTITION_FILE" "$PARTITION_CKSUM"; then
    log_ok "TC-02: Write succeeded with 2/3 managers active (quorum maintained)"
else
    log_fail "TC-02: Write failed — cluster should have quorum with 2/3 managers"
fi

# Verify via API on a non-partitioned manager
CLUSTER_STATUS=$(curl -sf --max-time 10 "http://${MGR_01}:9402/api/v1/cluster/status" 2>/dev/null || echo '{}')
LEADER=$(echo "$CLUSTER_STATUS" | jq -r '.raft.leader_id // empty')
assert_ne "$LEADER" "" "TC-02: A leader is still elected with 2/3 managers"

# ---------------------------------------------------------------------------
# TC-03: Restore mgr-03 and verify it re-syncs
# ---------------------------------------------------------------------------
log_section "TC-03: Restore mgr-03 partition and verify re-sync"
restore_network "$MGR_03"
restore_network "$MGR_01"
restore_network "$MGR_02"
log_info "Network restored for mgr-03"

# Poll /api/v1/nodes for mgr-03 to come back healthy
SYNC_TIMEOUT=60
SYNC_START=$(date +%s)
RESYNCED=false
while true; do
    NOW=$(date +%s)
    if [ $((NOW - SYNC_START)) -ge $SYNC_TIMEOUT ]; then
        break
    fi
    NODES_JSON=$(api_get "/api/v1/nodes" 15 2>/dev/null || echo '[]')
    MGR3_STATUS=$(echo "$NODES_JSON" | \
        jq -r --arg h "$MGR3" '.[] | select(.hostname == $h or .address == $h) | .status // .state' 2>/dev/null | head -1)
    if [ "$MGR3_STATUS" = "healthy" ] || [ "$MGR3_STATUS" = "active" ]; then
        RESYNCED=true
        break
    fi
    log_debug "mgr-03 status: '${MGR3_STATUS}' — waiting..."
    sleep 3
done

if $RESYNCED; then
    log_ok "TC-03: mgr-03 re-synced and is healthy within ${SYNC_TIMEOUT}s"
else
    log_fail "TC-03: mgr-03 did not report healthy within ${SYNC_TIMEOUT}s"
fi

# Verify previously written files are still intact
if read_verify_file "$CLIENT_01" "$PARTITION_FILE" "$PARTITION_CKSUM"; then
    log_ok "TC-03: File written during partition still verifies after re-sync"
else
    log_fail "TC-03: File written during partition failed checksum after re-sync"
fi

# ---------------------------------------------------------------------------
# TC-04: Minority partition — no quorum on any side, writes must fail
# ---------------------------------------------------------------------------
log_section "TC-04: Full minority partition (no quorum anywhere)"
log_info "Isolating all managers from each other — no quorum should exist"

# Block mgr-01 <-> mgr-02, mgr-01 <-> mgr-03, mgr-02 <-> mgr-03
partition_network "$MGR_01" "${MGR_02},${MGR_03}"
partition_network "$MGR_02" "${MGR_01},${MGR_03}"
partition_network "$MGR_03" "${MGR_01},${MGR_02}"

sleep 10  # Allow Raft to detect loss of quorum

log_info "Attempting write — should fail (no quorum)"
NO_QUORUM_FILE="${TEST_DIR}/no_quorum_write.dat"
WRITE_FAIL_RC=0
ssh_exec "$CLIENT_01" \
    "dd if=/dev/urandom bs=1M count=4 2>/dev/null > '${NO_QUORUM_FILE}'" 2>/dev/null || WRITE_FAIL_RC=$?
assert_nonzero_exit "$WRITE_FAIL_RC" "TC-04: Write correctly rejected when no quorum exists (EROFS/EIO)"

# ---------------------------------------------------------------------------
# TC-05: Restore all partitions, verify writes resume
# ---------------------------------------------------------------------------
log_section "TC-05: Restore all partitions — writes resume"
restore_network "$MGR_01"
restore_network "$MGR_02"
restore_network "$MGR_03"
log_info "All network partitions removed"

wait_for_health 60 || log_fail "TC-05: Cluster did not recover health within 60s"

LEADER=$(wait_for_leader 30) || true
if [ -n "$LEADER" ]; then
    log_ok "TC-05: Leader elected after full restore: $LEADER"
else
    log_fail "TC-05: No leader elected after restoring partitions"
fi

RESTORE_FILE="${TEST_DIR}/post_restore.dat"
RESTORE_CKSUM=$(write_test_file "$CLIENT_01" "$RESTORE_FILE" 32)
RESTORE_RC=$?
if [ $RESTORE_RC -eq 0 ] && read_verify_file "$CLIENT_01" "$RESTORE_FILE" "$RESTORE_CKSUM"; then
    log_ok "TC-05: Write succeeded after partition restore"
else
    log_fail "TC-05: Write failed after partition restore"
fi

# ---------------------------------------------------------------------------
# Teardown
# ---------------------------------------------------------------------------
log_section "Teardown"
# Ensure all iptables rules are cleaned up even if we failed mid-test
restore_network "$MGR_01" 2>/dev/null || true
restore_network "$MGR_02" 2>/dev/null || true
restore_network "$MGR_03" 2>/dev/null || true
cleanup_test_files "$CLIENT_01" "$TEST_DIR"

print_summary "$TEST_SUITE_NAME"

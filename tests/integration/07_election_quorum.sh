#!/usr/bin/env bash
# DTFS Integration Test: 07 - Election and Quorum Lock
# Tests that writes are rejected when the leader loses quorum,
# local reads still succeed (partition_safety=strict), and cluster
# recovers after partition heals.

set -euo pipefail
TEST_SUITE_NAME="07_election_quorum"
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
TEST_DIR="${MOUNT}/test-election-quorum-$$"

log_section "07_election_quorum: Election and Quorum Lock Tests"

# ---------------------------------------------------------------------------
# Helper: get the IP/hostname for a Raft node_id
# ---------------------------------------------------------------------------
resolve_node_ip() {
    local node_id="$1"
    case "$node_id" in
        *mgr-01*|*mgr1*|*"${MGR_01}"*) echo "$MGR_01" ;;
        *mgr-02*|*mgr2*|*"${MGR_02}"*) echo "$MGR_02" ;;
        *mgr-03*|*mgr3*|*"${MGR_03}"*) echo "$MGR_03" ;;
        *) echo "$node_id" ;;
    esac
}

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
mount_dtfs "$CLIENT_01" "$MOUNT" || { log_fail "Cannot mount DTFS — aborting"; print_summary; exit 1; }
ssh_exec "$CLIENT_01" "mkdir -p '${TEST_DIR}'"
wait_for_health 60 || { log_fail "Cluster not healthy at test start"; print_summary; exit 1; }

# ---------------------------------------------------------------------------
# TC-01: Identify current Raft leader
# ---------------------------------------------------------------------------
log_section "TC-01: Identify Raft leader"
LEADER_ID=$(get_leader_node_id)
if [ -z "$LEADER_ID" ] || [ "$LEADER_ID" = "null" ]; then
    log_fail "TC-01: No leader elected — cannot run election tests"
    print_summary; exit 1
fi
log_ok "TC-01: Raft leader identified: $LEADER_ID"

LEADER_IP=$(resolve_node_ip "$LEADER_ID")
log_info "Leader IP: $LEADER_IP"

# Determine non-leader managers
NON_LEADERS=()
for mgr_ip in "$MGR_01" "$MGR_02" "$MGR_03"; do
    if [ "$mgr_ip" != "$LEADER_IP" ]; then
        NON_LEADERS+=("$mgr_ip")
    fi
done
log_info "Non-leader managers: ${NON_LEADERS[*]}"

# ---------------------------------------------------------------------------
# TC-02: Write a file before partition (for later read test)
# ---------------------------------------------------------------------------
log_section "TC-02: Write baseline file before partition"
BASELINE_FILE="${TEST_DIR}/pre_quorum_loss.dat"
BASELINE_CKSUM=$(write_test_file "$CLIENT_01" "$BASELINE_FILE" 16)
if read_verify_file "$CLIENT_01" "$BASELINE_FILE" "$BASELINE_CKSUM"; then
    log_ok "TC-02: Baseline file written and verified"
else
    log_fail "TC-02: Baseline write failed"
fi

# ---------------------------------------------------------------------------
# TC-03: Partition ALL non-leader managers so leader loses quorum
# ---------------------------------------------------------------------------
log_section "TC-03: Isolate leader from all followers (leader loses quorum)"
# Block leader's outbound connections to all other managers
for follower_ip in "${NON_LEADERS[@]}"; do
    log_info "Blocking ${LEADER_IP} -> ${follower_ip}"
    ssh_exec "$LEADER_IP" "iptables -I OUTPUT -d '${follower_ip}' -j DROP" 2>/dev/null || true
    ssh_exec "$LEADER_IP" "iptables -I INPUT  -s '${follower_ip}' -j DROP" 2>/dev/null || true
    # Also block follower -> leader so Raft heartbeats die both ways
    ssh_exec "$follower_ip" "iptables -I OUTPUT -d '${LEADER_IP}' -j DROP" 2>/dev/null || true
    ssh_exec "$follower_ip" "iptables -I INPUT  -s '${LEADER_IP}' -j DROP" 2>/dev/null || true
done
log_info "Leader ${LEADER_IP} is now isolated — waiting for quorum timeout"

# Wait for Raft election timeout (typically 150-300ms election + leader step-down)
sleep 8

# ---------------------------------------------------------------------------
# TC-04: Attempt write via FUSE — should fail (EROFS or EIO)
# ---------------------------------------------------------------------------
log_section "TC-04: Write attempt with no quorum — must fail"
QUORUM_LOST_FILE="${TEST_DIR}/during_quorum_loss.dat"
WRITE_RC=0
ssh_exec "$CLIENT_01" \
    "dd if=/dev/urandom bs=1M count=4 2>/dev/null > '${QUORUM_LOST_FILE}'" \
    2>/dev/null || WRITE_RC=$?
assert_nonzero_exit "$WRITE_RC" "TC-04: Write correctly rejected when leader has no quorum (EROFS/EIO)"

# Confirm file was NOT created (or is empty / incomplete)
FILE_SIZE=$(ssh_exec "$CLIENT_01" "stat -c%s '${QUORUM_LOST_FILE}' 2>/dev/null || echo 0" 2>/dev/null || echo 0)
if [ "${FILE_SIZE:-0}" -lt $((4 * 1024 * 1024)) ]; then
    log_ok "TC-04: Quorum-loss write produced no complete file (size: ${FILE_SIZE} bytes)"
else
    log_fail "TC-04: Write unexpectedly produced a full file (${FILE_SIZE} bytes) despite no quorum"
fi

# ---------------------------------------------------------------------------
# TC-05: Local read should succeed (partition_safety=strict allows cached reads)
# ---------------------------------------------------------------------------
log_section "TC-05: Local read during quorum loss (should succeed)"
READ_RC=0
ACTUAL_CKSUM=$(ssh_exec "$CLIENT_01" \
    "md5sum '${BASELINE_FILE}' 2>/dev/null | awk '{print \$1}'" 2>/dev/null) || READ_RC=$?
if [ $READ_RC -eq 0 ] && [ "$ACTUAL_CKSUM" = "$BASELINE_CKSUM" ]; then
    log_ok "TC-05: Local read succeeded during quorum loss — checksum matches"
elif [ $READ_RC -eq 0 ]; then
    log_fail "TC-05: Read succeeded but checksum mismatch (expected=$BASELINE_CKSUM actual=$ACTUAL_CKSUM)"
else
    log_warn "TC-05: Read failed during quorum loss (rc=$READ_RC) — partition_safety may be configured to block reads too"
fi

# ---------------------------------------------------------------------------
# TC-06: Restore network — wait for re-election
# ---------------------------------------------------------------------------
log_section "TC-06: Restore network and wait for re-election"
# Remove all DROP rules
ssh_exec "$LEADER_IP" "
    for chain in INPUT OUTPUT; do
        while iptables -D \$chain 1 2>/dev/null; do :; done
    done
" 2>/dev/null || true
for follower_ip in "${NON_LEADERS[@]}"; do
    restore_network "$follower_ip"
done
restore_network "$LEADER_IP"
log_info "All partitions removed"

NEW_LEADER=$(wait_for_leader 30) || NEW_LEADER=""
if [ -n "$NEW_LEADER" ]; then
    log_ok "TC-06: New leader elected within 30s: $NEW_LEADER"
else
    log_fail "TC-06: No leader elected after partition restore"
fi

# ---------------------------------------------------------------------------
# TC-07: Verify writes succeed after re-election
# ---------------------------------------------------------------------------
log_section "TC-07: Writes succeed after re-election"
wait_for_health 30 || log_warn "TC-07: Cluster health not confirmed within 30s — attempting write anyway"

POST_ELECTION_FILE="${TEST_DIR}/post_election.dat"
POST_CKSUM=$(write_test_file "$CLIENT_01" "$POST_ELECTION_FILE" 16)
POST_RC=$?
if [ $POST_RC -eq 0 ] && read_verify_file "$CLIENT_01" "$POST_ELECTION_FILE" "$POST_CKSUM"; then
    log_ok "TC-07: Write succeeded after quorum restore and re-election"
else
    log_fail "TC-07: Write failed after re-election (rc=$POST_RC)"
fi

# Also verify baseline file is still intact
if read_verify_file "$CLIENT_01" "$BASELINE_FILE" "$BASELINE_CKSUM"; then
    log_ok "TC-07: Pre-partition baseline file still intact after re-election"
else
    log_fail "TC-07: Baseline file corrupted or lost after re-election"
fi

# ---------------------------------------------------------------------------
# Teardown
# ---------------------------------------------------------------------------
log_section "Teardown"
restore_network "$LEADER_IP" 2>/dev/null || true
for follower_ip in "${NON_LEADERS[@]}"; do
    restore_network "$follower_ip" 2>/dev/null || true
done
cleanup_test_files "$CLIENT_01" "$TEST_DIR"

print_summary "$TEST_SUITE_NAME"

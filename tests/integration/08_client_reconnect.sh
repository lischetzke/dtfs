#!/usr/bin/env bash
# DTFS Integration Test: 08 - FUSE Client Reconnect
# Tests that the FUSE client recovers after being killed and after the manager restarts.

set -euo pipefail
TEST_SUITE_NAME="08_client_reconnect"
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
TEST_DIR="${MOUNT}/test-client-reconnect-$$"
FUSE_SERVICE="dtfs-fuse@default.service"
FUSE_PROC="dtfs-fuse"
MANAGER_SERVICE="dtfs-manager"

log_section "08_client_reconnect: FUSE Client Reconnect Tests"

# ---------------------------------------------------------------------------
# Helper: wait for FUSE mount on client-01
# ---------------------------------------------------------------------------
wait_for_mount() {
    local timeout="${1:-30}"
    local start
    start=$(date +%s)
    while true; do
        local now
        now=$(date +%s)
        if [ $((now - start)) -ge "$timeout" ]; then
            return 1
        fi
        if ssh_exec "$CLIENT_01" "mountpoint -q '${MOUNT}'" 2>/dev/null; then
            return 0
        fi
        sleep 2
    done
}

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
mount_dtfs "$CLIENT_01" "$MOUNT" || { log_fail "Cannot mount DTFS — aborting"; print_summary; exit 1; }
ssh_exec "$CLIENT_01" "mkdir -p '${TEST_DIR}'"
wait_for_health 60 || { log_fail "Cluster not healthy at test start"; print_summary; exit 1; }

# ---------------------------------------------------------------------------
# TC-01: Write file via FUSE on client-01 (baseline)
# ---------------------------------------------------------------------------
log_section "TC-01: Write baseline file via FUSE"
BASELINE_FILE="${TEST_DIR}/baseline.dat"
BASELINE_CKSUM=$(write_test_file "$CLIENT_01" "$BASELINE_FILE" 64)
if read_verify_file "$CLIENT_01" "$BASELINE_FILE" "$BASELINE_CKSUM"; then
    log_ok "TC-01: Baseline file written and verified via FUSE"
else
    log_fail "TC-01: Baseline write/verify failed"
fi

# ---------------------------------------------------------------------------
# TC-02: Kill dtfs-fuse process on client-01
# ---------------------------------------------------------------------------
log_section "TC-02: Kill dtfs-fuse process (SIGKILL)"
ssh_exec "$CLIENT_01" "pkill -9 '${FUSE_PROC}' 2>/dev/null || true"
sleep 1
# Also do a lazy unmount to ensure the mount is torn down cleanly
ssh_exec "$CLIENT_01" "fusermount3 -uz '${MOUNT}' 2>/dev/null || umount -l '${MOUNT}' 2>/dev/null || true"
sleep 1

# Confirm mount is gone
MOUNT_GONE=false
if ! ssh_exec "$CLIENT_01" "mountpoint -q '${MOUNT}'" 2>/dev/null; then
    MOUNT_GONE=true
fi
if $MOUNT_GONE; then
    log_ok "TC-02: FUSE mount is down after kill"
else
    log_warn "TC-02: FUSE mount still appears active after kill — lazy unmount may be delayed"
fi

# ---------------------------------------------------------------------------
# TC-03: Restart FUSE service and wait for mount to come back
# ---------------------------------------------------------------------------
log_section "TC-03: Restart FUSE service — wait for mount"
ssh_exec "$CLIENT_01" "systemctl start '${FUSE_SERVICE}'"

if wait_for_mount 30; then
    log_ok "TC-03: FUSE mount came back within 30s after restart"
else
    log_fail "TC-03: FUSE mount did not come back within 30s"
fi

# ---------------------------------------------------------------------------
# TC-04: Read back originally written file and verify checksum
# ---------------------------------------------------------------------------
log_section "TC-04: Verify baseline file after FUSE restart"
if read_verify_file "$CLIENT_01" "$BASELINE_FILE" "$BASELINE_CKSUM"; then
    log_ok "TC-04: Baseline file checksum correct after FUSE restart"
else
    log_fail "TC-04: Baseline file checksum mismatch after FUSE restart"
fi

# ---------------------------------------------------------------------------
# TC-05: Write new file after FUSE restart — verify it works
# ---------------------------------------------------------------------------
log_section "TC-05: Write new file after FUSE restart"
POST_RESTART_FILE="${TEST_DIR}/post_restart.dat"
POST_CKSUM=$(write_test_file "$CLIENT_01" "$POST_RESTART_FILE" 32)
POST_RC=$?
if [ $POST_RC -eq 0 ] && read_verify_file "$CLIENT_01" "$POST_RESTART_FILE" "$POST_CKSUM"; then
    log_ok "TC-05: New write + verify succeeded after FUSE restart"
else
    log_fail "TC-05: New write failed after FUSE restart (rc=$POST_RC)"
fi

# ---------------------------------------------------------------------------
# TC-06: Restart dtfs-manager while FUSE is mounted
# ---------------------------------------------------------------------------
log_section "TC-06: Restart dtfs-manager while FUSE is mounted"
LEADER_ID=$(get_leader_node_id)
LEADER_IP="$MGR_01"
if [ -n "$LEADER_ID" ] && [ "$LEADER_ID" != "null" ]; then
    case "$LEADER_ID" in
        *mgr-01*|*"${MGR_01}"*) LEADER_IP="$MGR_01" ;;
        *mgr-02*|*"${MGR_02}"*) LEADER_IP="$MGR_02" ;;
        *mgr-03*|*"${MGR_03}"*) LEADER_IP="$MGR_03" ;;
    esac
fi
log_info "Restarting dtfs-manager on leader node: $LEADER_IP"

restart_service "$LEADER_IP" "$MANAGER_SERVICE"
log_info "Manager restarted on $LEADER_IP — waiting for re-election"

# Give Raft time to elect a new leader
NEW_LEADER=$(wait_for_leader 30) || NEW_LEADER=""
if [ -n "$NEW_LEADER" ]; then
    log_ok "TC-06: New leader elected after manager restart: $NEW_LEADER"
else
    log_fail "TC-06: No leader after manager restart within 30s"
fi

# ---------------------------------------------------------------------------
# TC-07: Verify FUSE reconnects — read/write within 60s of manager restart
# ---------------------------------------------------------------------------
log_section "TC-07: FUSE reconnects to cluster after manager restart"

# Poll reads/writes from FUSE with timeout
RECONNECT_TIMEOUT=60
RECONNECT_START=$(date +%s)
FUSE_RECONNECTED=false

while true; do
    NOW=$(date +%s)
    if [ $((NOW - RECONNECT_START)) -ge $RECONNECT_TIMEOUT ]; then
        break
    fi

    # Try a read
    READ_CKSUM=$(ssh_exec "$CLIENT_01" \
        "md5sum '${POST_RESTART_FILE}' 2>/dev/null | awk '{print \$1}'" 2>/dev/null) || true

    if [ "$READ_CKSUM" = "$POST_CKSUM" ]; then
        FUSE_RECONNECTED=true
        log_info "FUSE reconnected (elapsed: $((NOW - RECONNECT_START))s)"
        break
    fi
    sleep 3
done

if $FUSE_RECONNECTED; then
    log_ok "TC-07: FUSE successfully reconnected to new manager within ${RECONNECT_TIMEOUT}s"
else
    log_fail "TC-07: FUSE did not reconnect to new manager within ${RECONNECT_TIMEOUT}s"
fi

# Write another file to confirm full write path also works
RECONNECT_WRITE_FILE="${TEST_DIR}/after_mgr_restart.dat"
RECONNECT_CKSUM=$(write_test_file "$CLIENT_01" "$RECONNECT_WRITE_FILE" 16)
RECONNECT_RC=$?
if [ $RECONNECT_RC -eq 0 ] && read_verify_file "$CLIENT_01" "$RECONNECT_WRITE_FILE" "$RECONNECT_CKSUM"; then
    log_ok "TC-07: Write after manager restart succeeded"
else
    log_fail "TC-07: Write after manager restart failed (rc=$RECONNECT_RC)"
fi

# ---------------------------------------------------------------------------
# TC-08: Verify all original files still intact
# ---------------------------------------------------------------------------
log_section "TC-08: Data integrity after all reconnect events"
if read_verify_file "$CLIENT_01" "$BASELINE_FILE" "$BASELINE_CKSUM"; then
    log_ok "TC-08: Baseline file intact after all reconnect events"
else
    log_fail "TC-08: Baseline file corrupted"
fi
if read_verify_file "$CLIENT_01" "$POST_RESTART_FILE" "$POST_CKSUM"; then
    log_ok "TC-08: Post-FUSE-restart file intact"
else
    log_fail "TC-08: Post-FUSE-restart file corrupted"
fi

# ---------------------------------------------------------------------------
# Teardown
# ---------------------------------------------------------------------------
log_section "Teardown"
cleanup_test_files "$CLIENT_01" "$TEST_DIR"

print_summary "$TEST_SUITE_NAME"

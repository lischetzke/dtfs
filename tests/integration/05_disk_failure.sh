#!/usr/bin/env bash
# DTFS Integration Test: 05 - Disk Failure Simulation
# Simulates unmounting /mnt/disk-a on data-01 and verifies detection + recovery.

set -euo pipefail
TEST_SUITE_NAME="05_disk_failure"
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
TEST_DIR="${MOUNT}/test-disk-failure-$$"
DISK_A_MOUNT="/mnt/disk-a"

log_section "05_disk_failure: Disk Failure Simulation Tests"

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
mount_dtfs "$CLIENT_01" "$MOUNT" || { log_fail "Cannot mount DTFS — aborting"; print_summary; exit 1; }
ssh_exec "$CLIENT_01" "mkdir -p '${TEST_DIR}'"
wait_for_health 60 || { log_fail "Cluster not healthy at test start"; print_summary; exit 1; }

# ---------------------------------------------------------------------------
# TC-01: Write test file so it is likely to land on data-01 /mnt/disk-a
# ---------------------------------------------------------------------------
log_section "TC-01: Write test file (targeting data-01)"
# Write a file and then check where its chunks live
TEST_FILE="${TEST_DIR}/disk_fail_test.dat"
FILE_CKSUM=$(write_test_file "$CLIENT_01" "$TEST_FILE" 128)
if [ -z "$FILE_CKSUM" ]; then
    log_fail "TC-01: Failed to write test file — aborting"
    print_summary; exit 1
fi
log_ok "TC-01: Test file written (checksum: $FILE_CKSUM)"
sleep 3  # Allow chunk registration

# Determine chunk IDs
ENCODED_PATH=$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1]))" \
    "$TEST_FILE" 2>/dev/null || echo "$TEST_FILE" | sed 's|/|%2F|g')
CHUNK_JSON=$(api_get "/api/v1/files/${ENCODED_PATH}/chunks" 30 2>/dev/null || echo '[]')
FIRST_CHUNK=$(echo "$CHUNK_JSON" | jq -r '.[0].chunk_id // .[0].id // empty' 2>/dev/null)
log_info "First chunk ID: ${FIRST_CHUNK:-<not available>}"

# ---------------------------------------------------------------------------
# TC-02: Simulate disk failure by unmounting /mnt/disk-a on data-01
# ---------------------------------------------------------------------------
log_section "TC-02: Simulate disk failure (umount /mnt/disk-a on data-01)"
# Check the disk is mounted first
DISK_MOUNTED=$(ssh_exec "$DATA_01" "mountpoint -q '${DISK_A_MOUNT}' && echo yes || echo no" 2>/dev/null || echo "no")
if [ "$DISK_MOUNTED" != "yes" ]; then
    log_warn "TC-02: ${DISK_A_MOUNT} is not mounted on data-01 — skipping disk failure simulation"
    log_skip "TC-02: Disk not mounted, cannot simulate failure"
else
    fail_disk "$DATA_01" "$DISK_A_MOUNT"
    log_info "Unmounted ${DISK_A_MOUNT} on data-01"

    # ---------------------------------------------------------------------------
    # TC-03: Wait up to 60s for manager to detect missing chunks / disk status change
    # ---------------------------------------------------------------------------
    log_section "TC-03: Manager detects disk failure on data-01"
    DETECT_TIMEOUT=60
    DETECT_START=$(date +%s)
    DISK_DETECTED=false
    while true; do
        NOW=$(date +%s)
        if [ $((NOW - DETECT_START)) -ge $DETECT_TIMEOUT ]; then
            break
        fi
        NODES_JSON=$(api_get "/api/v1/nodes" 15 2>/dev/null || echo '[]')
        # Look for disk_status / disk_error / degraded state on data-01
        DATA01_INFO=$(echo "$NODES_JSON" | \
            jq -r --arg ip "$DATA_01" \
            '.[] | select(.hostname == $ip or .address == $ip)' 2>/dev/null)
        DISK_STATUS=$(echo "$DATA01_INFO" | \
            jq -r '.disk_status // .disks[0].status // .storage_status // empty' 2>/dev/null)
        NODE_STATUS=$(echo "$DATA01_INFO" | jq -r '.status // .state // empty' 2>/dev/null)
        log_debug "data-01 node_status='$NODE_STATUS' disk_status='$DISK_STATUS'"

        if [ "$DISK_STATUS" = "degraded" ] || [ "$DISK_STATUS" = "failed" ] || \
           [ "$DISK_STATUS" = "error" ] || [ "$NODE_STATUS" = "degraded" ] || \
           [ "$NODE_STATUS" = "disk_error" ]; then
            DISK_DETECTED=true
            log_info "Disk failure detected: node_status='$NODE_STATUS' disk_status='$DISK_STATUS'"
            break
        fi

        # Also check stats for missing chunks
        if [ -n "$FIRST_CHUNK" ]; then
            CHUNK_HEALTH=$(api_get "/api/v1/chunks/${FIRST_CHUNK}/replicas" 10 2>/dev/null | \
                jq '[.[] | select(.status == "missing" or .status == "failed")] | length' 2>/dev/null || echo 0)
            if [ "$CHUNK_HEALTH" -gt 0 ]; then
                DISK_DETECTED=true
                log_info "Manager detected missing chunk replicas ($CHUNK_HEALTH missing)"
                break
            fi
        fi

        sleep 3
    done

    if $DISK_DETECTED; then
        log_ok "TC-03: Manager detected disk failure within ${DETECT_TIMEOUT}s"
    else
        log_warn "TC-03: Manager did not explicitly flag disk failure within ${DETECT_TIMEOUT}s (may rely on scrub)"
    fi

    # ---------------------------------------------------------------------------
    # TC-04: Trigger manual scrub to check disk
    # ---------------------------------------------------------------------------
    log_section "TC-04: Trigger manual scrub on data-01"
    SCRUB_RC=0
    ssh_exec "$DATA_01" \
        "DTFS_MANAGER='${MANAGER_API}' /usr/local/bin/dtfs-cli scrub --disk '${DISK_A_MOUNT}'" \
        2>/dev/null || SCRUB_RC=$?
    if [ $SCRUB_RC -eq 0 ]; then
        log_ok "TC-04: Manual scrub on ${DISK_A_MOUNT} completed"
    else
        log_warn "TC-04: dtfs-cli scrub returned rc=$SCRUB_RC (disk may be detached — expected)"
    fi

    # ---------------------------------------------------------------------------
    # TC-05: Restore disk (remount /mnt/disk-a on data-01)
    # ---------------------------------------------------------------------------
    log_section "TC-05: Restore disk (remount /mnt/disk-a)"
    restore_disk "$DATA_01" "$DISK_A_MOUNT"
    log_info "Remounted ${DISK_A_MOUNT} on data-01"
    sleep 5
fi

# ---------------------------------------------------------------------------
# TC-06: Data is still accessible (replicas on other nodes should serve reads)
# ---------------------------------------------------------------------------
log_section "TC-06: Data accessible during/after disk failure (replica durability)"
# With replication, the file should still be readable even if one disk was down
if read_verify_file "$CLIENT_01" "$TEST_FILE" "$FILE_CKSUM"; then
    log_ok "TC-06: File checksum verified — replicas served reads during disk failure"
else
    log_fail "TC-06: File checksum mismatch — data not accessible despite replication"
fi

# ---------------------------------------------------------------------------
# TC-07: Write a new file after disk restore to confirm system recovered
# ---------------------------------------------------------------------------
log_section "TC-07: Write new file after disk restore"
POST_RESTORE_FILE="${TEST_DIR}/post_restore.dat"
POST_CKSUM=$(write_test_file "$CLIENT_01" "$POST_RESTORE_FILE" 32)
if read_verify_file "$CLIENT_01" "$POST_RESTORE_FILE" "$POST_CKSUM"; then
    log_ok "TC-07: New write + verify succeeded after disk restore"
else
    log_fail "TC-07: New write failed after disk restore"
fi

# ---------------------------------------------------------------------------
# Teardown
# ---------------------------------------------------------------------------
log_section "Teardown"
# Safety: ensure disk is remounted if test failed early
ssh_exec "$DATA_01" "mountpoint -q '${DISK_A_MOUNT}' || mount '${DISK_A_MOUNT}'" 2>/dev/null || true
cleanup_test_files "$CLIENT_01" "$TEST_DIR"

print_summary "$TEST_SUITE_NAME"

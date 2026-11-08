#!/usr/bin/env bash
# DTFS Integration Test: 04 - Service Crash Recovery
# Tests that dtfs-manager and dtfs-data survive SIGKILL and recover cleanly.

set -euo pipefail
TEST_SUITE_NAME="04_service_crash"
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
TEST_DIR="${MOUNT}/test-service-crash-$$"
MANAGER_SERVICE="dtfs-manager"
DATA_SERVICE="dtfs-data"

log_section "04_service_crash: Service Crash Recovery Tests"

# ---------------------------------------------------------------------------
# Helper: resolve node IP from node ID
# ---------------------------------------------------------------------------
node_ip_for_id() {
    local node_id="$1"
    # Try to map well-known node IDs to IPs; fall back to the node_id itself
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
# TC-01: Write a file to establish baseline
# ---------------------------------------------------------------------------
log_section "TC-01: Baseline write before crash"
PRE_CRASH_FILE="${TEST_DIR}/pre_crash.dat"
PRE_CRASH_CKSUM=$(write_test_file "$CLIENT_01" "$PRE_CRASH_FILE" 64)
if read_verify_file "$CLIENT_01" "$PRE_CRASH_FILE" "$PRE_CRASH_CKSUM"; then
    log_ok "TC-01: Baseline write + verify passed"
else
    log_fail "TC-01: Baseline write failed"
fi

# ---------------------------------------------------------------------------
# TC-02: Find and kill the current Raft leader's dtfs-manager
# ---------------------------------------------------------------------------
log_section "TC-02: Kill Raft leader's dtfs-manager"
LEADER_ID=$(get_leader_node_id)
if [ -z "$LEADER_ID" ] || [ "$LEADER_ID" = "null" ]; then
    log_fail "TC-02: Cannot determine current leader — aborting crash test"
    print_summary; exit 1
fi
log_info "Current Raft leader: $LEADER_ID"
LEADER_IP=$(node_ip_for_id "$LEADER_ID")
log_info "Leader IP resolved to: $LEADER_IP"

crash_service "$LEADER_IP" "$MANAGER_SERVICE"
log_info "dtfs-manager on $LEADER_IP killed (SIGKILL)"

# ---------------------------------------------------------------------------
# TC-03: Wait for new leader election (up to 30s)
# ---------------------------------------------------------------------------
log_section "TC-03: New leader election after crash"
# Poll a non-leader manager
FALLBACK_MGR="$MGR_02"
if [ "$LEADER_IP" = "$MGR_02" ] || [ "$LEADER_IP" = "$MGR_02" ]; then
    FALLBACK_MGR="$MGR_01"
fi

NEW_LEADER=""
ELECTION_TIMEOUT=30
ELECTION_START=$(date +%s)
while true; do
    NOW=$(date +%s)
    if [ $((NOW - ELECTION_START)) -ge $ELECTION_TIMEOUT ]; then
        break
    fi
    NEW_LEADER=$(curl -sf --max-time 5 \
        "http://${FALLBACK_MGR}:9402/api/v1/cluster/status" 2>/dev/null \
        | jq -r '.raft.leader_id // empty')
    if [ -n "$NEW_LEADER" ] && [ "$NEW_LEADER" != "null" ] && [ "$NEW_LEADER" != "$LEADER_ID" ]; then
        break
    fi
    sleep 2
done

if [ -n "$NEW_LEADER" ] && [ "$NEW_LEADER" != "$LEADER_ID" ]; then
    log_ok "TC-03: New leader elected: $NEW_LEADER (within ${ELECTION_TIMEOUT}s)"
else
    log_fail "TC-03: No new leader elected within ${ELECTION_TIMEOUT}s (got: '${NEW_LEADER}')"
fi

# ---------------------------------------------------------------------------
# TC-04: Write another file on new leader — should succeed
# ---------------------------------------------------------------------------
log_section "TC-04: Write after leader failover"
NEW_MANAGER_IP=$(node_ip_for_id "${NEW_LEADER:-$FALLBACK_MGR}")
POST_CRASH_FILE="${TEST_DIR}/post_crash.dat"

# Temporarily redirect API to new leader
OLD_API="$MANAGER_API"
MANAGER_API="http://${NEW_MANAGER_IP}:9402"

POST_CRASH_CKSUM=$(write_test_file "$CLIENT_01" "$POST_CRASH_FILE" 32)
WRITE_RC=$?
if [ $WRITE_RC -eq 0 ] && read_verify_file "$CLIENT_01" "$POST_CRASH_FILE" "$POST_CRASH_CKSUM"; then
    log_ok "TC-04: Write to new leader succeeded"
else
    log_fail "TC-04: Write to new leader failed (rc=$WRITE_RC)"
fi

MANAGER_API="$OLD_API"

# ---------------------------------------------------------------------------
# TC-05: Restart killed dtfs-manager and verify it rejoins
# ---------------------------------------------------------------------------
log_section "TC-05: Restart crashed dtfs-manager, verify re-join"
start_service "$LEADER_IP" "$MANAGER_SERVICE"
log_info "Restarted $MANAGER_SERVICE on $LEADER_IP"

REJOIN_TIMEOUT=30
REJOIN_START=$(date +%s)
REJOINED=false
while true; do
    NOW=$(date +%s)
    if [ $((NOW - REJOIN_START)) -ge $REJOIN_TIMEOUT ]; then
        break
    fi
    NODES_JSON=$(api_get "/api/v1/nodes" 15 2>/dev/null || echo '[]')
    NODE_STATUS=$(echo "$NODES_JSON" | \
        jq -r --arg ip "$LEADER_IP" \
        '.[] | select(.hostname == $ip or .address == $ip or (.node_id // "") == $ip) | .status // .state' \
        2>/dev/null | head -1)
    if [ "$NODE_STATUS" = "healthy" ] || [ "$NODE_STATUS" = "active" ]; then
        REJOINED=true
        break
    fi
    sleep 3
done

if $REJOINED; then
    log_ok "TC-05: Previously crashed manager rejoined cluster within ${REJOIN_TIMEOUT}s"
else
    log_fail "TC-05: Previously crashed manager did not rejoin within ${REJOIN_TIMEOUT}s"
fi

# ---------------------------------------------------------------------------
# TC-06: Kill dtfs-data on data-01
# ---------------------------------------------------------------------------
log_section "TC-06: Kill dtfs-data on data-01"
crash_service "$DATA_01" "$DATA_SERVICE"
log_info "dtfs-data on $DATA_01 killed (SIGKILL)"

# ---------------------------------------------------------------------------
# TC-07: Wait for manager to mark data-01 as suspect/down
# ---------------------------------------------------------------------------
log_section "TC-07: Manager detects data-01 as suspect/down"
DOWN_TIMEOUT=30
DOWN_START=$(date +%s)
DATA01_DOWN=false
while true; do
    NOW=$(date +%s)
    if [ $((NOW - DOWN_START)) -ge $DOWN_TIMEOUT ]; then
        break
    fi
    NODES_JSON=$(api_get "/api/v1/nodes" 15 2>/dev/null || echo '[]')
    DATA01_STATUS=$(echo "$NODES_JSON" | \
        jq -r --arg ip "$DATA_01" \
        '.[] | select(.hostname == $ip or .address == $ip) | .status // .state' \
        2>/dev/null | head -1)
    if [ "$DATA01_STATUS" = "suspect" ] || [ "$DATA01_STATUS" = "down" ] || \
       [ "$DATA01_STATUS" = "unreachable" ] || [ "$DATA01_STATUS" = "offline" ]; then
        DATA01_DOWN=true
        log_info "data-01 status: $DATA01_STATUS"
        break
    fi
    sleep 2
done

if $DATA01_DOWN; then
    log_ok "TC-07: Manager detected data-01 as down/suspect within ${DOWN_TIMEOUT}s"
else
    log_warn "TC-07: data-01 status did not transition to down/suspect within ${DOWN_TIMEOUT}s (status: '${DATA01_STATUS}')"
    # Not a hard failure — heartbeat interval may be longer than 30s in some configs
fi

# ---------------------------------------------------------------------------
# TC-08: Restart dtfs-data on data-01, verify re-registration
# ---------------------------------------------------------------------------
log_section "TC-08: Restart dtfs-data on data-01, verify re-registration"
start_service "$DATA_01" "$DATA_SERVICE"
log_info "Restarted $DATA_SERVICE on $DATA_01"

REREG_TIMEOUT=30
REREG_START=$(date +%s)
DATA01_HEALTHY=false
while true; do
    NOW=$(date +%s)
    if [ $((NOW - REREG_START)) -ge $REREG_TIMEOUT ]; then
        break
    fi
    NODES_JSON=$(api_get "/api/v1/nodes" 15 2>/dev/null || echo '[]')
    DATA01_STATUS=$(echo "$NODES_JSON" | \
        jq -r --arg ip "$DATA_01" \
        '.[] | select(.hostname == $ip or .address == $ip) | .status // .state' \
        2>/dev/null | head -1)
    if [ "$DATA01_STATUS" = "healthy" ] || [ "$DATA01_STATUS" = "active" ]; then
        DATA01_HEALTHY=true
        break
    fi
    sleep 2
done

if $DATA01_HEALTHY; then
    log_ok "TC-08: data-01 re-registered as healthy within ${REREG_TIMEOUT}s"
else
    log_fail "TC-08: data-01 did not re-register as healthy within ${REREG_TIMEOUT}s (status: '${DATA01_STATUS}')"
fi

# ---------------------------------------------------------------------------
# TC-09: Verify pre-crash data is still accessible
# ---------------------------------------------------------------------------
log_section "TC-09: Pre-crash file still accessible"
if read_verify_file "$CLIENT_01" "$PRE_CRASH_FILE" "$PRE_CRASH_CKSUM"; then
    log_ok "TC-09: File written before any crash is still intact"
else
    log_fail "TC-09: Pre-crash file checksum mismatch after recovery"
fi

# ---------------------------------------------------------------------------
# Teardown
# ---------------------------------------------------------------------------
log_section "Teardown"
cleanup_test_files "$CLIENT_01" "$TEST_DIR"

print_summary "$TEST_SUITE_NAME"

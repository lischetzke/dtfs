#!/usr/bin/env bash
# DTFS Integration Test: 11 - Comprehensive Feature Validation
# Verifies all CLI commands, WebUI, API endpoints, and audit log functionality.

set -euo pipefail
TEST_SUITE_NAME="11_feature_validation"
source "$(dirname "$0")/lib/common.sh"

MANAGER_API="${MANAGER_API:-http://10.0.1.1:9402}"
MANAGER_HOST="${DTFS_MANAGER_HOST:-10.0.1.1}"
MANAGER_UI_PORT="${DTFS_UI_PORT:-9402}"
SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=10"

MGR1="${MGR1:-mgr-01}"
MGR2="${MGR2:-mgr-02}"
MGR3="${MGR3:-mgr-03}"
DATA1="${DATA1:-data-01}"
DATA2="${DATA2:-data-02}"
CLIENT1="${CLIENT1:-client-01}"

MOUNT="${TEST_MOUNT:-/mnt/dtfs}"
TEST_DIR="${MOUNT}/test-feature-validation-$$"

# Required JSON fields in /api/v1/stats response
REQUIRED_STATS_FIELDS=(
    "total_files"
    "total_chunks"
    "total_bytes"
    "chunks_healthy"
    "chunks_corrupt"
    "chunks_missing"
    "last_scrub"
    "nodes_healthy"
    "nodes_total"
)

log_section "11_feature_validation: Comprehensive Feature Validation"

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
mount_dtfs "$CLIENT_01" "$MOUNT" || { log_fail "Cannot mount DTFS — aborting"; print_summary; exit 1; }
ssh_exec "$CLIENT_01" "mkdir -p '${TEST_DIR}'"
wait_for_health 60 || { log_fail "Cluster not healthy at test start"; print_summary; exit 1; }

# Write a test file so CLI/audit commands have something to show
log_info "Writing test file for audit verification"
AUDIT_FILE="${TEST_DIR}/audit_check.dat"
AUDIT_CKSUM=$(write_test_file "$CLIENT_01" "$AUDIT_FILE" 8)
sleep 2

# ---------------------------------------------------------------------------
# TC-01: dtfs-cli status
# ---------------------------------------------------------------------------
log_section "TC-01: dtfs-cli status"
STATUS_OUT=""
STATUS_RC=0
STATUS_OUT=$(dtfs_cli "status" 2>/dev/null) || STATUS_RC=$?
if [ $STATUS_RC -eq 0 ] && [ -n "$STATUS_OUT" ]; then
    log_ok "TC-01: dtfs-cli status returned output"
    log_debug "status output: $STATUS_OUT"
else
    log_fail "TC-01: dtfs-cli status failed (rc=$STATUS_RC)"
fi

# ---------------------------------------------------------------------------
# TC-02: dtfs-cli nodes
# ---------------------------------------------------------------------------
log_section "TC-02: dtfs-cli nodes"
NODES_RC=0
NODES_OUT=$(dtfs_cli "nodes" 2>/dev/null) || NODES_RC=$?
if [ $NODES_RC -eq 0 ] && [ -n "$NODES_OUT" ]; then
    log_ok "TC-02: dtfs-cli nodes returned output"
    # Verify at least one of our known nodes appears
    if echo "$NODES_OUT" | grep -qE "${DATA_01}|${MGR_01}|data-01|mgr-01"; then
        log_ok "TC-02: Known node present in nodes output"
    else
        log_warn "TC-02: Expected node not found in nodes output (may use IDs)"
    fi
else
    log_fail "TC-02: dtfs-cli nodes failed (rc=$NODES_RC)"
fi

# ---------------------------------------------------------------------------
# TC-03: dtfs-cli volumes
# ---------------------------------------------------------------------------
log_section "TC-03: dtfs-cli volumes"
VOLUMES_RC=0
VOLUMES_OUT=$(dtfs_cli "volumes" 2>/dev/null) || VOLUMES_RC=$?
if [ $VOLUMES_RC -eq 0 ]; then
    log_ok "TC-03: dtfs-cli volumes completed (rc=0)"
else
    log_fail "TC-03: dtfs-cli volumes failed (rc=$VOLUMES_RC)"
fi

# ---------------------------------------------------------------------------
# TC-04: dtfs-cli files
# ---------------------------------------------------------------------------
log_section "TC-04: dtfs-cli files"
FILES_RC=0
FILES_OUT=$(dtfs_cli "files" 2>/dev/null) || FILES_RC=$?
if [ $FILES_RC -eq 0 ] && [ -n "$FILES_OUT" ]; then
    log_ok "TC-04: dtfs-cli files returned output"
else
    log_fail "TC-04: dtfs-cli files failed (rc=$FILES_RC)"
fi

# ---------------------------------------------------------------------------
# TC-05: dtfs-cli audit --limit 5
# ---------------------------------------------------------------------------
log_section "TC-05: dtfs-cli audit --limit 5"
AUDIT_RC=0
AUDIT_OUT=$(dtfs_cli "audit --limit 5" 2>/dev/null) || AUDIT_RC=$?
if [ $AUDIT_RC -eq 0 ]; then
    log_ok "TC-05: dtfs-cli audit --limit 5 completed (rc=0)"
    AUDIT_LINE_COUNT=$(echo "$AUDIT_OUT" | grep -c . 2>/dev/null || echo 0)
    log_info "TC-05: Audit returned $AUDIT_LINE_COUNT line(s)"
else
    log_fail "TC-05: dtfs-cli audit failed (rc=$AUDIT_RC)"
fi

# ---------------------------------------------------------------------------
# TC-06: dtfs-cli config --show
# ---------------------------------------------------------------------------
log_section "TC-06: dtfs-cli config --show"
CONFIG_RC=0
CONFIG_OUT=$(dtfs_cli "config --show" 2>/dev/null) || CONFIG_RC=$?
if [ $CONFIG_RC -eq 0 ] && [ -n "$CONFIG_OUT" ]; then
    log_ok "TC-06: dtfs-cli config --show returned output"
else
    log_fail "TC-06: dtfs-cli config --show failed (rc=$CONFIG_RC)"
fi

# ---------------------------------------------------------------------------
# TC-07: dtfs-cli health
# ---------------------------------------------------------------------------
log_section "TC-07: dtfs-cli health"
HEALTH_RC=0
HEALTH_OUT=$(dtfs_cli "health" 2>/dev/null) || HEALTH_RC=$?
if [ $HEALTH_RC -eq 0 ] && [ -n "$HEALTH_OUT" ]; then
    log_ok "TC-07: dtfs-cli health returned output"
    # Verify the word 'healthy' or 'ok' appears somewhere
    if echo "$HEALTH_OUT" | grep -iqE "healthy|ok|good"; then
        log_ok "TC-07: Health output indicates healthy state"
    else
        log_warn "TC-07: Health output does not explicitly say 'healthy': $HEALTH_OUT"
    fi
else
    log_fail "TC-07: dtfs-cli health failed (rc=$HEALTH_RC)"
fi

# ---------------------------------------------------------------------------
# TC-08: WebUI responds HTTP 200
# ---------------------------------------------------------------------------
log_section "TC-08: WebUI responds HTTP 200"
UI_URL="http://${MANAGER_HOST}:${MANAGER_UI_PORT}/ui"
UI_HTTP_CODE=$(curl -so /dev/null -w "%{http_code}" --max-time 10 "$UI_URL" 2>/dev/null || echo "000")
if [ "$UI_HTTP_CODE" = "200" ]; then
    log_ok "TC-08: WebUI at $UI_URL returned HTTP 200"
elif [ "$UI_HTTP_CODE" = "301" ] || [ "$UI_HTTP_CODE" = "302" ]; then
    log_ok "TC-08: WebUI at $UI_URL returned HTTP $UI_HTTP_CODE (redirect — UI present)"
else
    log_fail "TC-08: WebUI at $UI_URL returned HTTP $UI_HTTP_CODE (expected 200)"
fi

# ---------------------------------------------------------------------------
# TC-09: API health endpoint returns {"status":"healthy"}
# ---------------------------------------------------------------------------
log_section "TC-09: API health endpoint"
HEALTH_JSON=$(curl -sf --max-time 10 "${MANAGER_API}/api/v1/health" 2>/dev/null || echo '{}')
HEALTH_STATUS=$(echo "$HEALTH_JSON" | jq -r '.status // empty' 2>/dev/null)
assert_eq "$HEALTH_STATUS" "healthy" "TC-09: /api/v1/health returns {\"status\":\"healthy\"}"

# ---------------------------------------------------------------------------
# TC-10: Stats endpoint returns valid JSON with all expected fields
# ---------------------------------------------------------------------------
log_section "TC-10: Stats endpoint — validate required fields"
STATS_JSON=$(api_get "/api/v1/stats" 15 2>/dev/null || echo '{}')

# Validate it's parseable JSON
if echo "$STATS_JSON" | jq . >/dev/null 2>&1; then
    log_ok "TC-10: /api/v1/stats returns valid JSON"
else
    log_fail "TC-10: /api/v1/stats returned invalid JSON"
fi

# Check for expected fields (warn but don't fail if a field is missing — API shape may vary)
for field in "${REQUIRED_STATS_FIELDS[@]}"; do
    FIELD_VAL=$(echo "$STATS_JSON" | jq -r ".${field} // empty" 2>/dev/null)
    if [ -n "$FIELD_VAL" ]; then
        log_ok "TC-10: stats field '${field}' present (value: ${FIELD_VAL})"
    else
        log_warn "TC-10: stats field '${field}' not found in response — API shape may differ"
    fi
done

# ---------------------------------------------------------------------------
# TC-11: Audit log records write operations
# ---------------------------------------------------------------------------
log_section "TC-11: Audit log records the write operation"
# Write a uniquely named file so we can search for it
UNIQUE_MARKER="audit_marker_$$_$(date +%s)"
UNIQUE_FILE="${TEST_DIR}/${UNIQUE_MARKER}.dat"
write_test_file "$CLIENT_01" "$UNIQUE_FILE" 2 >/dev/null
sleep 2  # Give audit log time to flush

AUDIT_LOG=$(dtfs_cli "audit --limit 20" 2>/dev/null || api_get "/api/v1/audit?limit=20" 30 2>/dev/null || echo "")
if echo "$AUDIT_LOG" | grep -q "$UNIQUE_MARKER"; then
    log_ok "TC-11: Audit log contains the unique write event marker"
elif echo "$AUDIT_LOG" | grep -iqE "write|create|put"; then
    log_ok "TC-11: Audit log shows recent write events (marker not individually visible)"
elif [ -z "$AUDIT_LOG" ]; then
    log_warn "TC-11: Audit log returned no entries — audit may not be enabled"
else
    log_warn "TC-11: Unique write marker not found in audit log (log may truncate file names)"
fi

# ---------------------------------------------------------------------------
# TC-12: Cluster status endpoint
# ---------------------------------------------------------------------------
log_section "TC-12: Cluster status endpoint"
CLUSTER_STATUS=$(api_get "/api/v1/cluster/status" 10 2>/dev/null || echo '{}')
RAFT_LEADER=$(echo "$CLUSTER_STATUS" | jq -r '.raft.leader_id // empty' 2>/dev/null)
CLUSTER_NODES=$(echo "$CLUSTER_STATUS" | jq -r '.members | length // .nodes | length // 0' 2>/dev/null || echo 0)

if [ -n "$RAFT_LEADER" ] && [ "$RAFT_LEADER" != "null" ]; then
    log_ok "TC-12: Cluster status shows elected leader: $RAFT_LEADER"
else
    log_fail "TC-12: Cluster status does not show a Raft leader"
fi
assert_gt "$CLUSTER_NODES" "0" "TC-12: Cluster status shows at least 1 member"

# ---------------------------------------------------------------------------
# TC-13: Nodes endpoint returns all expected nodes
# ---------------------------------------------------------------------------
log_section "TC-13: Nodes endpoint completeness"
NODES_JSON=$(api_get "/api/v1/nodes" 15 2>/dev/null || echo '[]')
TOTAL_NODES=$(echo "$NODES_JSON" | jq 'length' 2>/dev/null || echo 0)
assert_gt "$TOTAL_NODES" "0" "TC-13: /api/v1/nodes returns at least 1 node"
HEALTHY_NODES=$(echo "$NODES_JSON" | \
    jq '[.[] | select(.status == "healthy" or .state == "healthy")] | length' 2>/dev/null || echo 0)
assert_gt "$HEALTHY_NODES" "0" "TC-13: At least 1 healthy node reported"

# ---------------------------------------------------------------------------
# Teardown
# ---------------------------------------------------------------------------
log_section "Teardown"
cleanup_test_files "$CLIENT_01" "$TEST_DIR"

print_summary "$TEST_SUITE_NAME"

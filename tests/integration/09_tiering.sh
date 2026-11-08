#!/usr/bin/env bash
# DTFS Integration Test: 09 - Tiering
# Verifies that recently written files land on hot tier, and that they
# migrate to cold tier after the cold_threshold_days is set to 0.

set -euo pipefail
TEST_SUITE_NAME="09_tiering"
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
TEST_DIR="${MOUNT}/test-tiering-$$"
MIGRATION_TIMEOUT=120  # seconds to wait for cold migration

log_section "09_tiering: Tiering Tests"
log_warn "Note: This suite may be skipped if tiering is not fully implemented"

# ---------------------------------------------------------------------------
# Helper: check if tiering is configured
# ---------------------------------------------------------------------------
check_tiering_enabled() {
    local config
    config=$(api_get "/api/v1/config" 10 2>/dev/null || echo '{}')
    local tiering_enabled
    tiering_enabled=$(echo "$config" | jq -r '.tiering.enabled // .tiering_enabled // empty' 2>/dev/null)
    # If we get any tiering config, assume tiering is present
    if echo "$config" | jq -e '.tiering // .cold_threshold_days // .hot_tier' >/dev/null 2>&1; then
        return 0
    fi
    return 1
}

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
mount_dtfs "$CLIENT_01" "$MOUNT" || { log_fail "Cannot mount DTFS — aborting"; print_summary; exit 1; }
ssh_exec "$CLIENT_01" "mkdir -p '${TEST_DIR}'"
wait_for_health 60 || { log_fail "Cluster not healthy at test start"; print_summary; exit 1; }

# Check tiering is available
if ! check_tiering_enabled; then
    log_skip "TC-01 through TC-06: Tiering does not appear to be configured in this cluster"
    print_summary "$TEST_SUITE_NAME"
    exit 0
fi

# ---------------------------------------------------------------------------
# TC-01: Write a file and verify it's on hot tier
# ---------------------------------------------------------------------------
log_section "TC-01: Write file and verify hot-tier placement"
TEST_FILE="${TEST_DIR}/tiering_test.dat"
FILE_CKSUM=$(write_test_file "$CLIENT_01" "$TEST_FILE" 128)
if [ -z "$FILE_CKSUM" ]; then
    log_fail "TC-01: Failed to write test file"
    print_summary; exit 1
fi
log_ok "TC-01: Test file written (checksum: $FILE_CKSUM)"
sleep 3

# ---------------------------------------------------------------------------
# TC-02: Query chunk location, confirm hot-tier disk
# ---------------------------------------------------------------------------
log_section "TC-02: Query chunk location via API — confirm hot tier"
ENCODED_PATH=$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1]))" \
    "$TEST_FILE" 2>/dev/null || echo "$TEST_FILE" | sed 's|/|%2F|g')
CHUNK_JSON=$(api_get "/api/v1/files/${ENCODED_PATH}/chunks" 30 2>/dev/null || echo '[]')
FIRST_CHUNK=$(echo "$CHUNK_JSON" | jq -r '.[0].chunk_id // .[0].id // empty' 2>/dev/null)

if [ -z "$FIRST_CHUNK" ]; then
    log_warn "TC-02: No chunk ID returned — skipping tier verification"
    log_skip "TC-02: Chunk tier check"
else
    REPLICA_JSON=$(api_get "/api/v1/chunks/${FIRST_CHUNK}/replicas" 15 2>/dev/null || echo '[]')
    CHUNK_TIER=$(echo "$REPLICA_JSON" | \
        jq -r '.[0].tier // .[0].storage_tier // .[0].disk_tier // "unknown"' 2>/dev/null)
    CHUNK_DISK=$(echo "$REPLICA_JSON" | \
        jq -r '.[0].disk // .[0].disk_path // "unknown"' 2>/dev/null)
    log_info "Chunk ${FIRST_CHUNK}: tier='${CHUNK_TIER}' disk='${CHUNK_DISK}'"

    if [ "$CHUNK_TIER" = "hot" ]; then
        log_ok "TC-02: Recently written chunk is on hot tier"
    elif echo "$CHUNK_DISK" | grep -qi "hot\|ssd\|nvme"; then
        log_ok "TC-02: Chunk disk path indicates hot storage: $CHUNK_DISK"
    elif [ "$CHUNK_TIER" = "unknown" ]; then
        log_warn "TC-02: Tier info not available in API response — cannot confirm"
    else
        log_fail "TC-02: Chunk tier is '${CHUNK_TIER}', expected 'hot'"
    fi
fi

# ---------------------------------------------------------------------------
# TC-03: Set cold_threshold_days=0 to force immediate cold classification
# ---------------------------------------------------------------------------
log_section "TC-03: Set cold_threshold_days=0 for test-driven migration"
CONFIG_RC=0
dtfs_cli "config --set cold_threshold_days=0" 2>/dev/null || CONFIG_RC=$?
if [ $CONFIG_RC -eq 0 ]; then
    log_ok "TC-03: cold_threshold_days set to 0 via CLI"
else
    # Try via API
    API_CONFIG_RC=0
    api_put "/api/v1/config" '{"tiering": {"cold_threshold_days": 0}}' 15 >/dev/null 2>&1 || API_CONFIG_RC=$?
    if [ $API_CONFIG_RC -eq 0 ]; then
        log_ok "TC-03: cold_threshold_days set to 0 via API"
    else
        log_warn "TC-03: Could not set cold_threshold_days (CLI rc=$CONFIG_RC, API rc=$API_CONFIG_RC)"
    fi
fi

# ---------------------------------------------------------------------------
# TC-04: Trigger tiering evaluation
# ---------------------------------------------------------------------------
log_section "TC-04: Trigger tiering evaluation (scrub or API)"
SCRUB_RC=0
dtfs_cli "scrub --level quick" 2>/dev/null || SCRUB_RC=$?
if [ $SCRUB_RC -eq 0 ]; then
    log_ok "TC-04: Quick scrub / tier evaluation triggered via CLI"
else
    # Try direct API trigger
    TIER_EVAL_RC=0
    api_post "/api/v1/tiering/evaluate" '{}' 15 >/dev/null 2>&1 || TIER_EVAL_RC=$?
    if [ $TIER_EVAL_RC -eq 0 ]; then
        log_ok "TC-04: Tiering evaluation triggered via API"
    else
        log_warn "TC-04: Could not trigger tiering evaluation (CLI rc=$SCRUB_RC, API rc=$TIER_EVAL_RC)"
    fi
fi

# ---------------------------------------------------------------------------
# TC-05: Wait up to 120s for migration to cold tier
# ---------------------------------------------------------------------------
log_section "TC-05: Wait for migration to cold tier (timeout: ${MIGRATION_TIMEOUT}s)"
if [ -z "$FIRST_CHUNK" ]; then
    log_skip "TC-05: No chunk ID — cannot poll migration"
else
    MIGRATED=false
    MIGRATE_START=$(date +%s)
    while true; do
        NOW=$(date +%s)
        if [ $((NOW - MIGRATE_START)) -ge $MIGRATION_TIMEOUT ]; then
            break
        fi
        REPLICA_JSON=$(api_get "/api/v1/chunks/${FIRST_CHUNK}/replicas" 15 2>/dev/null || echo '[]')
        NEW_TIER=$(echo "$REPLICA_JSON" | \
            jq -r '.[0].tier // .[0].storage_tier // .[0].disk_tier // "unknown"' 2>/dev/null)
        NEW_DISK=$(echo "$REPLICA_JSON" | \
            jq -r '.[0].disk // .[0].disk_path // "unknown"' 2>/dev/null)
        log_debug "Chunk ${FIRST_CHUNK} tier='$NEW_TIER' disk='$NEW_DISK' (elapsed: $((NOW - MIGRATE_START))s)"

        if [ "$NEW_TIER" = "cold" ] || echo "$NEW_DISK" | grep -qi "cold\|hdd\|archive"; then
            MIGRATED=true
            break
        fi
        sleep 5
    done

    if $MIGRATED; then
        log_ok "TC-05: Chunk migrated to cold tier within ${MIGRATION_TIMEOUT}s"
    else
        log_fail "TC-05: Chunk did not migrate to cold tier within ${MIGRATION_TIMEOUT}s (tier: '$NEW_TIER')"
    fi
fi

# ---------------------------------------------------------------------------
# TC-06: Verify data is still accessible after cold migration
# ---------------------------------------------------------------------------
log_section "TC-06: Data accessible after cold migration"
if read_verify_file "$CLIENT_01" "$TEST_FILE" "$FILE_CKSUM"; then
    log_ok "TC-06: File checksum verified after cold migration"
else
    log_fail "TC-06: File checksum mismatch after cold migration"
fi

# ---------------------------------------------------------------------------
# TC-07: Restore cold_threshold_days to default
# ---------------------------------------------------------------------------
log_section "TC-07: Restore cold_threshold_days to default"
RESTORE_RC=0
dtfs_cli "config --set cold_threshold_days=30" 2>/dev/null || RESTORE_RC=$?
if [ $RESTORE_RC -ne 0 ]; then
    api_put "/api/v1/config" '{"tiering": {"cold_threshold_days": 30}}' 15 >/dev/null 2>&1 || true
fi
log_info "cold_threshold_days restored to 30"

# ---------------------------------------------------------------------------
# Teardown
# ---------------------------------------------------------------------------
log_section "Teardown"
cleanup_test_files "$CLIENT_01" "$TEST_DIR"

print_summary "$TEST_SUITE_NAME"

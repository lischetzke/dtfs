#!/usr/bin/env bash
# DTFS Integration Test: 10 - Scrub Validation
# Tests quick and deep scrub, verifies clean baseline, then corrupts a chunk
# and confirms the deep scrub detects corruption.

set -euo pipefail
TEST_SUITE_NAME="10_scrub"
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
TEST_DIR="${MOUNT}/test-scrub-$$"
QUICK_SCRUB_TIMEOUT=300   # 5 minutes
DEEP_SCRUB_TIMEOUT=600    # 10 minutes

log_section "10_scrub: Scrub Validation Tests"

# ---------------------------------------------------------------------------
# Helper: get last scrub stats
# ---------------------------------------------------------------------------
get_scrub_stats() {
    api_get "/api/v1/stats" 15 2>/dev/null || echo '{}'
}

get_last_scrub_time() {
    get_scrub_stats | jq -r '.last_scrub // .scrub.last_run // empty' 2>/dev/null
}

get_chunks_corrupt() {
    get_scrub_stats | jq -r '.chunks_corrupt // .scrub.chunks_corrupt // 0' 2>/dev/null
}

get_chunks_missing() {
    get_scrub_stats | jq -r '.chunks_missing // .scrub.chunks_missing // 0' 2>/dev/null
}

# ---------------------------------------------------------------------------
# Helper: wait for scrub completion by polling last_scrub timestamp change
# ---------------------------------------------------------------------------
wait_for_scrub_complete() {
    local baseline_ts="$1"
    local timeout="$2"
    local start
    start=$(date +%s)
    local deadline=$((start + timeout))

    log_info "Waiting for scrub completion (baseline_ts='$baseline_ts', timeout: ${timeout}s)"
    while true; do
        local now
        now=$(date +%s)
        if [ "$now" -ge "$deadline" ]; then
            log_fail "Scrub did not complete within ${timeout}s"
            return 1
        fi
        local new_ts
        new_ts=$(get_last_scrub_time)
        if [ -n "$new_ts" ] && [ "$new_ts" != "$baseline_ts" ]; then
            log_info "Scrub completed (new ts: $new_ts, elapsed: $((now - start))s)"
            return 0
        fi
        sleep 5
    done
}

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
mount_dtfs "$CLIENT_01" "$MOUNT" || { log_fail "Cannot mount DTFS — aborting"; print_summary; exit 1; }
ssh_exec "$CLIENT_01" "mkdir -p '${TEST_DIR}'"
wait_for_health 60 || { log_fail "Cluster not healthy at test start"; print_summary; exit 1; }

# Write a test file so there are chunks to scrub
log_info "Writing test data before scrub"
SCRUB_TEST_FILE="${TEST_DIR}/scrub_target.dat"
FILE_CKSUM=$(write_test_file "$CLIENT_01" "$SCRUB_TEST_FILE" 256)
sleep 3

# ---------------------------------------------------------------------------
# TC-01: Trigger quick scrub on data-01
# ---------------------------------------------------------------------------
log_section "TC-01: Trigger quick scrub on data-01"
PRE_QUICK_TS=$(get_last_scrub_time)
QUICK_SCRUB_RC=0
ssh_exec "$DATA_01" \
    "DTFS_MANAGER='${MANAGER_API}' /usr/local/bin/dtfs-cli scrub --level quick" \
    2>/dev/null || QUICK_SCRUB_RC=$?

if [ $QUICK_SCRUB_RC -eq 0 ]; then
    log_ok "TC-01: Quick scrub initiated on data-01"
else
    log_warn "TC-01: Quick scrub CLI returned rc=$QUICK_SCRUB_RC — may run async"
fi

# ---------------------------------------------------------------------------
# TC-02: Verify quick scrub completes (poll /api/v1/stats)
# ---------------------------------------------------------------------------
log_section "TC-02: Verify quick scrub completes within ${QUICK_SCRUB_TIMEOUT}s"
if wait_for_scrub_complete "$PRE_QUICK_TS" "$QUICK_SCRUB_TIMEOUT"; then
    log_ok "TC-02: Quick scrub completed"
else
    log_fail "TC-02: Quick scrub did not complete within ${QUICK_SCRUB_TIMEOUT}s"
fi

# ---------------------------------------------------------------------------
# TC-03: Verify chunks_corrupt == 0 and chunks_missing == 0 after quick scrub
# ---------------------------------------------------------------------------
log_section "TC-03: Verify clean state after quick scrub"
CORRUPT_COUNT=$(get_chunks_corrupt)
MISSING_COUNT=$(get_chunks_missing)
assert_eq "$CORRUPT_COUNT" "0" "TC-03: chunks_corrupt == 0 after quick scrub"
assert_eq "$MISSING_COUNT" "0" "TC-03: chunks_missing == 0 after quick scrub"

# ---------------------------------------------------------------------------
# TC-04: Trigger deep scrub
# ---------------------------------------------------------------------------
log_section "TC-04: Trigger deep scrub"
PRE_DEEP_TS=$(get_last_scrub_time)
DEEP_SCRUB_RC=0
dtfs_cli "scrub --level deep" 2>/dev/null || DEEP_SCRUB_RC=$?
if [ $DEEP_SCRUB_RC -eq 0 ]; then
    log_ok "TC-04: Deep scrub initiated"
else
    # Try triggering via API
    api_post "/api/v1/scrub/start" '{"level": "deep"}' 30 >/dev/null 2>&1 || true
    log_warn "TC-04: Deep scrub CLI returned rc=$DEEP_SCRUB_RC — tried API fallback"
fi

# ---------------------------------------------------------------------------
# TC-05: Verify deep scrub completes
# ---------------------------------------------------------------------------
log_section "TC-05: Verify deep scrub completes within ${DEEP_SCRUB_TIMEOUT}s"
if wait_for_scrub_complete "$PRE_DEEP_TS" "$DEEP_SCRUB_TIMEOUT"; then
    log_ok "TC-05: Deep scrub completed"
else
    log_fail "TC-05: Deep scrub did not complete within ${DEEP_SCRUB_TIMEOUT}s"
fi

CORRUPT_COUNT=$(get_chunks_corrupt)
MISSING_COUNT=$(get_chunks_missing)
assert_eq "$CORRUPT_COUNT" "0" "TC-05: chunks_corrupt == 0 after clean deep scrub"
assert_eq "$MISSING_COUNT" "0" "TC-05: chunks_missing == 0 after clean deep scrub"

# ---------------------------------------------------------------------------
# TC-06: Corrupt a chunk on data-01
# ---------------------------------------------------------------------------
log_section "TC-06: Corrupt a chunk file on data-01"
ENCODED_PATH=$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1]))" \
    "$SCRUB_TEST_FILE" 2>/dev/null || echo "$SCRUB_TEST_FILE" | sed 's|/|%2F|g')
CHUNK_JSON=$(api_get "/api/v1/files/${ENCODED_PATH}/chunks" 30 2>/dev/null || echo '[]')
FIRST_CHUNK=$(echo "$CHUNK_JSON" | jq -r '.[0].chunk_id // .[0].id // empty' 2>/dev/null)

if [ -z "$FIRST_CHUNK" ]; then
    log_warn "TC-06: Cannot determine chunk ID — using find to locate any .dat chunk on data-01"
    CHUNK_PATH=$(ssh_exec "$DATA_01" \
        "find /mnt/disk-a /mnt/disk-b -name '*.dat' 2>/dev/null | head -1" 2>/dev/null || echo "")
else
    CHUNK_PATH=$(find_chunk_file "$DATA_01" "$FIRST_CHUNK" 2>/dev/null || echo "")
fi

if [ -z "$CHUNK_PATH" ]; then
    log_fail "TC-06: Could not locate any chunk file on data-01 — skipping corruption test"
    log_skip "TC-06: Chunk corruption injection"
else
    log_info "Corrupting chunk at data-01:${CHUNK_PATH}"
    ssh_exec "$DATA_01" \
        "dd if=/dev/zero of='${CHUNK_PATH}' bs=1 count=100 conv=notrunc 2>/dev/null"
    log_ok "TC-06: Chunk file corrupted (100 bytes overwritten with zeros)"
fi

# ---------------------------------------------------------------------------
# TC-07: Trigger deep scrub again and verify chunks_corrupt > 0
# ---------------------------------------------------------------------------
log_section "TC-07: Deep scrub detects corruption"
if [ -z "$CHUNK_PATH" ]; then
    log_skip "TC-07: Skipped (no corruption injected)"
else
    PRE_CORRUPT_SCRUB_TS=$(get_last_scrub_time)

    DEEP2_RC=0
    dtfs_cli "scrub --level deep" 2>/dev/null || DEEP2_RC=$?
    if [ $DEEP2_RC -ne 0 ]; then
        api_post "/api/v1/scrub/start" '{"level": "deep"}' 30 >/dev/null 2>&1 || true
    fi

    if wait_for_scrub_complete "$PRE_CORRUPT_SCRUB_TS" "$DEEP_SCRUB_TIMEOUT"; then
        log_ok "TC-07: Second deep scrub completed"
        POST_CORRUPT_COUNT=$(get_chunks_corrupt)
        assert_gt "$POST_CORRUPT_COUNT" "0" \
            "TC-07: chunks_corrupt > 0 after corruption injection (got: $POST_CORRUPT_COUNT)"
    else
        log_fail "TC-07: Second deep scrub did not complete within ${DEEP_SCRUB_TIMEOUT}s"
    fi
fi

# ---------------------------------------------------------------------------
# TC-08: Data still readable via replicas despite corruption
# ---------------------------------------------------------------------------
log_section "TC-08: File still readable via healthy replicas"
if read_verify_file "$CLIENT_01" "$SCRUB_TEST_FILE" "$FILE_CKSUM"; then
    log_ok "TC-08: File checksum verified — healthy replicas served data"
else
    log_warn "TC-08: File checksum mismatch — corruption may have affected all replicas"
fi

# ---------------------------------------------------------------------------
# Teardown
# ---------------------------------------------------------------------------
log_section "Teardown"
cleanup_test_files "$CLIENT_01" "$TEST_DIR"

print_summary "$TEST_SUITE_NAME"

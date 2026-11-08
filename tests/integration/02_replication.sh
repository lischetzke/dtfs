#!/usr/bin/env bash
# DTFS Integration Test: 02 - Replication Verification
# Verifies that written files have the correct number of replicas spread across
# distinct hosts, and that hot chunks land on hot-tier nodes.

set -euo pipefail
TEST_SUITE_NAME="02_replication"
source "$(dirname "$0")/lib/common.sh"

MANAGER_API="${MANAGER_API:-http://10.0.1.1:9402}"
SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=10"

MGR1="mgr-01"
MGR2="mgr-02"
MGR3="mgr-03"
DATA1="data-01"
DATA2="data-02"
CLIENT1="client-01"

MOUNT="${TEST_MOUNT:-/mnt/dtfs}"
TEST_DIR="${MOUNT}/test-replication-$$"
EXPECTED_REPLICA_COUNT="${DTFS_REPLICA_COUNT:-3}"

log_section "02_replication: Replication Verification Tests"

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
log_info "Ensuring DTFS is mounted on ${CLIENT1}"
mount_dtfs "$CLIENT_01" "$MOUNT" || {
    log_fail "Cannot mount DTFS — aborting"
    print_summary; exit 1
}
ssh_exec "$CLIENT_01" "mkdir -p '${TEST_DIR}'"

# ---------------------------------------------------------------------------
# TC-01: Write a test file and confirm it appears in the API
# ---------------------------------------------------------------------------
log_section "TC-01: Write test file"
TEST_FILE="${TEST_DIR}/repl_test_256mb.dat"
CKSUM=$(write_test_file "$CLIENT_01" "$TEST_FILE" 256)
if [ -z "$CKSUM" ]; then
    log_fail "TC-01: Failed to write test file"
    print_summary; exit 1
fi
log_ok "TC-01: Test file written (checksum: $CKSUM)"

# Give the manager a moment to register the chunks
sleep 3

# ---------------------------------------------------------------------------
# TC-02: Query /api/v1/files, confirm file is listed and has chunk info
# ---------------------------------------------------------------------------
log_section "TC-02: Query chunk info via API"

# Encode path for URL
ENCODED_PATH=$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1]))" \
    "$TEST_FILE" 2>/dev/null || echo "$TEST_FILE" | sed 's|/|%2F|g')

CHUNK_JSON=$(api_get "/api/v1/files/${ENCODED_PATH}/chunks" 30 2>/dev/null || echo '[]')
CHUNK_COUNT=$(echo "$CHUNK_JSON" | jq 'length' 2>/dev/null || echo 0)

assert_gt "$CHUNK_COUNT" "0" "TC-02: File has at least 1 chunk registered"
log_info "File has $CHUNK_COUNT chunk(s)"

# Also verify via flat file list
FILES_JSON=$(api_get "/api/v1/files" 30 2>/dev/null || echo '[]')
assert_gt "$(echo "$FILES_JSON" | jq 'length' 2>/dev/null || echo 0)" \
    "0" "TC-02: /api/v1/files returns at least one entry"

# ---------------------------------------------------------------------------
# TC-03: For each chunk, verify it appears on expected number of nodes
# ---------------------------------------------------------------------------
log_section "TC-03: Replica count per chunk"

CHUNK_IDS=$(echo "$CHUNK_JSON" | jq -r '.[].chunk_id // .[].id' 2>/dev/null)
if [ -z "$CHUNK_IDS" ]; then
    log_warn "TC-03: No chunk IDs found in API response — skipping per-chunk checks"
    log_skip "TC-03: chunk replica count (no chunk IDs available)"
else
    ALL_CHUNKS_OK=true
    while IFS= read -r chunk_id; do
        [ -z "$chunk_id" ] && continue
        REPLICA_JSON=$(api_get "/api/v1/chunks/${chunk_id}/replicas" 15 2>/dev/null || echo '[]')
        ACTUAL_REPLICAS=$(echo "$REPLICA_JSON" | jq 'length' 2>/dev/null || echo 0)
        if wait_for_replication "$chunk_id" "$EXPECTED_REPLICA_COUNT" 60; then
            log_ok "TC-03: Chunk ${chunk_id}: ${EXPECTED_REPLICA_COUNT} replicas present"
        else
            log_fail "TC-03: Chunk ${chunk_id}: expected ${EXPECTED_REPLICA_COUNT} replicas, got ${ACTUAL_REPLICAS}"
            ALL_CHUNKS_OK=false
        fi
    done <<< "$CHUNK_IDS"
fi

# ---------------------------------------------------------------------------
# TC-04: Verify replicas are on different hosts
# ---------------------------------------------------------------------------
log_section "TC-04: Replicas on distinct hosts"

if [ -z "$CHUNK_IDS" ]; then
    log_skip "TC-04: distinct host check (no chunk IDs)"
else
    while IFS= read -r chunk_id; do
        [ -z "$chunk_id" ] && continue
        REPLICA_JSON=$(api_get "/api/v1/chunks/${chunk_id}/replicas" 15 2>/dev/null || echo '[]')
        # Extract all node hostnames
        HOSTS=$(echo "$REPLICA_JSON" | jq -r '.[].node_hostname // .[].host // ""' 2>/dev/null | sort)
        UNIQUE_HOSTS=$(echo "$HOSTS" | sort -u | grep -c . 2>/dev/null || echo 0)
        TOTAL_HOSTS=$(echo "$HOSTS" | grep -c . 2>/dev/null || echo 0)

        if [ "$UNIQUE_HOSTS" -eq "$TOTAL_HOSTS" ] && [ "$UNIQUE_HOSTS" -gt 1 ]; then
            log_ok "TC-04: Chunk ${chunk_id}: replicas on ${UNIQUE_HOSTS} distinct hosts"
        elif [ "$UNIQUE_HOSTS" -le 1 ]; then
            log_fail "TC-04: Chunk ${chunk_id}: all replicas on same host (got ${UNIQUE_HOSTS} unique)"
        else
            log_fail "TC-04: Chunk ${chunk_id}: duplicate host detected (${UNIQUE_HOSTS} unique / ${TOTAL_HOSTS} total)"
        fi
    done <<< "$CHUNK_IDS"
fi

# ---------------------------------------------------------------------------
# TC-05: Verify hot chunks are placed on hot-tier nodes
# ---------------------------------------------------------------------------
log_section "TC-05: Hot chunks on hot-tier nodes"

NODES_JSON=$(api_get "/api/v1/nodes" 15 2>/dev/null || echo '[]')
HOT_NODES=$(echo "$NODES_JSON" | jq -r '.[] | select(.tier == "hot" or .storage_tier == "hot") | .hostname // .id' 2>/dev/null)

if [ -z "$HOT_NODES" ]; then
    log_skip "TC-05: No hot-tier nodes reported by API — tiering may not be configured"
else
    log_info "Hot-tier nodes: $(echo "$HOT_NODES" | tr '\n' ' ')"

    if [ -z "$CHUNK_IDS" ]; then
        log_skip "TC-05: hot-tier check (no chunk IDs)"
    else
        # Check the first chunk only (recently written = hot access)
        FIRST_CHUNK=$(echo "$CHUNK_IDS" | head -1)
        REPLICA_JSON=$(api_get "/api/v1/chunks/${FIRST_CHUNK}/replicas" 15 2>/dev/null || echo '[]')
        CHUNK_TIER=$(echo "$REPLICA_JSON" | jq -r '.[0].tier // .[0].storage_tier // "unknown"' 2>/dev/null)

        if [ "$CHUNK_TIER" = "hot" ]; then
            log_ok "TC-05: Recently written chunk is on hot tier"
        elif [ "$CHUNK_TIER" = "unknown" ]; then
            log_warn "TC-05: Tier information not available in replica API response"
        else
            log_fail "TC-05: Recently written chunk has tier='${CHUNK_TIER}', expected 'hot'"
        fi
    fi
fi

# ---------------------------------------------------------------------------
# TC-06: Verify /api/v1/nodes reports data nodes as healthy
# ---------------------------------------------------------------------------
log_section "TC-06: Data nodes reported healthy"
if [ -z "$NODES_JSON" ] || [ "$NODES_JSON" = "[]" ]; then
    NODES_JSON=$(api_get "/api/v1/nodes" 15 2>/dev/null || echo '[]')
fi

DATA_NODE_COUNT=$(echo "$NODES_JSON" | jq '[.[] | select(.role == "data" or .type == "data")] | length' 2>/dev/null || echo 0)
HEALTHY_DATA_COUNT=$(echo "$NODES_JSON" | \
    jq '[.[] | select((.role == "data" or .type == "data") and (.status == "healthy" or .state == "healthy"))] | length' \
    2>/dev/null || echo 0)

if [ "$DATA_NODE_COUNT" -gt 0 ]; then
    assert_eq "$HEALTHY_DATA_COUNT" "$DATA_NODE_COUNT" \
        "TC-06: All $DATA_NODE_COUNT data nodes are healthy"
else
    log_warn "TC-06: API did not return role-tagged data nodes — checking total node health"
    HEALTHY_COUNT=$(echo "$NODES_JSON" | \
        jq '[.[] | select(.status == "healthy" or .state == "healthy")] | length' 2>/dev/null || echo 0)
    assert_gt "$HEALTHY_COUNT" "0" "TC-06: At least one healthy node reported"
fi

# ---------------------------------------------------------------------------
# Teardown
# ---------------------------------------------------------------------------
log_section "Teardown"
cleanup_test_files "$CLIENT_01" "$TEST_DIR"

print_summary "$TEST_SUITE_NAME"

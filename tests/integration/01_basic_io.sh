#!/usr/bin/env bash
# DTFS Integration Test: 01 - Basic I/O
# Tests fundamental file operations: write, read-back checksum, list, rename, delete.

set -euo pipefail
TEST_SUITE_NAME="01_basic_io"
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
TEST_DIR="${MOUNT}/test-basic-io-$$"

log_section "01_basic_io: Basic File I/O Tests"

# ---------------------------------------------------------------------------
# Setup: mount DTFS on client-01
# ---------------------------------------------------------------------------
log_info "Ensuring DTFS is mounted on ${CLIENT1}"
if ! mount_dtfs "$CLIENT_01" "$MOUNT"; then
    log_fail "Cannot mount DTFS on ${CLIENT1} — aborting suite"
    print_summary
    exit 1
fi

ssh_exec "$CLIENT_01" "mkdir -p '${TEST_DIR}'"
log_info "Test directory: ${TEST_DIR}"

# ---------------------------------------------------------------------------
# TC-01: Write 128 MB file (single chunk), verify checksum
# ---------------------------------------------------------------------------
log_section "TC-01: Write 128 MB single-chunk file"
FILE_128="${TEST_DIR}/file_128mb.dat"
CKSUM_128=$(write_test_file "$CLIENT_01" "$FILE_128" 128)
if read_verify_file "$CLIENT_01" "$FILE_128" "$CKSUM_128"; then
    log_ok "TC-01: 128 MB write + checksum verified"
else
    log_fail "TC-01: 128 MB checksum mismatch"
fi

# ---------------------------------------------------------------------------
# TC-02: Write 512 MB file (multi-chunk), verify checksum
# ---------------------------------------------------------------------------
log_section "TC-02: Write 512 MB multi-chunk file"
FILE_512="${TEST_DIR}/file_512mb.dat"
CKSUM_512=$(write_test_file "$CLIENT_01" "$FILE_512" 512)
if read_verify_file "$CLIENT_01" "$FILE_512" "$CKSUM_512"; then
    log_ok "TC-02: 512 MB write + checksum verified"
else
    log_fail "TC-02: 512 MB checksum mismatch"
fi

# ---------------------------------------------------------------------------
# TC-03: Write 1 GB file, verify checksum
# ---------------------------------------------------------------------------
log_section "TC-03: Write 1 GB file"
FILE_1G="${TEST_DIR}/file_1gb.dat"
CKSUM_1G=$(write_test_file "$CLIENT_01" "$FILE_1G" 1024)
if read_verify_file "$CLIENT_01" "$FILE_1G" "$CKSUM_1G"; then
    log_ok "TC-03: 1 GB write + checksum verified"
else
    log_fail "TC-03: 1 GB checksum mismatch"
fi

# ---------------------------------------------------------------------------
# TC-04: List directory, verify files appear
# ---------------------------------------------------------------------------
log_section "TC-04: Directory listing"
DIR_LISTING=$(ssh_exec "$CLIENT_01" "ls '${TEST_DIR}'")
for fname in "file_128mb.dat" "file_512mb.dat" "file_1gb.dat"; do
    assert_contains "$DIR_LISTING" "$fname" "TC-04: '$fname' present in directory listing"
done

# ---------------------------------------------------------------------------
# TC-05: Rename file, verify old name gone and new name present
# ---------------------------------------------------------------------------
log_section "TC-05: Rename file"
RENAME_SRC="${TEST_DIR}/file_128mb.dat"
RENAME_DST="${TEST_DIR}/file_128mb_renamed.dat"
ssh_exec "$CLIENT_01" "mv '${RENAME_SRC}' '${RENAME_DST}'"
DIR_AFTER_RENAME=$(ssh_exec "$CLIENT_01" "ls '${TEST_DIR}'")
assert_not_contains "$DIR_AFTER_RENAME" "file_128mb.dat"      "TC-05: Old name absent after rename"
assert_contains     "$DIR_AFTER_RENAME" "file_128mb_renamed.dat" "TC-05: New name present after rename"
# Verify checksum of renamed file still matches
if read_verify_file "$CLIENT_01" "$RENAME_DST" "$CKSUM_128"; then
    log_ok "TC-05: Renamed file checksum intact"
else
    log_fail "TC-05: Renamed file checksum mismatch"
fi

# ---------------------------------------------------------------------------
# TC-06: Delete file, verify gone
# ---------------------------------------------------------------------------
log_section "TC-06: Delete file"
ssh_exec "$CLIENT_01" "rm -f '${RENAME_DST}'"
DIR_AFTER_DEL=$(ssh_exec "$CLIENT_01" "ls '${TEST_DIR}'")
assert_not_contains "$DIR_AFTER_DEL" "file_128mb_renamed.dat" "TC-06: Deleted file absent from listing"
# Attempt to stat the file — should fail
DELETE_RC=0
ssh_exec "$CLIENT_01" "stat '${RENAME_DST}'" 2>/dev/null || DELETE_RC=$?
assert_nonzero_exit "$DELETE_RC" "TC-06: stat of deleted file returns error"

# ---------------------------------------------------------------------------
# TC-07: Write 100 small files (1 MB each), verify all present
# ---------------------------------------------------------------------------
log_section "TC-07: Write 100 x 1 MB small files"
SMALL_DIR="${TEST_DIR}/small_files"
ssh_exec "$CLIENT_01" "mkdir -p '${SMALL_DIR}'"
SMALL_WRITE_FAIL=0
for i in $(seq -w 1 100); do
    SMALL_PATH="${SMALL_DIR}/small_${i}.dat"
    CKSUM=$(write_test_file "$CLIENT_01" "$SMALL_PATH" 1)
    if ! read_verify_file "$CLIENT_01" "$SMALL_PATH" "$CKSUM"; then
        log_warn "TC-07: Checksum mismatch for small_${i}.dat"
        SMALL_WRITE_FAIL=$((SMALL_WRITE_FAIL + 1))
    fi
done
if [ "$SMALL_WRITE_FAIL" -eq 0 ]; then
    log_ok "TC-07: All 100 small files written and verified"
else
    log_fail "TC-07: $SMALL_WRITE_FAIL / 100 small files failed checksum"
fi

# Verify count via ls
SMALL_COUNT=$(ssh_exec "$CLIENT_01" "ls '${SMALL_DIR}' | wc -l | tr -d ' '")
assert_eq "$SMALL_COUNT" "100" "TC-07: Directory count is 100"

# ---------------------------------------------------------------------------
# TC-08: Verify file count via API: GET /api/v1/files
# ---------------------------------------------------------------------------
log_section "TC-08: Verify file count via API"
API_RESPONSE=$(api_get "/api/v1/files" 30 2>/dev/null || echo '[]')
API_FILE_COUNT=$(echo "$API_RESPONSE" | jq 'length' 2>/dev/null || echo "0")
assert_gt "$API_FILE_COUNT" "0" "TC-08: API /api/v1/files returns at least 1 file"

# Check that our test files are represented (look for test dir name in paths)
if echo "$API_RESPONSE" | jq -r '.[].path // .[].name // ""' 2>/dev/null | grep -q "test-basic-io"; then
    log_ok "TC-08: Test files visible in API response"
else
    log_warn "TC-08: Test files not individually identifiable in API listing (API may use internal IDs)"
fi

# ---------------------------------------------------------------------------
# Teardown
# ---------------------------------------------------------------------------
log_section "Teardown"
cleanup_test_files "$CLIENT_01" "$TEST_DIR"
log_info "Test directory cleaned up"

print_summary "$TEST_SUITE_NAME"

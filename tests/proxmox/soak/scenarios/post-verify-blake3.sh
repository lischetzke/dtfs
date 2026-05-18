#!/usr/bin/env bash
# Post-soak BLAKE3 corpus verification. Walks every file in $CORPUS_DIR
# and compares its BLAKE3 against the same path served via the FUSE
# mount on client-c (fastest path). Any mismatch is a hard FAIL.
#
# Usage: post-verify-blake3.sh CORPUS_DIR RESULTS_DIR
set -uo pipefail
CORPUS_DIR="${1:?corpus dir required}"
RESULTS_DIR="${2:?results dir required}"
CLIENT_C="${CLIENT_C:-10.150.0.162}"
SSH_USER="${SSH_USER:-debian}"
SSH_OPTS="-o StrictHostKeyChecking=no -o LogLevel=ERROR"
MNT=/mnt/dtfs

mismatches=0
missing=0
total=0

echo "=== post-verify-blake3 from $CORPUS_DIR ==="
echo "(comparing to FUSE on $CLIENT_C:$MNT)"

if ! command -v b3sum >/dev/null 2>&1; then
    echo "verdict=fail: b3sum not installed on control host"
    exit 1
fi

while IFS= read -r src; do
    rel="${src#$CORPUS_DIR/}"
    expect=$(b3sum --no-names "$src" 2>/dev/null | awk '{print $1}')
    actual=$(ssh $SSH_OPTS "$SSH_USER@$CLIENT_C" "b3sum --no-names '$MNT/$rel' 2>/dev/null" \
        | awk '{print $1}')
    total=$((total + 1))
    if [ -z "$actual" ]; then
        missing=$((missing + 1))
        echo "MISSING $rel"
        continue
    fi
    if [ "$expect" != "$actual" ]; then
        mismatches=$((mismatches + 1))
        echo "MISMATCH $rel"
        echo "  expect=$expect"
        echo "  actual=$actual"
    fi
done < <(find "$CORPUS_DIR" -type f \( -name '*.mkv' -o -name '*.mp4' \) | sort)

echo ""
echo "Total: $total"
echo "Mismatches: $mismatches"
echo "Missing: $missing"

if [ "$mismatches" -gt 0 ] || [ "$missing" -gt 0 ]; then
    echo "verdict=fail: $mismatches mismatches, $missing missing"
    exit 1
fi
echo "verdict=pass: all $total files match BLAKE3"

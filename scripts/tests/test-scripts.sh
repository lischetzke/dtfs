#!/usr/bin/env bash
# Test harness for the DTFS deployment scripts.
#
# Validates argument parsing, error paths, idempotency, and (where
# possible) the actual install + update flow against a temp directory.
#
# Run from project root:
#   bash scripts/tests/test-scripts.sh

set -u

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

PASS=0
FAIL=0

t() {
    local label=$1; shift
    if "$@" > /tmp/test-scripts.out 2>&1; then
        echo "  PASS  $label"
        PASS=$((PASS+1))
    else
        echo "  FAIL  $label: $(head -c 200 /tmp/test-scripts.out)"
        FAIL=$((FAIL+1))
    fi
}

t_fail() {
    local label=$1; shift
    if ! "$@" > /tmp/test-scripts.out 2>&1; then
        echo "  PASS  $label (correctly failed)"
        PASS=$((PASS+1))
    else
        echo "  FAIL  $label (should have failed but didn't)"
        FAIL=$((FAIL+1))
    fi
}

assert_contains() {
    local label=$1
    local hay=$2
    local needle=$3
    # Use `grep -- "$needle"` so leading dashes in the needle (e.g.
    # `--debug`) aren't parsed as grep options.
    if echo "$hay" | grep -qF -- "$needle"; then
        echo "  PASS  $label"
        PASS=$((PASS+1))
    else
        echo "  FAIL  $label: missing '$needle'"
        FAIL=$((FAIL+1))
    fi
}

echo "=== dtfs-build.sh ==="

# --help shows usage
HELP=$(bash "$SCRIPT_DIR/dtfs-build.sh" --help 2>&1 || true)
assert_contains "dtfs-build.sh --help shows usage"        "$HELP" "DTFS build orchestrator"
assert_contains "dtfs-build.sh --help shows --no-tests"   "$HELP" "--no-tests"
assert_contains "dtfs-build.sh --help shows --debug"      "$HELP" "--debug"

# unknown arg → exit 3
t_fail "dtfs-build.sh rejects unknown arg" \
    bash "$SCRIPT_DIR/dtfs-build.sh" --bogus

# bash -n syntax check
t "dtfs-build.sh syntax"  bash -n "$SCRIPT_DIR/dtfs-build.sh"

echo ""
echo "=== dtfs-install.sh ==="

HELP=$(bash "$SCRIPT_DIR/dtfs-install.sh" --help 2>&1 || true)
assert_contains "dtfs-install.sh --help shows usage"      "$HELP" "interactive installer"
assert_contains "dtfs-install.sh mentions manager role"   "$HELP" "manager"
assert_contains "dtfs-install.sh mentions data role"      "$HELP" "data"
assert_contains "dtfs-install.sh mentions fuse-client"    "$HELP" "fuse-client"

# Non-root must fail (we're not in sudo).
if [ "$(id -u)" -ne 0 ]; then
    t_fail "dtfs-install.sh refuses non-root" \
        bash "$SCRIPT_DIR/dtfs-install.sh" /tmp/nonexistent.tar.gz
fi

# Missing tarball → fail
t_fail "dtfs-install.sh rejects missing tarball" \
    sudo bash "$SCRIPT_DIR/dtfs-install.sh" /tmp/does-not-exist.tar.gz

t "dtfs-install.sh syntax"  bash -n "$SCRIPT_DIR/dtfs-install.sh"

echo ""
echo "=== dtfs-update.sh ==="

HELP=$(bash "$SCRIPT_DIR/dtfs-update.sh" --help 2>&1 || true)
assert_contains "dtfs-update.sh --help shows rolling"     "$HELP" "rolling-upgrade"
assert_contains "dtfs-update.sh shows --dry-run"          "$HELP" "--dry-run"
assert_contains "dtfs-update.sh shows --rollback"         "$HELP" "--rollback"
assert_contains "dtfs-update.sh mentions inventory"       "$HELP" "Inventory"

t_fail "dtfs-update.sh rejects missing tarball" \
    bash "$SCRIPT_DIR/dtfs-update.sh"

t_fail "dtfs-update.sh rejects nonexistent tarball" \
    bash "$SCRIPT_DIR/dtfs-update.sh" /tmp/does-not-exist.tar.gz

# dry-run accepts a valid-looking tarball and inventory.
TARBALL=$(mktemp --suffix=.tar.gz)
tar -czf "$TARBALL" -C /tmp --files-from /dev/null 2>/dev/null
trap "rm -f $TARBALL" EXIT
INV=$(mktemp --suffix=.yml)
cat > "$INV" <<'EOF'
all:
  children:
    managers:
      hosts:
        mgr-01:
          ansible_host: 10.0.0.1
    data_nodes:
      hosts:
        data-01:
          ansible_host: 10.0.0.2
    clients:
      hosts:
        client-01:
          ansible_host: 10.0.0.3
EOF
DRY_OUT=$(bash "$SCRIPT_DIR/dtfs-update.sh" --dry-run "$TARBALL" "$INV" 2>&1 || true)
rm -f "$INV"
assert_contains "dtfs-update.sh dry-run inventory parsed" "$DRY_OUT" "1 mgr, 1 data, 1 client"

t "dtfs-update.sh syntax"  bash -n "$SCRIPT_DIR/dtfs-update.sh"

echo ""
echo "=== Summary ==="
echo "  Total: $((PASS + FAIL))  PASS: $PASS  FAIL: $FAIL"
exit $FAIL

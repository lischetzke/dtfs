#!/usr/bin/env bash
# Run the full abuse-test suite and emit a summary table.

set -u
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export RESULTS_FILE=/tmp/dtfs-abuse-results.tsv
: > "$RESULTS_FILE"
printf "category\ttest\tresult\tduration_s\tdetail\n" > "$RESULTS_FILE"

SSH_KEY="${SSH_KEY:-$HOME/.ssh/dtfs-lab/id_ed25519}"
SSH_KH="${SSH_KH:-$HOME/.ssh/dtfs-lab/known_hosts}"
SSH_OPTS_LOCAL=(-i "$SSH_KEY" -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile="$SSH_KH")

ensure_client_healthy() {
    local cl=$1
    # FUSE mount is root-only (no allow_other) — use sudo for the probe.
    local probe
    probe=$(ssh "${SSH_OPTS_LOCAL[@]}" "$cl" "sudo timeout 5 ls /mnt/dtfs/ >/dev/null 2>&1 && sudo timeout 5 touch /mnt/dtfs/.heartbeat 2>/dev/null && sudo timeout 5 rm -f /mnt/dtfs/.heartbeat 2>/dev/null && echo OK" 2>/dev/null)
    if [ "$probe" = "OK" ]; then
        return 0
    fi
    echo "  ⚠ $cl FUSE unhealthy — recovering"
    ssh "${SSH_OPTS_LOCAL[@]}" "$cl" "sudo pkill -9 dtfs-fuse 2>/dev/null; sleep 2; sudo fusermount3 -uz /mnt/dtfs 2>/dev/null; sudo umount -l /mnt/dtfs 2>/dev/null; sudo systemctl reset-failed dtfs-fuse@mnt-dtfs.service 2>/dev/null; sudo systemctl restart dtfs-fuse@mnt-dtfs.service; sleep 5; sudo mountpoint -q /mnt/dtfs && echo recovered || echo failed" 2>&1 | tail -2
}

START=$(date +%s)

# REV-2-5 — was `0[1-9]-*.sh 10-*.sh`, which silently orphaned every
# bug-specific regression script from 11 upward (12-fuse-restart-after-crash,
# 15-bug-av, 16-bug-az, 17-bug-ba, 18-bug-bc, 19-bug-bh, 20-bug-bl, ...).
# Those are precisely the scripts that pin previously-shipped data-loss fixes,
# and nothing was running them.
for s in $(ls "$SCRIPT_DIR"/[0-9][0-9]-*.sh 2>/dev/null | sort); do
    [ -f "$s" ] || continue
    ensure_client_healthy client-01
    ensure_client_healthy client-02
    bash "$s" || true
done

END=$(date +%s)
ELAPSED=$((END - START))

echo ""
echo "═══════════════════════════════════════════════════════════════════"
echo " Abuse suite complete in ${ELAPSED}s — summary:"
echo "═══════════════════════════════════════════════════════════════════"
column -t -s $'\t' "$RESULTS_FILE"

echo ""
echo "═══════════════════════════════════════════════════════════════════"
pass=$(awk -F'\t' '$3=="PASS"' "$RESULTS_FILE" | wc -l)
fail=$(awk -F'\t' '$3=="FAIL"' "$RESULTS_FILE" | wc -l)
warn=$(awk -F'\t' '$3=="WARN"' "$RESULTS_FILE" | wc -l)
skip=$(awk -F'\t' '$3=="SKIP"' "$RESULTS_FILE" | wc -l)
total=$((pass + fail + warn + skip))
printf " Total: %d  PASS: %d  FAIL: %d  WARN: %d  SKIP: %d\n" "$total" "$pass" "$fail" "$warn" "$skip"
echo "═══════════════════════════════════════════════════════════════════"
echo ""
echo " Full TSV: $RESULTS_FILE"

if [ "$fail" -gt 0 ]; then
    echo ""
    echo "═══ FAILURES ═══"
    awk -F'\t' '$3=="FAIL"' "$RESULTS_FILE"
fi
if [ "$warn" -gt 0 ]; then
    echo ""
    echo "═══ WARNINGS ═══"
    awk -F'\t' '$3=="WARN"' "$RESULTS_FILE"
fi

exit "$fail"

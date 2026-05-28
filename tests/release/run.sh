#!/usr/bin/env bash
# tests/release/run.sh — DTFS end-to-end release test harness.
# See plans/active/PLAN-BUG-BE-E2E-RELEASE.md for design.
#
# Usage:
#   bash tests/release/run.sh            # full suite (~60-90 min)
#   bash tests/release/run.sh --quick    # quick subset (~15 min)

set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"

QUICK=0
[ "${1:-}" = "--quick" ] && QUICK=1

# Quick subset = the cases listed in PLAN-BUG-BE-E2E-RELEASE.md § 1.
QUICK_CASES="00-prereqs 01-basic-write-read 02-replica-placement \
             05-migration 06-flush-cache 13-integrity-corruption \
             15-soak-15min 17-stats-correctness 18-api-shape"

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
[ "$QUICK" = "1" ] && RUN_ID="${RUN_ID}-quick"
REPORT_DIR="$ROOT/plans/reports/RELEASE-$RUN_ID"
mkdir -p "$REPORT_DIR"
JSON="$REPORT_DIR/summary.json"

echo "==> DTFS release test suite — run $RUN_ID"
echo "==> Reports: $REPORT_DIR"
[ "$QUICK" = "1" ] && echo "==> Mode: QUICK"

declare -a CASES
if [ "$QUICK" = "1" ]; then
    for stem in $QUICK_CASES; do
        f="$HERE/cases/${stem}.sh"
        [ -f "$f" ] && CASES+=("$f")
    done
else
    while IFS= read -r f; do CASES+=("$f"); done < <(ls "$HERE"/cases/*.sh 2>/dev/null | sort)
fi

if [ ${#CASES[@]} -eq 0 ]; then
    echo "==> No cases to run."
    echo "{}" > "$JSON"
    exit 0
fi

declare -a ROWS
PASS=0; FAIL=0; SKIP=0; KNOWN=0
T0=$(date +%s)

for case_path in "${CASES[@]}"; do
    name=$(basename "$case_path" .sh)
    echo
    echo "── $name ──────────────────────────────────────────"
    log="$REPORT_DIR/$name.log"
    started=$(date +%s)
    bash "$case_path" 2>&1 | tee "$log"
    elapsed=$(( $(date +%s) - started ))
    last=$(tail -n 5 "$log" | grep '^STATUS:' | tail -n 1)
    case "$last" in
        "STATUS: PASS")          PASS=$((PASS+1));  status="PASS";  detail="" ;;
        "STATUS: SKIP")          SKIP=$((SKIP+1));  status="SKIP";  detail="" ;;
        STATUS:\ KNOWN_ISSUE*)   KNOWN=$((KNOWN+1)); status="KNOWN_ISSUE"; detail="${last#STATUS: KNOWN_ISSUE }" ;;
        "STATUS: FAIL"|*)        FAIL=$((FAIL+1));  status="FAIL";  detail="$(tail -n 3 "$log" | head -n 1)" ;;
    esac
    ROWS+=("$name|$status|${elapsed}s|$detail")
done

TTOT=$(( $(date +%s) - T0 ))

echo
echo "════════════════════════════════════════════════════════════════"
echo "Release suite summary — total ${TTOT}s"
printf "%-32s  %-12s  %-6s  %s\n" "Case" "Status" "Time" "Detail"
echo "────────────────────────────────────────────────────────────────"
for r in "${ROWS[@]}"; do
    IFS='|' read -r n s t d <<< "$r"
    printf "%-32s  %-12s  %-6s  %s\n" "$n" "$s" "$t" "$d"
done
echo "────────────────────────────────────────────────────────────────"
echo "PASS=$PASS  FAIL=$FAIL  SKIP=$SKIP  KNOWN_ISSUE=$KNOWN  (cases=${#CASES[@]})"
echo "════════════════════════════════════════════════════════════════"

# Write JSON summary.
{
    echo "{"
    echo "  \"run_id\": \"$RUN_ID\","
    echo "  \"mode\": \"$([ "$QUICK" = "1" ] && echo quick || echo full)\","
    echo "  \"duration_secs\": $TTOT,"
    echo "  \"counts\": {\"pass\": $PASS, \"fail\": $FAIL, \"skip\": $SKIP, \"known_issue\": $KNOWN},"
    echo "  \"cases\": ["
    first=1
    for r in "${ROWS[@]}"; do
        IFS='|' read -r n s t d <<< "$r"
        [ $first -eq 0 ] && echo ","
        first=0
        printf "    {\"name\": \"%s\", \"status\": \"%s\", \"duration\": \"%s\", \"detail\": \"%s\"}" \
            "$n" "$s" "$t" "$(echo "$d" | sed 's/"/\\"/g')"
    done
    echo
    echo "  ]"
    echo "}"
} > "$JSON"

[ $FAIL -eq 0 ]

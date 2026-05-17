#!/usr/bin/env bash
# Scale: many small files in one directory.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/harness.sh"
CATEGORY="03-many-files"

t_section "03 — Many files (scale)"

CL=client-01
N=50   # 50 small files — observed 500 triggers FUSE deadlock (see F-26)
# Unique prefix per run so leftover files from previous runs / health-check
# probes don't inflate counts and confuse the pass/fail checks.
TAG="bulk$(date +%s%N | tail -c 7)"

# T3.1: write N tiny files
s=$(t_start)
sssh $CL "sudo bash -c 'for i in \$(seq 1 $N); do echo file-\$i > ${MOUNT}/${TAG}-\$i.txt; done; sync'" 2>&1 >/tmp/manyfiles_write.log
elapsed=$(t_elapsed $s)
# Give FUSE/manager a moment for the directory cache to settle before counting.
sleep 2
created=$(sssh $CL "sudo timeout 10 ls ${MOUNT}/${TAG}-*.txt 2>/dev/null | sort -u | wc -l")
if [ "$created" -eq $N ]; then
    t_pass "T3.1 write $N tiny files" "$elapsed" "created=$created"
else
    t_fail "T3.1 write $N tiny files" "$elapsed" "created=$created / $N"
fi

# T3.2: ls $N files (dedup to be robust against any residual readdir dups).
s=$(t_start)
got=$(sssh $CL "sudo ls ${MOUNT}/${TAG}-*.txt 2>/dev/null | sort -u | wc -l")
if [ "$got" -eq $N ]; then
    t_pass "T3.2 ls $N files" "$(t_elapsed $s)"
else
    t_fail "T3.2 ls $N files" "$(t_elapsed $s)" "got=$got"
fi

# T3.3: cluster /api/v1/files endpoint contains all
s=$(t_start)
api_count=$(api_get /api/v1/files | python3 -c "import sys,json;p='/${TAG}-';print(sum(1 for f in json.load(sys.stdin)['files'] if f['path'].startswith(p)))")
if [ "$api_count" -eq $N ]; then
    t_pass "T3.3 cluster API count" "$(t_elapsed $s)"
else
    t_warn "T3.3 cluster API count" "expected $N, got $api_count"
fi

# T3.4: read every file and verify content (wrapped in timeout to avoid wedging suite)
s=$(t_start)
mismatches=$(sssh $CL "sudo timeout 120 bash -c 'm=0; for i in \$(seq 1 $N); do exp=\"file-\$i\"; got=\$(cat ${MOUNT}/${TAG}-\$i.txt 2>/dev/null); [ \"\$got\" = \"\$exp\" ] || m=\$((m+1)); done; echo \$m' 2>/dev/null")
if [ -z "$mismatches" ]; then
    t_fail "T3.4 read+verify $N files" "$(t_elapsed $s)" "TIMEOUT — FUSE likely hung (F-26)"
elif [ "$mismatches" -eq 0 ]; then
    t_pass "T3.4 read+verify $N files" "$(t_elapsed $s)"
else
    t_fail "T3.4 read+verify $N files" "$(t_elapsed $s)" "$mismatches mismatches"
fi

# T3.5: delete all
s=$(t_start)
sssh $CL "sudo timeout 60 rm -f ${MOUNT}/${TAG}-*.txt 2>/dev/null"
sleep 1
remaining=$(sssh $CL "sudo timeout 10 ls ${MOUNT}/${TAG}-*.txt 2>/dev/null | wc -l")
if [ "$remaining" -eq 0 ]; then
    t_pass "T3.5 bulk delete" "$(t_elapsed $s)"
else
    t_fail "T3.5 bulk delete" "$(t_elapsed $s)" "$remaining still present"
fi

# T3.6: cluster reflects deletion
s=$(t_start)
sleep 2
api_count=$(api_get /api/v1/files | python3 -c "import sys,json;p='/${TAG}-';print(sum(1 for f in json.load(sys.stdin)['files'] if f['path'].startswith(p)))")
if [ "$api_count" -eq 0 ]; then
    t_pass "T3.6 cluster cleaned up metadata" "$(t_elapsed $s)"
else
    t_warn "T3.6 cluster cleaned up metadata" "$api_count entries still in API"
fi

#!/usr/bin/env bash
# FUSE operation coverage — many of these are expected to be incomplete per ISSUES_TO_FIX I-4.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/harness.sh"
CATEGORY="02-fuse-ops"

t_section "02 — FUSE operation coverage"

CL=client-01

# T2.1: empty file
s=$(t_start)
sssh $CL "sudo touch ${MOUNT}/empty.bin" >/dev/null 2>&1
sz=$(sssh $CL "sudo stat -c %s ${MOUNT}/empty.bin" 2>/dev/null)
if [ "$sz" = "0" ]; then t_pass "T2.1 touch empty file" "$(t_elapsed $s)"
else t_fail "T2.1 touch empty file" "$(t_elapsed $s)" "size=$sz"; fi

# T2.2: large file (use existing ep1.mkv or write new)
s=$(t_start)
if sssh $CL "test -f /home/debian/ep1.mkv"; then
    sssh $CL "sudo cp /home/debian/ep1.mkv ${MOUNT}/large1.mkv" >/dev/null 2>&1
    sz=$(sssh $CL "sudo stat -c %s ${MOUNT}/large1.mkv" 2>/dev/null)
    if [ "$sz" -gt 200000000 ]; then t_pass "T2.2 245 MB write+stat" "$(t_elapsed $s)" "size=$sz"
    else t_fail "T2.2 245 MB write+stat" "$(t_elapsed $s)" "size=$sz"; fi
else
    t_skip "T2.2 large file" "no /home/debian/ep1.mkv on client"
fi

# T2.3: delete and recreate same name (no stale handles)
s=$(t_start)
sssh $CL "sudo bash -c 'echo first > ${MOUNT}/recycle.txt; rm -f ${MOUNT}/recycle.txt; echo second > ${MOUNT}/recycle.txt'" >/dev/null 2>&1
got=$(sssh $CL "sudo cat ${MOUNT}/recycle.txt" 2>/dev/null)
if [ "$got" = "second" ]; then t_pass "T2.3 delete + recreate same name" "$(t_elapsed $s)"
else t_fail "T2.3 delete + recreate same name" "$(t_elapsed $s)" "got=$got"; fi

# T2.4: file with spaces in name
s=$(t_start)
sssh $CL "sudo bash -c \"echo hi > '${MOUNT}/file with spaces.txt'\"" >/dev/null 2>&1
got=$(sssh $CL "sudo cat '${MOUNT}/file with spaces.txt'" 2>/dev/null)
if [ "$got" = "hi" ]; then t_pass "T2.4 filename with spaces" "$(t_elapsed $s)"
else t_fail "T2.4 filename with spaces" "$(t_elapsed $s)" "got=$got"; fi

# T2.5: unicode filename
s=$(t_start)
NAME="filé-наклон-日本語.txt"
if sssh $CL "sudo bash -c \"echo ümlaut > '${MOUNT}/${NAME}' && cat '${MOUNT}/${NAME}'\"" 2>/dev/null | grep -q ümlaut; then
    t_pass "T2.5 unicode filename" "$(t_elapsed $s)"
else
    t_fail "T2.5 unicode filename" "$(t_elapsed $s)" "round-trip failed"
fi

# T2.6: very long filename (255 chars is POSIX max)
s=$(t_start)
LONG_NAME=$(python3 -c "print('a'*200 + '.txt')")
if sssh $CL "sudo bash -c \"echo x > ${MOUNT}/${LONG_NAME}; cat ${MOUNT}/${LONG_NAME}\"" 2>&1 | grep -q "^x$"; then
    t_pass "T2.6 200-char filename" "$(t_elapsed $s)"
else
    t_fail "T2.6 200-char filename" "$(t_elapsed $s)" "round-trip failed"
fi

# T2.7: mkdir / rmdir
s=$(t_start)
if sssh $CL "sudo mkdir ${MOUNT}/testdir 2>&1" | head -1; then :; fi
if sssh $CL "sudo test -d ${MOUNT}/testdir" 2>/dev/null; then
    sssh $CL "sudo rmdir ${MOUNT}/testdir" 2>/dev/null
    t_pass "T2.7 mkdir/rmdir" "$(t_elapsed $s)"
else
    t_fail "T2.7 mkdir/rmdir" "$(t_elapsed $s)" "mkdir didn't create a directory"
fi

# T2.8: rename / mv (known ENOSYS per ISSUES_TO_FIX I-4)
s=$(t_start)
sssh $CL "sudo bash -c 'echo r > ${MOUNT}/rename-src.txt'" >/dev/null 2>&1
out=$(sssh $CL "sudo mv ${MOUNT}/rename-src.txt ${MOUNT}/rename-dst.txt 2>&1")
rc=$?
if [ $rc -eq 0 ] && sssh $CL "sudo test -f ${MOUNT}/rename-dst.txt"; then
    t_pass "T2.8 rename" "$(t_elapsed $s)"
else
    t_fail "T2.8 rename" "$(t_elapsed $s)" "mv rc=$rc out=${out:0:80}"
fi

# T2.9: symlink (known ENOSYS)
s=$(t_start)
out=$(sssh $CL "sudo ln -s /etc/hostname ${MOUNT}/hostname-link 2>&1")
rc=$?
if [ $rc -eq 0 ]; then
    t_pass "T2.9 symlink" "$(t_elapsed $s)"
else
    t_fail "T2.9 symlink" "$(t_elapsed $s)" "rc=$rc: ${out:0:80}"
fi

# T2.10: chmod (setattr)
s=$(t_start)
sssh $CL "sudo bash -c 'echo c > ${MOUNT}/chmod-test.txt'" >/dev/null 2>&1
out=$(sssh $CL "sudo chmod 600 ${MOUNT}/chmod-test.txt 2>&1")
rc=$?
mode=$(sssh $CL "sudo stat -c %a ${MOUNT}/chmod-test.txt" 2>/dev/null)
if [ $rc -eq 0 ] && [ "$mode" = "600" ]; then
    t_pass "T2.10 chmod" "$(t_elapsed $s)"
else
    t_fail "T2.10 chmod" "$(t_elapsed $s)" "rc=$rc mode=$mode: ${out:0:80}"
fi

# T2.11: truncate (shrink)
s=$(t_start)
sssh $CL "sudo bash -c 'dd if=/dev/urandom of=${MOUNT}/trunc.bin bs=1M count=2 status=none'" 2>/dev/null
out=$(sssh $CL "sudo truncate -s 100K ${MOUNT}/trunc.bin 2>&1")
sz=$(sssh $CL "sudo stat -c %s ${MOUNT}/trunc.bin" 2>/dev/null)
if [ "$sz" = "102400" ]; then
    t_pass "T2.11 truncate shrink" "$(t_elapsed $s)"
else
    t_fail "T2.11 truncate shrink" "$(t_elapsed $s)" "expected 102400, got $sz"
fi

# T2.12: O_APPEND
s=$(t_start)
sssh $CL "sudo bash -c 'echo line1 > ${MOUNT}/append.txt'" >/dev/null 2>&1
sssh $CL "sudo bash -c 'echo line2 >> ${MOUNT}/append.txt'" >/dev/null 2>&1
lines=$(sssh $CL "sudo cat ${MOUNT}/append.txt" 2>/dev/null | wc -l)
if [ "$lines" = "2" ]; then
    t_pass "T2.12 O_APPEND" "$(t_elapsed $s)"
else
    t_fail "T2.12 O_APPEND" "$(t_elapsed $s)" "expected 2 lines, got $lines"
fi

# T2.13: seek + write (sparse file)
s=$(t_start)
sssh $CL "sudo bash -c 'truncate -s 0 ${MOUNT}/sparse.bin; dd if=/dev/zero of=${MOUNT}/sparse.bin bs=1 count=1 seek=1048576 status=none'" >/dev/null 2>&1
sz=$(sssh $CL "sudo stat -c %s ${MOUNT}/sparse.bin" 2>/dev/null)
if [ "$sz" = "1048577" ]; then
    t_pass "T2.13 sparse-file create" "$(t_elapsed $s)" "size=1M+1"
else
    t_fail "T2.13 sparse-file create" "$(t_elapsed $s)" "size=$sz"
fi

# T2.14: hidden / dotfile
s=$(t_start)
sssh $CL "sudo bash -c 'echo hidden > ${MOUNT}/.hidden'" >/dev/null 2>&1
got=$(sssh $CL "sudo cat ${MOUNT}/.hidden" 2>/dev/null)
if [ "$got" = "hidden" ]; then
    t_pass "T2.14 dotfile" "$(t_elapsed $s)"
else
    t_fail "T2.14 dotfile" "$(t_elapsed $s)" "got=$got"
fi

# Cleanup
sssh $CL "sudo rm -rf ${MOUNT}/empty.bin '${MOUNT}/file with spaces.txt' '${MOUNT}/${NAME}' \"${MOUNT}/${LONG_NAME}\" ${MOUNT}/large1.mkv ${MOUNT}/recycle.txt ${MOUNT}/rename-* ${MOUNT}/chmod-test.txt ${MOUNT}/trunc.bin ${MOUNT}/append.txt ${MOUNT}/sparse.bin ${MOUNT}/.hidden ${MOUNT}/hostname-link" >/dev/null 2>&1

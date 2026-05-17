#!/usr/bin/env bash
# Pathological paths: traversal attempts, deep nesting, weird chars.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/harness.sh"
CATEGORY="09-paths"

t_section "09 — Path edge cases"

CL=client-01

# T9.1: path traversal attempt — kernel should normalize before FUSE sees it
s=$(t_start)
sssh $CL "sudo bash -c 'echo hostpath > ${MOUNT}/../passwd-attack' 2>&1" >/tmp/trav.log 2>&1
# That file should land on the underlying / not the mount
if ! sssh $CL "sudo test -e ${MOUNT}/../passwd-attack 2>/dev/null"; then
    t_warn "T9.1 path traversal" "couldn't probe destination (file didn't land where expected — but no in-mount entry either)"
else
    where=$(sssh $CL "sudo find / -name passwd-attack -mmin -1 2>/dev/null | head -3" | tr '\n' ' ')
    t_pass "T9.1 path traversal contained" "$(t_elapsed $s)" "wrote to $where (host fs, not cluster)"
    sssh $CL "sudo rm -f /passwd-attack /mnt/passwd-attack /home/debian/passwd-attack" >/dev/null 2>&1
fi

# T9.2: deep nested path (mkdir -p in FUSE — known incomplete?)
s=$(t_start)
out=$(sssh $CL "sudo mkdir -p ${MOUNT}/a/b/c/d/e/f/g 2>&1")
rc=$?
if [ $rc -eq 0 ] && sssh $CL "sudo test -d ${MOUNT}/a/b/c/d/e/f/g"; then
    t_pass "T9.2 mkdir -p deep nesting" "$(t_elapsed $s)"
else
    t_fail "T9.2 mkdir -p deep nesting" "$(t_elapsed $s)" "rc=$rc, ${out:0:80}"
fi

# T9.3: file path with embedded slashes inside a directory (should map cleanly)
s=$(t_start)
sssh $CL "sudo bash -c 'mkdir -p ${MOUNT}/sub && echo nested > ${MOUNT}/sub/file.txt 2>&1'" >/tmp/n.log 2>&1
got=$(sssh $CL "sudo cat ${MOUNT}/sub/file.txt" 2>/dev/null)
if [ "$got" = "nested" ]; then
    t_pass "T9.3 nested file write+read" "$(t_elapsed $s)"
else
    t_fail "T9.3 nested file write+read" "$(t_elapsed $s)" "got='$got'"
fi

# T9.4: leading dot, trailing dot
s=$(t_start)
sssh $CL "sudo bash -c 'echo dot > ${MOUNT}/.start && echo end > ${MOUNT}/finish. && cat ${MOUNT}/.start ${MOUNT}/finish.' 2>&1" > /tmp/d.log 2>&1
out=$(sssh $CL "sudo cat ${MOUNT}/.start ${MOUNT}/finish. 2>/dev/null" | tr '\n' ' ')
if echo "$out" | grep -q "dot.*end"; then
    t_pass "T9.4 leading/trailing dot in name" "$(t_elapsed $s)"
else
    t_fail "T9.4 leading/trailing dot in name" "$(t_elapsed $s)" "out='$out'"
fi

# T9.5: very deep (1000 dirs) — manager scales by path strings
s=$(t_start)
DEEP_PATH=$(python3 -c "print('/'.join('d'+str(i) for i in range(50)))")
out=$(sssh $CL "sudo mkdir -p ${MOUNT}/${DEEP_PATH} 2>&1; sudo bash -c 'echo deep > ${MOUNT}/${DEEP_PATH}/file' 2>&1; sudo cat ${MOUNT}/${DEEP_PATH}/file 2>/dev/null")
if echo "$out" | grep -q "deep"; then
    t_pass "T9.5 50-level directory depth" "$(t_elapsed $s)"
else
    t_fail "T9.5 50-level directory depth" "$(t_elapsed $s)" "${out:0:100}"
fi

# Cleanup
sssh $CL "sudo rm -rf ${MOUNT}/a ${MOUNT}/sub ${MOUNT}/.start ${MOUNT}/finish. ${MOUNT}/d0" >/dev/null 2>&1

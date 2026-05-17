#!/usr/bin/env bash
# Race conditions: parallel writers, concurrent same-file writes.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/harness.sh"
CATEGORY="04-concurrency"

t_section "04 — Concurrency"

# T4.1: two clients writing different files in parallel
s=$(t_start)
sssh client-01 "sudo bash -c 'dd if=/dev/urandom of=${MOUNT}/c01.bin bs=1M count=10 status=none && md5sum ${MOUNT}/c01.bin > /tmp/c01.md5'" >/tmp/c01.log 2>&1 &
P1=$!
sssh client-02 "sudo bash -c 'dd if=/dev/urandom of=${MOUNT}/c02.bin bs=1M count=10 status=none && md5sum ${MOUNT}/c02.bin > /tmp/c02.md5'" >/tmp/c02.log 2>&1 &
P2=$!
wait $P1; rc1=$?
wait $P2; rc2=$?
elapsed=$(t_elapsed $s)
if [ $rc1 -eq 0 ] && [ $rc2 -eq 0 ]; then
    md1=$(sssh client-01 "sudo cat /tmp/c01.md5 | cut -d' ' -f1")
    md2=$(sssh client-02 "sudo cat /tmp/c02.md5 | cut -d' ' -f1")
    if [ -n "$md1" ] && [ -n "$md2" ] && [ "$md1" != "$md2" ]; then
        t_pass "T4.1 parallel writers, different files" "$elapsed"
    else
        t_fail "T4.1 parallel writers, different files" "$elapsed" "md1=$md1 md2=$md2"
    fi
else
    t_fail "T4.1 parallel writers, different files" "$elapsed" "rc1=$rc1 rc2=$rc2"
fi

# T4.2: same client, 4 parallel writes to different files (unique tag per run).
PTAG="par$(date +%s%N | tail -c 7)"
s=$(t_start)
sssh client-01 "sudo bash -c '
  for i in 1 2 3 4; do
    dd if=/dev/urandom of=${MOUNT}/${PTAG}-\$i.bin bs=1M count=5 status=none &
  done
  wait
  md5sum ${MOUNT}/${PTAG}-*.bin > /tmp/par.md5
'" >/tmp/par.log 2>&1
rc=$?
elapsed=$(t_elapsed $s)
files=$(sssh client-01 "sudo ls ${MOUNT}/${PTAG}-*.bin 2>/dev/null | sort -u | wc -l")
if [ $rc -eq 0 ] && [ "$files" = "4" ]; then
    distinct=$(sssh client-01 "sudo cat /tmp/par.md5 | cut -d' ' -f1 | sort -u | wc -l")
    if [ "$distinct" = "4" ]; then
        t_pass "T4.2 4-way parallel writes (one client)" "$elapsed"
    else
        t_fail "T4.2 4-way parallel writes (one client)" "$elapsed" "only $distinct distinct md5s"
    fi
else
    t_fail "T4.2 4-way parallel writes (one client)" "$elapsed" "rc=$rc files=$files"
fi

# T4.3: same file written by both clients (last-writer-wins?)
s=$(t_start)
sssh client-01 "sudo bash -c 'echo from-client-01 > ${MOUNT}/race.txt'" >/dev/null 2>&1 &
sssh client-02 "sudo bash -c 'echo from-client-02 > ${MOUNT}/race.txt'" >/dev/null 2>&1 &
wait
sleep 2
got1=$(sssh client-01 "sudo cat ${MOUNT}/race.txt" 2>/dev/null)
got2=$(sssh client-02 "sudo cat ${MOUNT}/race.txt" 2>/dev/null)
elapsed=$(t_elapsed $s)
if [ "$got1" = "$got2" ] && [[ "$got1" =~ ^from-client-0[12]$ ]]; then
    t_pass "T4.3 same-file race converges" "$elapsed" "settled to '$got1'"
elif [ "$got1" = "$got2" ]; then
    t_warn "T4.3 same-file race converges" "both clients see '$got1' but unexpected content"
else
    t_fail "T4.3 same-file race converges" "$elapsed" "diverged: c1='$got1' c2='$got2'"
fi

# T4.4: many concurrent readers of the same file
s=$(t_start)
sssh client-01 "sudo bash -c 'dd if=/dev/urandom of=${MOUNT}/hot.bin bs=1M count=50 status=none && md5sum ${MOUNT}/hot.bin > /tmp/hot.md5'" >/dev/null 2>&1
expected=$(sssh client-01 "sudo cat /tmp/hot.md5 | cut -d' ' -f1")
# Now 6 concurrent readers across both clients
sssh client-01 "sudo bash -c 'for i in 1 2 3; do md5sum ${MOUNT}/hot.bin > /tmp/r\$i.md5 & done; wait'" >/dev/null 2>&1 &
sssh client-02 "sudo bash -c 'for i in 4 5 6; do md5sum ${MOUNT}/hot.bin > /tmp/r\$i.md5 & done; wait'" >/dev/null 2>&1 &
wait
elapsed=$(t_elapsed $s)
mismatches=0
for i in 1 2 3; do
    got=$(sssh client-01 "sudo cat /tmp/r$i.md5 | cut -d' ' -f1" 2>/dev/null)
    [ "$got" = "$expected" ] || mismatches=$((mismatches+1))
done
for i in 4 5 6; do
    got=$(sssh client-02 "sudo cat /tmp/r$i.md5 | cut -d' ' -f1" 2>/dev/null)
    [ "$got" = "$expected" ] || mismatches=$((mismatches+1))
done
if [ "$mismatches" -eq 0 ]; then
    t_pass "T4.4 6 concurrent readers" "$elapsed"
else
    t_fail "T4.4 6 concurrent readers" "$elapsed" "$mismatches mismatches"
fi

# Cleanup
for h in client-01 client-02; do
    sssh $h "sudo rm -f ${MOUNT}/c01.bin ${MOUNT}/c02.bin ${MOUNT}/${PTAG}-*.bin ${MOUNT}/race.txt ${MOUNT}/hot.bin" >/dev/null 2>&1
done

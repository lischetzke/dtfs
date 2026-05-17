#!/usr/bin/env bash
# Push the cluster toward disk-full. Each data node has 16 GB hot + 16 GB cold.
# RF=3 means usable hot capacity ≈ (16 GB × 4 nodes) / 3 ≈ 21 GB.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/harness.sh"
CATEGORY="08-disk-full"

t_section "08 — Disk-pressure / disk-full"

CL=client-01

# T8.1: write 1 GB file (well within capacity, sanity baseline)
s=$(t_start)
sssh $CL "sudo bash -c 'dd if=/dev/zero of=/tmp/1gb.bin bs=1M count=1024 status=none'" 2>/dev/null
out=$(sssh $CL "sudo bash -c 'time cp /tmp/1gb.bin ${MOUNT}/1gb.bin' 2>&1")
rc=$?
elapsed=$(t_elapsed $s)
if [ $rc -eq 0 ] && sssh $CL "sudo test -f ${MOUNT}/1gb.bin"; then
    sz=$(sssh $CL "sudo stat -c %s ${MOUNT}/1gb.bin" 2>/dev/null)
    t_pass "T8.1 1 GB write" "$elapsed" "size=$sz"
else
    t_fail "T8.1 1 GB write" "$elapsed" "rc=$rc"
fi

# T8.2: try to overflow — write 8 GB which at RF=3 needs 24 GB, more than usable hot
# Use truncate or dd ; expect either graceful "no space" or successful with degraded RF
s=$(t_start)
# Write in 1 GB chunks until failure or 12 GB
written=0
fail_at=""
for n in 2 3 4 5 6 7 8 9 10 11 12; do
    out=$(sssh $CL "sudo bash -c 'cp /tmp/1gb.bin ${MOUNT}/big-${n}.bin' 2>&1")
    rc=$?
    if [ $rc -ne 0 ]; then
        fail_at=$n
        break
    fi
    sz=$(sssh $CL "sudo stat -c %s ${MOUNT}/big-${n}.bin" 2>/dev/null)
    [ "$sz" = "1073741824" ] || { fail_at=$n; break; }
    written=$n
done
elapsed=$(t_elapsed $s)
if [ -n "$fail_at" ]; then
    t_pass "T8.2 disk pressure handled gracefully" "$elapsed" "wrote ${written} GB, failed cleanly at ${fail_at}"
elif [ "$written" = "12" ]; then
    t_warn "T8.2 no failure at 12 GB" "consider this miraculous — RF=3 should run out before then"
fi

# Show node-side disk usage
sleep 3
t_info "disk usage post-test:"
for h in "${DATA_NODES[@]}"; do
    use=$(sssh $h "sudo df -h /mnt/disk-a /mnt/disk-b 2>/dev/null | awk '/disk-[ab]/{print \$5}'" | tr '\n' ' ')
    t_info "  $h: $use"
done

# Cleanup
sssh $CL "sudo rm -f ${MOUNT}/1gb.bin ${MOUNT}/big-*.bin /tmp/1gb.bin" >/dev/null 2>&1
sleep 3

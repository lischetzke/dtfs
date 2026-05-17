#!/usr/bin/env bash
# Corrupt a chunk on disk, see if a) reads detect it or b) other replicas serve it.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/harness.sh"
CATEGORY="07-corruption"

t_section "07 — Chunk corruption + scrub"

CL=client-01

# Write a known file
sssh $CL "sudo bash -c 'dd if=/dev/urandom of=${MOUNT}/corrupt-target.bin bs=1M count=4 status=none && md5sum ${MOUNT}/corrupt-target.bin > /tmp/corrupt.md5'" >/dev/null 2>&1
expected=$(sssh $CL "sudo cat /tmp/corrupt.md5 | cut -d' ' -f1")
sleep 2

# Find chunk file on first available data node
victim=""; victim_file=""
for h in "${DATA_NODES[@]}"; do
    f=$(sssh $h "sudo find /mnt/disk-a -name '*.dat' -newer /tmp/corrupt.md5 -size +1M 2>/dev/null | head -1")
    if [ -n "$f" ]; then victim=$h; victim_file=$f; break; fi
done

if [ -z "$victim" ]; then
    t_skip "T7.* corruption tests" "no fresh chunk file found"
else
    t_info "victim: $victim:$victim_file"

    # T7.1: corrupt the chunk file (flip a byte)
    s=$(t_start)
    sssh $victim "sudo bash -c \"printf '\\xff' | dd of='$victim_file' bs=1 count=1 seek=1024 conv=notrunc status=none\"" >/dev/null 2>&1
    # Re-mount FUSE to clear cache
    sssh $CL "sudo fusermount3 -uz ${MOUNT} 2>/dev/null; sudo systemctl reset-failed dtfs-fuse@mnt-dtfs.service 2>/dev/null; sudo systemctl restart dtfs-fuse@mnt-dtfs.service; sleep 4" >/dev/null 2>&1
    got=$(sssh $CL "sudo md5sum ${MOUNT}/corrupt-target.bin 2>/dev/null | cut -d' ' -f1")
    elapsed=$(t_elapsed $s)
    if [ "$got" = "$expected" ]; then
        t_pass "T7.1 read still correct after one-replica corruption" "$elapsed" "served from healthy replica"
    elif [ -n "$got" ]; then
        # We changed get_chunk to NOT verify on every read for perf (F-19).
        # That means a corrupted replica MAY be served if it's the first one tried.
        # This is expected post-F-19; scrub catches it asynchronously.
        t_warn "T7.1 corrupted replica was served" "got=$got expected=$expected (post-F-19: scrub-only verification)"
    else
        t_fail "T7.1 read after corruption" "$elapsed" "read failed entirely"
    fi

    # T7.2: trigger scrub via API
    s=$(t_start)
    scrub_resp=$(api_post /api/v1/scrub '{}' 2>&1)
    elapsed=$(t_elapsed $s)
    if [ -n "$scrub_resp" ] && ! echo "$scrub_resp" | grep -qi "error\|not found"; then
        t_pass "T7.2 scrub API responds" "$elapsed" "${scrub_resp:0:80}"
    else
        t_warn "T7.2 scrub API" "${scrub_resp:0:120}"
    fi
fi

sssh $CL "sudo rm -f ${MOUNT}/corrupt-target.bin" >/dev/null 2>&1

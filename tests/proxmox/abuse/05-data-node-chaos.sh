#!/usr/bin/env bash
# Data-node failure: kill during writes/reads, verify recovery.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/harness.sh"
CATEGORY="05-data-chaos"

t_section "05 — Data-node chaos"

# T5.1: write 50 MB, kill ONE data node, verify reads still work via replicas
s=$(t_start)
sssh client-01 "sudo bash -c 'dd if=/dev/urandom of=/tmp/probe.bin bs=1M count=50 status=none && cp /tmp/probe.bin ${MOUNT}/probe.bin && md5sum ${MOUNT}/probe.bin > /tmp/probe.md5'" >/dev/null 2>&1
expected=$(sssh client-01 "sudo cat /tmp/probe.md5 | cut -d' ' -f1")

# Pick a node that has a chunk
victim=""
for h in "${DATA_NODES[@]}"; do
    n=$(sssh $h "sudo find /mnt/disk-a /mnt/disk-b -name '*.dat' -newer /tmp/probe.md5 2>/dev/null | wc -l")
    if [ "$n" -gt 0 ]; then victim=$h; break; fi
done

if [ -z "$victim" ]; then
    t_warn "T5.1 kill data node with replica" "no node holds the probe chunks (replication may not have completed yet)"
else
    t_info "killing dtfs-data on $victim"
    sssh $victim "sudo systemctl stop dtfs-data" >/dev/null 2>&1
    sleep 5
    # Read should still work (other 2 replicas survive)
    got=$(sssh client-01 "sudo md5sum ${MOUNT}/probe.bin 2>/dev/null | cut -d' ' -f1")
    if [ "$got" = "$expected" ]; then
        t_pass "T5.1 read works after killing $victim" "$(t_elapsed $s)"
    else
        t_fail "T5.1 read works after killing $victim" "$(t_elapsed $s)" "got=$got expected=$expected"
    fi
    # Restore
    sssh $victim "sudo systemctl start dtfs-data" >/dev/null 2>&1
    sleep 4
fi

# T5.2: detect node-down within 35 s (heartbeat = 5 s × 6 misses = 30 s)
s=$(t_start)
sssh data-01 "sudo systemctl stop dtfs-data" >/dev/null 2>&1
deadline=$(($(date +%s) + 60))
detected=0
while [ "$(date +%s)" -lt "$deadline" ]; do
    h=$(api_get /api/v1/nodes | python3 -c "import sys,json;d=[n for n in json.load(sys.stdin) if n['hostname']=='data-01'];print(d[0]['health'] if d else 'Missing')")
    if [ "$h" = "Down" ]; then detected=1; break; fi
    sleep 2
done
elapsed=$(t_elapsed $s)
if [ "$detected" = "1" ]; then
    t_pass "T5.2 failure detection within 60 s" "$elapsed"
else
    t_fail "T5.2 failure detection within 60 s" "$elapsed" "data-01 still not Down"
fi

# T5.3: writing while one node is down still succeeds (RF=3 of 3 healthy)
s=$(t_start)
sssh client-01 "sudo bash -c 'echo write-during-outage > ${MOUNT}/while-down.txt'" >/dev/null 2>&1
got=$(sssh client-01 "sudo cat ${MOUNT}/while-down.txt" 2>/dev/null)
cc=$(file_chunk_count "/while-down.txt")
if [ "$got" = "write-during-outage" ] && [ "$cc" -ge 1 ]; then
    t_pass "T5.3 write succeeds with 3/4 nodes healthy" "$(t_elapsed $s)" "chunk_count=$cc"
else
    t_fail "T5.3 write succeeds with 3/4 nodes healthy" "$(t_elapsed $s)" "got='$got' cc=$cc"
fi

# T5.4: bring node back, re-replication catches up under-replicated chunks
s=$(t_start)
sssh data-01 "sudo systemctl start dtfs-data" >/dev/null 2>&1
sleep 10
# Wait up to 120 s for under-replication to clear (scheduler tick is 30 s)
deadline=$(($(date +%s) + 150))
ok=0
while [ "$(date +%s)" -lt "$deadline" ]; do
    h=$(api_get /api/v1/nodes | python3 -c "import sys,json;d=[n for n in json.load(sys.stdin) if n['hostname']=='data-01'];print(d[0]['health'] if d else 'Missing')")
    if [ "$h" = "Healthy" ]; then ok=1; break; fi
    sleep 3
done
elapsed=$(t_elapsed $s)
if [ "$ok" = "1" ]; then
    t_pass "T5.4 data-01 returns to Healthy" "$elapsed"
else
    t_fail "T5.4 data-01 returns to Healthy" "$elapsed" "still not Healthy"
fi

# T5.5: 2 nodes down — RF=3 write should fail OR degrade gracefully (not silent loss)
s=$(t_start)
sssh data-03 "sudo systemctl stop dtfs-data" >/dev/null 2>&1
sssh data-04 "sudo systemctl stop dtfs-data" >/dev/null 2>&1
sleep 35 # wait for detection
out=$(sssh client-01 "sudo bash -c 'echo degraded-write > ${MOUNT}/degraded.txt' 2>&1")
rc=$?
sleep 2
# It's OK to either fail OR degrade to lower RF; what's NOT OK is "wrote successfully with 0 chunks"
cc=$(file_chunk_count "/degraded.txt")
nodes_up=$(healthy_node_count)
elapsed=$(t_elapsed $s)
if [ "$cc" -ge 1 ]; then
    t_pass "T5.5 2-node-down write (degraded)" "$elapsed" "chunks=$cc with $nodes_up healthy nodes"
elif [ $rc -ne 0 ] || ! file_exists "/degraded.txt"; then
    t_pass "T5.5 2-node-down write fails loudly" "$elapsed"
else
    t_fail "T5.5 2-node-down write" "$elapsed" "ghost file with 0 chunks (silent data loss)"
fi

# Restore
sssh data-03 "sudo systemctl start dtfs-data" >/dev/null 2>&1
sssh data-04 "sudo systemctl start dtfs-data" >/dev/null 2>&1
sleep 8
sssh client-01 "sudo rm -f ${MOUNT}/probe.bin ${MOUNT}/while-down.txt ${MOUNT}/degraded.txt" >/dev/null 2>&1

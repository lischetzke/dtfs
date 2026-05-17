#!/usr/bin/env bash
# Baseline sanity: cluster shape, basic R/W. Anything here failing is a regression.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/harness.sh"
CATEGORY="01-sanity"

t_section "01 — Baseline sanity"

# T1.1: All 3 managers return the same cluster status
s=$(t_start)
fail=
declare -A node_counts
for ip in 10.150.0.151 10.150.0.152 10.150.0.153; do
    n=$(curl -sS --max-time 5 "http://$ip:9402/api/v1/cluster/status" | python3 -c "import sys,json;print(json.load(sys.stdin)['data_nodes'])" 2>/dev/null || echo X)
    node_counts[$ip]=$n
done
if [ "${node_counts[10.150.0.151]}" = "${node_counts[10.150.0.152]}" ] && \
   [ "${node_counts[10.150.0.152]}" = "${node_counts[10.150.0.153]}" ] && \
   [ "${node_counts[10.150.0.151]}" -ge 4 ]; then
    t_pass "T1.1 multi-manager raft replication" "$(t_elapsed $s)" "mgrs all see ${node_counts[10.150.0.151]} data nodes"
else
    t_fail "T1.1 multi-manager raft replication" "$(t_elapsed $s)" "diverged: ${node_counts[*]}"
fi

# T1.2: 4 healthy data nodes
s=$(t_start)
n=$(healthy_node_count)
if [ "$n" -eq 4 ]; then
    t_pass "T1.2 four healthy data nodes" "$(t_elapsed $s)"
else
    t_fail "T1.2 four healthy data nodes" "$(t_elapsed $s)" "got $n / 4"
fi

# T1.3: FUSE mount on client-01
s=$(t_start)
if sssh client-01 "sudo mountpoint -q ${MOUNT}" 2>/dev/null; then
    t_pass "T1.3 FUSE mount visible" "$(t_elapsed $s)"
else
    t_fail "T1.3 FUSE mount visible" "$(t_elapsed $s)" "not a mountpoint"
fi

# T1.4: tiny write+read round-trip
s=$(t_start)
sssh client-01 "sudo bash -c 'echo sanity-$RANDOM > ${MOUNT}/sanity.txt'" >/dev/null 2>&1
got=$(sssh client-01 "sudo cat ${MOUNT}/sanity.txt" 2>/dev/null | head -c 64)
if echo "$got" | grep -q "^sanity-"; then
    t_pass "T1.4 small write+read round-trip" "$(t_elapsed $s)"
else
    t_fail "T1.4 small write+read round-trip" "$(t_elapsed $s)" "got=$got"
fi

# T1.5: file appears in cluster API with chunks
s=$(t_start)
sleep 1
cc=$(file_chunk_count "/sanity.txt")
if [ "$cc" -ge 1 ]; then
    t_pass "T1.5 cluster sees the file" "$(t_elapsed $s)" "chunk_count=$cc"
else
    t_fail "T1.5 cluster sees the file" "$(t_elapsed $s)" "chunk_count=$cc"
fi

# T1.6: ls returns expected entry
s=$(t_start)
if sssh client-01 "sudo ls ${MOUNT}/" 2>/dev/null | grep -q "^sanity.txt$"; then
    t_pass "T1.6 ls shows the file" "$(t_elapsed $s)"
else
    t_fail "T1.6 ls shows the file" "$(t_elapsed $s)" "not in ls"
fi

# Cleanup
sssh client-01 "sudo rm -f ${MOUNT}/sanity.txt" >/dev/null 2>&1

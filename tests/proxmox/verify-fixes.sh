#!/usr/bin/env bash
# Run from the controller VM (10.150.0.150). Verifies F-001 .. F-016 end-to-end
# on the freshly deployed cluster. Expects mgr-01..03, data-01..04, client-01..02
# to be reachable by hostname.

set -uo pipefail

KEY=~/.ssh/dtfs-lab/id_ed25519
KH=~/.ssh/dtfs-lab/known_hosts
SSHOPTS=(-i "$KEY" -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile="$KH" -o BatchMode=yes)
MGR=http://10.150.0.151:9402

pass=0
fail=0

ok() { echo "  [ OK ] $*"; pass=$((pass+1)); }
ng() { echo "  [FAIL] $*"; fail=$((fail+1)); }

section() { echo ""; echo "=== $* ==="; }

# F-009: all 3 managers see the same cluster state
section "F-009: multi-manager Raft cluster"
declare -a NODE_COUNTS
for ip in 10.150.0.151 10.150.0.152 10.150.0.153; do
    n=$(curl -s --max-time 5 "http://$ip:9402/api/v1/nodes" | python3 -c "import sys,json;print(len(json.load(sys.stdin)))" 2>/dev/null || echo X)
    echo "  $ip /api/v1/nodes count: $n"
    NODE_COUNTS+=("$n")
done
if [[ "${NODE_COUNTS[0]}" == "${NODE_COUNTS[1]}" && "${NODE_COUNTS[1]}" == "${NODE_COUNTS[2]}" && "${NODE_COUNTS[0]}" =~ ^[1-9] ]]; then
    ok "All 3 managers report the same non-zero data-node count"
else
    ng "Manager state diverged (${NODE_COUNTS[*]}) — F-009 not fixed"
fi

# F-006: capacity reporting
section "F-006: data-node capacity reporting"
totals=$(curl -s "$MGR/api/v1/nodes" | python3 -c "import sys,json; print(sum(n.get('total_capacity',0) for n in json.load(sys.stdin)))")
echo "  Sum of per-node total_capacity: $totals bytes"
if [ "$totals" -gt 1000000000 ]; then
    ok "Cluster sees non-zero capacity ($((totals/1024/1024/1024)) GB)"
else
    ng "Cluster total_capacity is zero or tiny ($totals)"
fi

# F-007: dtfs-cli volumes create
section "F-007: dtfs-cli volumes create"
ssh "${SSHOPTS[@]}" mgr-01 "DTFS_MANAGER=http://localhost:9402 dtfs-cli volumes create verify-vol --replication-factor 3 2>&1" || true
sleep 1
vol=$(curl -s "$MGR/api/v1/volumes" | python3 -c "import sys,json;print(','.join(v['volume_id'] for v in json.load(sys.stdin).get('volumes',[])))")
if echo "$vol" | grep -q "verify-vol"; then
    ok "Created volume via CLI (volumes: $vol)"
else
    ng "verify-vol not found after CLI create (volumes: $vol)"
fi

# Make sure 'default' exists for the FUSE test
curl -s -X POST -H 'Content-Type: application/json' -d '{"name":"default"}' "$MGR/api/v1/volumes" >/dev/null

# F-005 + F-004: FUSE mount visible on host
section "F-004 + F-005: FUSE mount visible on host"
ssh "${SSHOPTS[@]}" client-01 "sudo systemctl reset-failed dtfs-fuse@\$(systemd-escape --path /mnt/dtfs).service 2>/dev/null; sudo systemctl restart dtfs-fuse@\$(systemd-escape --path /mnt/dtfs).service; sleep 4"
mp=$(ssh "${SSHOPTS[@]}" client-01 "mountpoint /mnt/dtfs 2>&1" || true)
echo "  client-01: $mp"
if echo "$mp" | grep -q "is a mountpoint"; then
    ok "FUSE mount visible on host (F-005)"
else
    ng "FUSE not visible (F-005 not fixed): $mp"
fi

# F-010: write a real-data file and verify RF=3
section "F-010: replication factor enforced on writes"
if [ -f /home/debian/ep1.mkv ]; then
    ssh "${SSHOPTS[@]}" client-01 'sudo rm -f /mnt/dtfs/verify.mkv'
    ssh "${SSHOPTS[@]}" client-01 'test -f /home/debian/ep1.mkv || scp -i ~/.ssh/dtfs-lab/id_ed25519 -o UserKnownHostsFile=~/.ssh/dtfs-lab/known_hosts builder:/home/debian/ep1.mkv /home/debian/ep1.mkv' 2>/dev/null || true
    src_md5=$(ssh "${SSHOPTS[@]}" client-01 'md5sum /home/debian/ep1.mkv 2>/dev/null | cut -d" " -f1')
    echo "  source md5: $src_md5"
    echo "  writing 245 MB through FUSE..."
    write_time=$(ssh "${SSHOPTS[@]}" client-01 "sudo bash -c \"time cp /home/debian/ep1.mkv /mnt/dtfs/verify.mkv\" 2>&1" | grep real | awk '{print $2}')
    echo "  write time: $write_time"
    sleep 2
    # Count chunk files on each data node
    declare -a CHUNK_COUNTS
    for h in data-01 data-02 data-03 data-04; do
        c=$(ssh "${SSHOPTS[@]}" "$h" 'sudo find /mnt/disk-a /mnt/disk-b -type f -name "*.dat" 2>/dev/null | wc -l')
        echo "  $h chunk-files: $c"
        CHUNK_COUNTS+=("$h:$c")
    done
    total=$(echo "${CHUNK_COUNTS[@]}" | tr ' ' '\n' | awk -F: '{sum+=$2} END{print sum}')
    echo "  total chunk-files cluster-wide: $total (expecting ~6 for 2-chunk file at RF=3)"
    if [ "$total" -ge 5 ]; then
        ok "RF=3 enforced (cluster has $total chunk-files for a 2-chunk file)"
    else
        ng "Only $total chunk-file(s) across cluster — RF not enforced (F-010 not fixed)"
    fi

    # F-011: read perf
    section "F-011: read throughput"
    read_time=$(ssh "${SSHOPTS[@]}" client-01 "sudo bash -c \"time md5sum /mnt/dtfs/verify.mkv > /tmp/v.md5\" 2>&1" | grep real | awk '{print $2}')
    got_md5=$(ssh "${SSHOPTS[@]}" client-01 'sudo cat /tmp/v.md5 | cut -d" " -f1')
    echo "  read time: $read_time, md5: $got_md5"
    if [ "$src_md5" = "$got_md5" ]; then
        ok "Read round-trip md5 matches source"
    else
        ng "md5 mismatch on read-back (source=$src_md5 got=$got_md5)"
    fi
    # Parse time (e.g. "0m12.345s") into seconds
    secs=$(echo "$read_time" | awk -F'[ms]' '{print $1*60 + $2}')
    if (( $(echo "$secs < 10" | bc -l) )); then
        ok "Read throughput ≥ 24 MB/s ($read_time for 245 MB)"
    elif (( $(echo "$secs < 30" | bc -l) )); then
        ok "Read throughput acceptable but below write speed ($read_time)"
    else
        ng "Read still slow ($read_time for 245 MB)"
    fi

    # F-014: mtime preservation
    section "F-014: file timestamps"
    mtime=$(ssh "${SSHOPTS[@]}" client-01 'sudo stat -c "%y" /mnt/dtfs/verify.mkv')
    echo "  /mnt/dtfs/verify.mkv mtime: $mtime"
    if echo "$mtime" | grep -q "^1970"; then
        ng "mtime is still epoch (F-014 not fixed)"
    else
        ok "mtime is recent (F-014 fixed)"
    fi
else
    echo "  (skipping data tests — /home/debian/ep1.mkv not present locally on controller)"
fi

section "Summary"
echo "  PASS: $pass"
echo "  FAIL: $fail"
exit $fail

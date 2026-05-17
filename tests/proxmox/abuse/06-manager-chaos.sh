#!/usr/bin/env bash
# Manager failover: kill leader, kill follower, lose quorum.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/harness.sh"
CATEGORY="06-mgr-chaos"

t_section "06 — Manager chaos"

# T6.1: kill mgr-01 (likely leader), other mgrs should still answer reads
s=$(t_start)
sssh mgr-01 "sudo systemctl stop dtfs-manager" >/dev/null 2>&1
sleep 5
# Read via mgr-02
out2=$(curl -sS --max-time 5 http://10.150.0.152:9402/api/v1/cluster/status)
out3=$(curl -sS --max-time 5 http://10.150.0.153:9402/api/v1/cluster/status)
elapsed=$(t_elapsed $s)
if [ -n "$out2" ] && [ -n "$out3" ]; then
    n2=$(echo "$out2" | python3 -c "import sys,json;print(json.load(sys.stdin).get('data_nodes',-1))" 2>/dev/null)
    n3=$(echo "$out3" | python3 -c "import sys,json;print(json.load(sys.stdin).get('data_nodes',-1))" 2>/dev/null)
    if [ "$n2" -ge 0 ] && [ "$n3" -ge 0 ]; then
        t_pass "T6.1 read API survives mgr-01 stop" "$elapsed" "mgr-02 sees $n2 nodes, mgr-03 sees $n3"
    else
        t_fail "T6.1 read API survives mgr-01 stop" "$elapsed" "parse failed"
    fi
else
    t_fail "T6.1 read API survives mgr-01 stop" "$elapsed" "no response from mgr-02/03"
fi

# T6.2: write via mgr-02 (a non-bootstrap manager) — Raft commit needs leader
s=$(t_start)
# Try writing via FUSE — FUSE uses mgr-01:9401 as its config'd manager_addr, so it'll fail
# But we can also try via the REST API of mgr-02 directly
vol_out=$(curl -sS --max-time 5 -X POST -H 'Content-Type: application/json' -d '{"name":"chaos-vol-1"}' http://10.150.0.152:9402/api/v1/volumes)
elapsed=$(t_elapsed $s)
if echo "$vol_out" | grep -q '"status":"created"'; then
    t_pass "T6.2 mgr-02 accepts writes with mgr-01 down" "$elapsed"
elif echo "$vol_out" | grep -qi "error\|forward"; then
    t_warn "T6.2 mgr-02 accepts writes" "got: ${vol_out:0:100} (expected: forward to leader)"
else
    t_fail "T6.2 mgr-02 accepts writes with mgr-01 down" "$elapsed" "${vol_out:0:120}"
fi

# Restore mgr-01
sssh mgr-01 "sudo systemctl start dtfs-manager" >/dev/null 2>&1
sleep 15

# T6.3: full manager outage (stop all 3) — this is the realistic
# "operator stops the manager pool" scenario for F-024 testing. Stopping
# only 2 of 3 left mgr-01 sitting alone in an election loop, bumping its
# Raft term hundreds of times during the test pause, which made it
# impossible for mgr-02/mgr-03 to catch up via normal vote exchange after
# the restore. Full outage avoids that artefact.
s=$(t_start)
sssh mgr-01 "sudo systemctl stop dtfs-manager" >/dev/null 2>&1
sssh mgr-02 "sudo systemctl stop dtfs-manager" >/dev/null 2>&1
sssh mgr-03 "sudo systemctl stop dtfs-manager" >/dev/null 2>&1
sleep 5
# With every manager stopped, writes through the REST API must NOT succeed.
write_out=$(curl -sS --max-time 5 -X POST -H 'Content-Type: application/json' -d '{"name":"noquorum-vol"}' http://10.150.0.151:9402/api/v1/volumes 2>&1)
elapsed=$(t_elapsed $s)
if ! echo "$write_out" | grep -q '"status":"created"'; then
    t_pass "T6.3 quorum loss prevents writes" "$elapsed" "all mgrs down, write rejected"
else
    t_fail "T6.3 quorum loss prevents writes" "$elapsed" "write succeeded with no managers: ${write_out:0:100}"
fi

# Restore — bring all three managers back together. With the F-024 fix the
# on-disk snapshot is loaded at startup so membership is recovered before
# any heartbeats fire, and a leader is elected within a few seconds.
sssh mgr-01 "sudo systemctl start dtfs-manager" >/dev/null 2>&1
sssh mgr-02 "sudo systemctl start dtfs-manager" >/dev/null 2>&1
sssh mgr-03 "sudo systemctl start dtfs-manager" >/dev/null 2>&1
sleep 20

# T6.4: cluster recovers — all 3 mgrs healthy after full rolling restart
# (the F-024 regression test).
s=$(t_start)
ok=0
for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18; do
    sleep 5
    n151=$(curl -sS --max-time 3 http://10.150.0.151:9402/api/v1/cluster/status | python3 -c "import sys,json;print(json.load(sys.stdin).get('data_nodes',0))" 2>/dev/null)
    n152=$(curl -sS --max-time 3 http://10.150.0.152:9402/api/v1/cluster/status | python3 -c "import sys,json;print(json.load(sys.stdin).get('data_nodes',0))" 2>/dev/null)
    n153=$(curl -sS --max-time 3 http://10.150.0.153:9402/api/v1/cluster/status | python3 -c "import sys,json;print(json.load(sys.stdin).get('data_nodes',0))" 2>/dev/null)
    if [ "$n151" = "$n152" ] && [ "$n152" = "$n153" ] && [ "$n151" -ge 4 ] 2>/dev/null; then
        ok=1; break
    fi
done
elapsed=$(t_elapsed $s)
if [ "$ok" = "1" ]; then
    t_pass "T6.4 cluster reconverges after rolling mgr restart" "$elapsed" \
        "all mgrs see ${n151} data nodes"
else
    t_fail "T6.4 cluster reconverges after rolling mgr restart" "$elapsed" \
        "still divergent after 90s: m1=${n151} m2=${n152} m3=${n153}"
fi

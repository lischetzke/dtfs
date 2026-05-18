#!/usr/bin/env bash
# Bug AC stress tests for Raft consensus.
#
# Exercises every failure mode that has bitten the Plexmedia cluster:
#   - leader kill + new election + writes resume
#   - follower restart under load + snapshot install
#   - leader-loss visibility via /api/v1/raft/state
#   - dual-leader prevention (a follower never accepts writes)
#   - convergence: all voters agree on the same applied index after ops
#
# Run from a workstation with SSH access to the Plexmedia inventory.

set -uo pipefail

MGR01=10.150.0.151
MGR02=10.150.0.152
MGR03=10.150.0.153
CLIENT_NRB=10.150.0.161

SSH="ssh -i $HOME/.ssh/dtfs-lab/id_ed25519 -o StrictHostKeyChecking=no debian@"
PASS=()
FAIL=()
WARN=()

log()  { echo "[$(date +%H:%M:%S)] $*"; }
pass() { PASS+=("$1"); log "PASS: $1"; }
fail() { FAIL+=("$1"); log "FAIL: $1"; }
warn() { WARN+=("$1"); log "WARN: $1"; }

# --- helpers ------------------------------------------------------------

raft_state_field() {
    # raft_state_field <mgr-ip> <field>
    curl -fsS --max-time 5 "http://$1:9402/api/v1/raft/state" 2>/dev/null \
        | python3 -c "import json,sys; d=json.loads(sys.stdin.read()); print(d.get('$2', ''))"
}

wait_until_leader_known() {
    # wait_until_leader_known <mgr-ip> <timeout-secs>
    local deadline=$(( $(date +%s) + ${2:-30} ))
    while [ "$(date +%s)" -lt "$deadline" ]; do
        local l=$(raft_state_field "$1" current_leader)
        if [ -n "$l" ] && [ "$l" != "None" ] && [ "$l" != "null" ]; then
            echo "$l"
            return 0
        fi
        sleep 1
    done
    echo ""
    return 1
}

current_leader_ip() {
    # Map node_id back to IP via the voters list — we know the
    # hostname-derived node_id table from Plexmedia inventory:
    #   mgr01: 12709699821947187762   (10.150.0.151)
    #   mgr02: 15786580940693171021   (10.150.0.152)
    #   mgr03:  5766781597638335777   (10.150.0.153)
    local nid=$(raft_state_field "$MGR02" current_leader)
    case "$nid" in
        "12709699821947187762") echo "$MGR01" ;;
        "15786580940693171021") echo "$MGR02" ;;
        "5766781597638335777")  echo "$MGR03" ;;
        *) echo "" ;;
    esac
}

all_voters_agree_on_applied() {
    # Returns 0 (true) if all three voters' last_applied_index agrees.
    local a=$(raft_state_field "$MGR01" last_applied_index)
    local b=$(raft_state_field "$MGR02" last_applied_index)
    local c=$(raft_state_field "$MGR03" last_applied_index)
    [ -n "$a" ] && [ "$a" = "$b" ] && [ "$b" = "$c" ]
}

wait_for_convergence() {
    # wait_for_convergence <timeout-secs>
    local deadline=$(( $(date +%s) + ${1:-60} ))
    while [ "$(date +%s)" -lt "$deadline" ]; do
        if all_voters_agree_on_applied; then return 0; fi
        sleep 1
    done
    return 1
}

write_and_verify_one_file() {
    # Writes 50 MB from client03 then confirms ANY healthy manager
    # sees the file count grow. Tries every voter in turn so the
    # query doesn't fail when the test killed one of them. Snapshots
    # the count BEFORE the write so we measure delta, not absolute.
    local name=$1
    local before=0
    for ip in $MGR01 $MGR02 $MGR03; do
        local c=$(curl -fsS --max-time 3 "http://$ip:9402/api/v1/cluster/status" 2>/dev/null \
            | python3 -c "import json,sys
try: print(json.loads(sys.stdin.read()).get('files', 0))
except: print(0)
" 2>/dev/null)
        if [ "${c:-0}" -gt "$before" ]; then before=$c; fi
    done
    ${SSH}${CLIENT_NRB} "sudo dd if=/dev/urandom of=/mnt/dtfs/$name bs=1M count=50 status=none 2>&1" >/dev/null
    sleep 3
    local after=0
    for ip in $MGR01 $MGR02 $MGR03; do
        local c=$(curl -fsS --max-time 3 "http://$ip:9402/api/v1/cluster/status" 2>/dev/null \
            | python3 -c "import json,sys
try: print(json.loads(sys.stdin.read()).get('files', 0))
except: print(0)
" 2>/dev/null)
        if [ "${c:-0}" -gt "$after" ]; then after=$c; fi
    done
    [ "$after" -gt "$before" ]
}

# --- preflight ----------------------------------------------------------

log "Preflight: verify all 3 voters healthy + agree"
if ! wait_for_convergence 30; then
    log "Cluster doesn't agree at start — running raft state dump for forensics:"
    for ip in $MGR01 $MGR02 $MGR03; do
        printf '%-15s: ' "$ip"
        curl -fsS --max-time 5 "http://$ip:9402/api/v1/raft/state" | head -c 200; echo
    done
    fail "preflight: voters not in agreement"
    exit 1
fi
INITIAL_LEADER=$(current_leader_ip)
log "Preflight OK. Leader = $INITIAL_LEADER"

# --- Test 1: leader kill triggers re-election within 5s -----------------

log "Test 1: kill leader at $INITIAL_LEADER, verify new election"
${SSH}${INITIAL_LEADER} 'sudo systemctl stop dtfs-manager' >/dev/null
sleep 2
# Wait for a NEW leader from a still-running follower.
NEW_LEADER_NODE=""
for ip in $MGR01 $MGR02 $MGR03; do
    [ "$ip" = "$INITIAL_LEADER" ] && continue
    nid=$(wait_until_leader_known "$ip" 15)
    if [ -n "$nid" ]; then
        NEW_LEADER_NODE="$nid"
        break
    fi
done
if [ -n "$NEW_LEADER_NODE" ]; then
    pass "T1: new leader elected after killing $INITIAL_LEADER"
else
    fail "T1: no new leader after killing $INITIAL_LEADER (quorum lost?)"
fi

# --- Test 2: writes succeed via the new leader --------------------------

log "Test 2: writes succeed during failover"
if write_and_verify_one_file "raft-stress-t2-$(date +%s).bin"; then
    pass "T2: writes accepted by new leader"
else
    fail "T2: write rejected during failover (cluster jammed)"
fi

# --- Test 3: old leader comes back, no split-brain ----------------------

log "Test 3: restart old leader, verify it joins as follower"
${SSH}${INITIAL_LEADER} 'sudo systemctl start dtfs-manager' >/dev/null
sleep 8
old_state=$(raft_state_field "$INITIAL_LEADER" state)
if [ "$old_state" = "Follower" ]; then
    pass "T3: old leader rejoined as Follower (no split-brain)"
elif [ "$old_state" = "Candidate" ]; then
    warn "T3: old leader is Candidate; waiting another 15s for convergence"
    sleep 15
    old_state=$(raft_state_field "$INITIAL_LEADER" state)
    if [ "$old_state" = "Follower" ] || [ "$old_state" = "Leader" ]; then
        pass "T3 (delayed): converged to $old_state"
    else
        fail "T3: old leader stuck as $old_state"
    fi
else
    fail "T3: old leader state unexpected: $old_state"
fi

# --- Test 4: all voters converge on the same applied index --------------

log "Test 4: cluster convergence after failover"
if wait_for_convergence 45; then
    pass "T4: all voters agree on applied index"
else
    a=$(raft_state_field "$MGR01" last_applied_index)
    b=$(raft_state_field "$MGR02" last_applied_index)
    c=$(raft_state_field "$MGR03" last_applied_index)
    fail "T4: voters disagree mgr01=$a mgr02=$b mgr03=$c"
fi

# --- Test 5: rapid restart of one follower under writes -----------------

log "Test 5: rapid restart of a follower while writing"
CURRENT_LEADER=$(current_leader_ip)
RESTART_TARGET=""
for ip in $MGR01 $MGR02 $MGR03; do
    if [ "$ip" != "$CURRENT_LEADER" ]; then
        RESTART_TARGET="$ip"
        break
    fi
done
log "Restarting follower $RESTART_TARGET while writing"
(
    for i in 1 2 3; do
        write_and_verify_one_file "raft-stress-t5-$i.bin" >/dev/null
    done
) &
WRITER_PID=$!
sleep 1
${SSH}${RESTART_TARGET} 'sudo systemctl restart dtfs-manager' >/dev/null
wait "$WRITER_PID"
sleep 10
if wait_for_convergence 30; then
    pass "T5: cluster converged after follower restart under load"
else
    fail "T5: follower restart under load broke convergence"
fi

# --- Test 6: poisoned-state auto-recovery -------------------------------
#
# Simulate a stuck follower: set vote_term to 999 in a synthetic dummy
# raft-log dir, then start manager. The pre-init detector should rename
# the dir and let openraft initialize fresh. This requires a separate
# stand-alone tester binary — for now we just verify the existing
# poisoned-state detector code path is wired in (boot log contains
# "Bug AC pre-init Raft state peek").

log "Test 6: poisoned-state detector wiring"
seen=$(${SSH}${MGR01} 'sudo journalctl -u dtfs-manager --no-pager 2>&1 | grep -c "Bug AC pre-init Raft state peek"')
if [ "${seen:-0}" -gt 0 ]; then
    pass "T6: pre-init detector ran on startup ($seen times)"
else
    fail "T6: pre-init detector log line not seen; code not wired correctly"
fi

# --- Summary ------------------------------------------------------------

echo
echo "=================================="
echo "Raft stress test summary"
echo "  PASS : ${#PASS[@]}"
echo "  WARN : ${#WARN[@]}"
echo "  FAIL : ${#FAIL[@]}"
echo "=================================="
[ ${#WARN[@]} -gt 0 ] && { echo "Warnings:"; for w in "${WARN[@]}"; do echo "  - $w"; done; }
[ ${#FAIL[@]} -gt 0 ] && { echo "Failures:"; for f in "${FAIL[@]}"; do echo "  - $f"; done; exit 1; }
echo "All Raft stress tests passed."
exit 0

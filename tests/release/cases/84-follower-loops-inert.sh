#!/usr/bin/env bash
# REV-1-1 / REV-1-2 — every manager background loop must be leader-gated.
#
# TaskScheduler::tick is the only gated loop. run_orphan_scrub,
# run_auto_unquarantine_sweep, run_lease_gc, TieringEngine::run and the
# POST /api/v1/cache/flush endpoint run on every manager including cross-DC
# learners, and they dispatch DIRECT data-node delete RPCs that no Raft
# rejection can undo. A follower's tiering read window is always empty (read
# samples arrive on the heartbeat the leader serves), so it reads every cache
# replica as idle, deletes the bytes, and only then fails to record the removal:
# bytes gone, location entries kept. REV-1-2 additionally requires the orphan
# reaper's state machine to be caught up, so a manager replaying an old snapshot
# cannot classify every file created during its outage as an orphan.
#
# Assert, over a window longer than one tiering + orphan-scrub interval:
#   1. no NON-LEADER manager logs a dispatch from any of those loops;
#   2. the on-disk chunk inventory of every data node is unchanged;
#   3. a file written before the window still reads back byte-identical.
#
# This case is deliberately observational: it does not partition anything, so it
# is safe to run at any point in the suite.
source "$(dirname "$0")/../lib/common.sh"

WINDOW=${REV77_WINDOW:-90}

F=""
case_cleanup() { rm_fuse_file "$F"; }
trap case_cleanup EXIT

mgr_is_leader() {
    curl "${CURL_OPTS[@]}" "http://$1:$MGR_PORT/api/v1/raft/state" 2>/dev/null \
        | python3 -c "import json,sys; print('yes' if json.load(sys.stdin).get('is_leader') else 'no')" 2>/dev/null \
        || echo unknown
}

chunk_inventory() {
    ssh_run "$1" "sudo find /mnt -name '*.dat' -printf '%f\n' 2>/dev/null | sort"
}

echo "[1/5] write a fixture and record its checksum"
F=$(make_fuse_file "release/77-gate-$$.bin" 8)
[ -n "$F" ] || fail "could not create fixture"
HASH=$(sha256_via_fuse "$F")
[ -n "$HASH" ] || fail "could not checksum fixture"
sleep 5

echo "[2/5] identify the leader and the followers"
LEADER=""
FOLLOWERS=()
for m in $(mgrs_list); do
    case "$(mgr_is_leader "$m")" in
        yes) LEADER="$m" ;;
        no)  FOLLOWERS+=("$m") ;;
    esac
done
[ -n "$LEADER" ] || fail "no raft leader reachable"
[ ${#FOLLOWERS[@]} -ge 1 ] || skip "single-manager cluster - nothing to gate"
echo "  leader=$LEADER followers=${FOLLOWERS[*]}"

echo "[3/5] snapshot the on-disk chunk inventory of every data node"
declare -A BEFORE
for d in $(data_list); do
    BEFORE["$d"]=$(chunk_inventory "$d")
done

SINCE=$(date -u +"%Y-%m-%d %H:%M:%S")
echo "[4/5] observe for ${WINDOW}s (longer than one tiering + orphan interval)"
sleep "$WINDOW"

# Each pattern is emitted only on the dispatch path of the corresponding loop.
PATTERNS='cache.demote|orphan_scrub tick reaped|auto-unquarantine cleared|DeleteChunkReplica|cache.flush_replica'
VIOLATIONS=0
for m in "${FOLLOWERS[@]}"; do
    HITS=$(ssh_run "$m" "sudo journalctl -u dtfs-manager --since '$SINCE' --no-pager 2>/dev/null | grep -Ec '$PATTERNS'" 2>/dev/null | tr -d '\r')
    HITS=${HITS:-0}
    echo "  $m: $HITS dispatch line(s)"
    if [ "$HITS" -gt 0 ]; then
        VIOLATIONS=$((VIOLATIONS + 1))
        ssh_run "$m" "sudo journalctl -u dtfs-manager --since '$SINCE' --no-pager 2>/dev/null | grep -E '$PATTERNS' | head -5" 2>/dev/null
    fi
done
[ "$VIOLATIONS" -eq 0 ] || fail "$VIOLATIONS follower manager(s) dispatched background-loop work"

echo "[5/5] no chunk disappeared and the fixture still reads back intact"
for d in $(data_list); do
    AFTER=$(chunk_inventory "$d")
    MISSING=$(comm -23 <(echo "${BEFORE[$d]}") <(echo "$AFTER") | wc -l | tr -d ' ')
    [ "$MISSING" -eq 0 ] || fail "$d lost $MISSING chunk(s) during the observation window"
done
assert_eq "$HASH" "$(sha256_via_fuse "$F")" "fixture readback after the window"
pass

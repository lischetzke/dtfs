#!/usr/bin/env bash
# CNF-2 / Bug CN — terminal migration tasks must DRAIN on the CONFIGURED
# retention windows, and the prune must keep running with a data node down.
#
# 2026-06-10 dtfs-lm incident aftermath: a flood left thousands of terminal
# tasks in migration_state_json; Bug CN hardened the scheduler and CNF-2 made
# the retention windows configurable (raft.failed_task_retention_secs /
# raft.done_task_retention_secs) so this case can observe the prune without
# waiting the production 5/30 minutes.
#
# Flow:
#   1. Set short retention (Done 30s / Failed 45s) on every manager + restart
#      (followers first, leader last).
#   2. Seed fresh files (cold-born), save the volume's mandatory_replicas.warm.
#   3. set-mandatory warm=1 -> the under-replication scheduler queues real
#      warm backfills -> Done tasks appear (scheduler ticking, tasks complete).
#   4. Assert the Done backlog prunes to 0 within 150s of first observation:
#      discriminates the configured 30s window from the 300s compiled-in
#      default (a non-configurable binary keeps the tasks >=300s -> timeout).
#   5. Stop a data node: the tick loop must keep running (Bug CN) — a second
#      backfill wave (coverage loss) and any Failed stragglers must still
#      drain on their configured windows.
# Cleanup restores mandatory_replicas, the manager configs, and the victim.
#
# NOT in QUICK_CASES: 2x rolling manager restarts put the runtime at ~5-10 min.
source "$(dirname "$0")/../lib/common.sh"

MGR_TOML=/etc/dtfs/manager.toml
BAK=$MGR_TOML.case61.bak
FAILED_RETENTION=45
DONE_RETENTION=30

PREFIX="release/61-backlog-$$"
DIRF="$MOUNT/$PREFIX"

# ── Helpers ──────────────────────────────────────────────────────
migr_json() { mgr_get_any /api/v1/migrations; }
count_status() {  # <status> — cluster-wide task count with that status
    migr_json | python3 -c "
import json,sys
try:
    d=json.load(sys.stdin)
    print(sum(1 for t in d.get('tasks',[]) if t.get('status')=='$1'))
except Exception:
    print(-1)
"
}
have_done()      { [ "$(count_status done)" -ge 1 ]; }
done_drained()   { [ "$(count_status done)" = "0" ]; }
failed_drained() { [ "$(count_status failed)" = "0" ]; }

victim_healthy() {
    mgr_get_any /api/v1/nodes | python3 -c "
import json,sys
for n in json.load(sys.stdin):
    if n.get('hostname') == '$VICTIM':
        sys.exit(0 if n.get('health') == 'Healthy' else 1)
sys.exit(1)
"
}

restart_manager() {  # <host> — restart + wait for its API to answer again
    ssh_run "$1" 'sudo systemctl restart dtfs-manager' || return 1
    wait_for 60 "manager $1 API back" \
        curl "${CURL_OPTS[@]}" -o /dev/null "http://$1:$MGR_PORT/api/v1/health"
}

CONFIGS_TOUCHED=0
MANDATORY_TOUCHED=0
ORIG_WARM=""
case_cleanup() {
    # Restore the volume's mandatory warm count first (stops new backfills).
    if [ "$MANDATORY_TOUCHED" = "1" ] && [ -n "$ORIG_WARM" ]; then
        mgr_post_leader "/api/v1/volumes/$VOL/set-mandatory" \
            "{\"warm_replicas\":$ORIG_WARM}" >/dev/null 2>&1 || true
    fi
    [ -n "${VICTIM:-}" ] && ssh_run "$VICTIM" 'sudo systemctl start dtfs-data' \
        >/dev/null 2>&1 || true
    # Restore original manager configs (backup exists only if we got far
    # enough to take one) and restart so the prod retention applies again.
    if [ "$CONFIGS_TOUCHED" = "1" ]; then
        for h in $(mgrs_list); do
            ssh_run "$h" "if sudo test -f $BAK; then \
                sudo mv $BAK $MGR_TOML && sudo systemctl restart dtfs-manager; fi" \
                >/dev/null 2>&1 || true
        done
        wait_for_soft 90 cluster_api_up || true
    fi
    [ -n "${VICTIM:-}" ] && { wait_for_soft 90 victim_healthy || true; }
    client_run "sudo rm -rf '$DIRF'" >/dev/null 2>&1 || true
}
trap case_cleanup EXIT

# ── Prereqs ──────────────────────────────────────────────────────
nodes_json=$(mgr_get_any /api/v1/nodes) || skip "no manager reachable"
echo "$nodes_json" | grep -q '"role":"Data"' || skip "no data nodes registered"
for h in $(mgrs_list); do
    ssh_run "$h" "sudo test -f $MGR_TOML" \
        || skip "$MGR_TOML not found on $h (cannot set short retention)"
done

vol_json=$(mgr_get_any "/api/v1/volumes/$VOL") || skip "volume $VOL not readable"
ORIG_WARM=$(echo "$vol_json" | python3 -c "
import json,sys
print(json.load(sys.stdin).get('mandatory_replicas',{}).get('warm',''))
")
[ -n "$ORIG_WARM" ] || skip "cannot read mandatory_replicas.warm of $VOL"
echo "$nodes_json" | python3 -c "
import json,sys
warm=[n for n in json.load(sys.stdin)
      if n.get('role')=='Data'
      and any(d.get('tier')=='warm' for d in n.get('disks',[]))]
sys.exit(0 if warm else 1)
" || skip "no warm-tier data node (cannot provoke warm backfills)"

# Victim = the data node holding the most bytes (most likely to have
# in-flight work when stopped).
VICTIM=$(echo "$nodes_json" | python3 -c "
import json,sys
ds=[n for n in json.load(sys.stdin) if n.get('role')=='Data']
ds.sort(key=lambda n: n.get('used_capacity',0), reverse=True)
print(ds[0]['hostname'] if ds else '')
")
[ -n "$VICTIM" ] || VICTIM=$(data_list | awk '{print $NF}')
echo "INFO: victim=$VICTIM orig_mandatory_warm=$ORIG_WARM"

# ── 1. Short retention on every manager, leader restarted last ───
echo "[1/5] set done/failed retention = ${DONE_RETENTION}s/${FAILED_RETENTION}s on managers"
CONFIGS_TOUCHED=1
LEADER=$(mgr_leader) || LEADER=""
for h in $(mgrs_list); do
    # Keep the ORIGINAL config if a crashed earlier run left a backup
    # (explicit test, not 'cp -n': coreutils >=9.2 exits nonzero on skip).
    ssh_run "$h" "{ sudo test -f $BAK || sudo cp $MGR_TOML $BAK; } && \
        sudo sed -i '/^failed_task_retention_secs/d;/^done_task_retention_secs/d' $MGR_TOML && \
        if ! sudo grep -q '^\[raft\]' $MGR_TOML; then printf '\n[raft]\n' | sudo tee -a $MGR_TOML >/dev/null; fi && \
        sudo sed -i -e '/^\[raft\]/a failed_task_retention_secs = $FAILED_RETENTION' \
                    -e '/^\[raft\]/a done_task_retention_secs = $DONE_RETENTION' $MGR_TOML && \
        sudo grep -q '^failed_task_retention_secs' $MGR_TOML" \
        || fail "could not rewrite [raft] retention on $h"
done
for h in $(mgrs_list); do
    [ "$h" = "$LEADER" ] && continue
    restart_manager "$h" || fail "manager $h did not come back after restart"
done
[ -n "$LEADER" ] && { restart_manager "$LEADER" || fail "leader $LEADER did not come back"; }
wait_for 90 "raft re-elected a leader" mgr_leader

# ── 2. Seed cold-born files ──────────────────────────────────────
echo "[2/5] seed files under $PREFIX"
client_run "sudo mkdir -p '$DIRF'" || skip "cannot write via the FUSE mount"
for i in 1 2 3 4 5 6; do
    make_fuse_file "$PREFIX/f$i.bin" 6 >/dev/null || fail "seed write f$i failed"
done

# ── 3. mandatory warm=1 -> real backfill tasks -> Done ───────────
echo "[3/5] set-mandatory warm=1 and wait for Done tasks"
MANDATORY_TOUCHED=1
RESP=$(mgr_post_leader "/api/v1/volumes/$VOL/set-mandatory" '{"warm_replicas":1}')
echo "  set-mandatory response: $RESP"
echo "$RESP" | grep -qi 'error' && fail "set-mandatory rejected: $RESP"

# Backfills appear on the next scheduler tick (30s cadence) and complete
# fast on a healthy LAN. If the seeds were warm-born on this deployment
# there is nothing to backfill -> honest skip.
if ! wait_for_soft 180 have_done; then
    skip "no Done backfill tasks appeared (seeds already warm-satisfied on this topology)"
fi
DONE_SEEN_AT=$(date +%s)
echo "  done tasks observed: $(count_status done)"

# ── 4. Done backlog must prune on the CONFIGURED 30s window ──────
echo "[4/5] Done tasks must prune within 150s of first observation (configured 30s + ticks)"
# Discriminating assert: with the 300s compiled-in default the tasks are
# still retained at the 150s mark and this wait_for fails the case; only
# the configured 30s window drains in time. The prune runs inside tick().
ELAPSED=$(( $(date +%s) - DONE_SEEN_AT ))
REMAIN=$(( 150 - ELAPSED )); [ "$REMAIN" -lt 30 ] && REMAIN=30
wait_for "$REMAIN" "Done backlog pruned to 0 on the 30s window" done_drained
echo "  done backlog drained $(( $(date +%s) - DONE_SEEN_AT ))s after first observation"

# ── 5. Node down: scheduler keeps ticking; stragglers still prune ─
echo "[5/5] stop $VICTIM — tick loop must survive and keep pruning (Bug CN)"
ssh_run "$VICTIM" 'sudo systemctl stop dtfs-data' || skip "cannot stop dtfs-data on $VICTIM"
# Chunks whose warm replica sat on the victim lose live coverage once its
# health commits Down -> a second backfill wave (target = the surviving
# warm node) -> Done -> pruned again. Placement-dependent, so soft: no
# wave is fine; a wave that appears MUST drain (prune with a node down).
if wait_for_soft 240 have_done; then
    echo "  second backfill wave observed with $VICTIM down"
    wait_for 240 "second-wave Done pruned with $VICTIM down" done_drained
else
    echo "  no second wave (no warm replica lived on $VICTIM) — fine"
fi
FAILED_NOW=$(count_status failed)
if [ "$FAILED_NOW" -ge 1 ]; then
    echo "  $FAILED_NOW Failed task(s) (copies caught by the stop) must prune on the 45s window"
    wait_for 300 "Failed backlog pruned to 0" failed_drained
fi

# Cluster sane: managers healthy, raft has a leader.
for h in $(mgrs_list); do
    curl "${CURL_OPTS[@]}" "http://$h:$MGR_PORT/api/v1/health" \
        | grep -q '"healthy"' || fail "manager $h unhealthy after drain"
done
mgr_leader >/dev/null || fail "raft lost its leader"

pass

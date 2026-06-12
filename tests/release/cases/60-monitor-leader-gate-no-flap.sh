#!/usr/bin/env bash
# Bug CM — only the raft LEADER may evaluate + commit data-node liveness.
#
# 2026-06-09 production incident (dtfs-lm v0.1.5): the health monitor ran on
# every manager with no leader gate. Bug CJ (#29) had moved liveness onto the
# replicated last_seen_ms, assuming "every manager observes the same
# freshness" — but a learner on a flaky cross-DC link is bursty-stale between
# raft catch-up rounds, so its 10s monitor tick read an OLD last_seen, judged
# the data node Down, and committed UpdateNodeStatus(down). That fought the
# heartbeat handler's healthy write and the node FLAPPED (~9s period), which
# in turn drove a cache.promote raft-write flood on the leader. The scheduler
# already avoids this with its Bug AC leader gate; the monitor now mirrors it.
#
# With every data node healthy, this case watches each manager's journal over
# a multi-tick window and asserts that NO non-leader manager committed a
# monitor Down. Pre-fix a lagging follower/learner emits them; post-fix only
# the leader ever can (and with a healthy roster, none do).
source "$(dirname "$0")/../lib/common.sh"

WINDOW=${CM_WINDOW:-70}   # > several 10s monitor ticks

all_data_healthy() {
    mgr_get_any "/api/v1/nodes" | python3 -c "
import json,sys
ds=[n for n in json.load(sys.stdin) if n.get('role')=='Data']
sys.exit(0 if ds and all(n.get('health')=='Healthy' for n in ds) else 1)
"
}

# ── Prereqs ──────────────────────────────────────────────────────
raft=$(mgr_get_any "/api/v1/raft/state") || skip "no manager reachable"
leader=$(echo "$raft" | python3 -c "
import json,sys
d=json.load(sys.stdin)
cl=d.get('current_leader')
if cl is None: sys.exit(3)
for m in d.get('membership',[]):
    if str(m.get('node_id'))==str(cl):
        print(m.get('hostname','')); break
") || skip "raft has no leader"
[ -n "$leader" ] || skip "leader hostname unknown"

all_data_healthy || skip "not all data nodes Healthy at start (real Down != flap)"

# ── Act: watch a multi-tick window ───────────────────────────────
start=$(date +%s)
sleep "$WINDOW"

# If a node genuinely dropped during the window, a Down is legitimate — skip
# rather than fail (this case only proves the no-FLAP property on a healthy
# roster).
all_data_healthy || skip "a data node went Down during the window"

# ── Assert: no NON-leader manager committed a monitor Down in-window ──
DOWN_RE='is DOWN|UpdateNodeStatus\([0-9]+, down\)'
viol=0
for h in $(mgrs_list); do
    [ "$h" = "$leader" ] && continue
    n=$(ssh_run "$h" "sudo journalctl -u dtfs-manager --since '@$start' --no-pager 2>/dev/null \
        | grep 'dtfs_manager::monitor' | grep -icE '$DOWN_RE'") || n=0
    n=${n:-0}
    echo "INFO: non-leader $h monitor-down lines in ${WINDOW}s window: $n"
    [ "$n" -gt 0 ] && viol=$((viol+1))
done

[ "$viol" -eq 0 ] || fail "non-leader manager committed monitor Down for a healthy node (Bug CM regression)"

echo "INFO: leader=$leader window=${WINDOW}s; non-leader managers committed 0 Down transitions"
pass

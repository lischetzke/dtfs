#!/usr/bin/env bash
# Bug CH — node loss must NOT trigger a migration-task storm.
#
# 2026-06-06 production incident: the monitor's own evacuation path ran on
# every manager, round-robined replication targets over ALL registered nodes
# (managers + FUSE client included), hardcoded tier=hot, and re-fired every
# tick — flooding the leader with ~1920 garbage tasks/40s (36k persisted)
# until it OOM-wedged and the cluster lost quorum. Post-CH, repair is owned
# solely by the leader-gated scheduler tick via live-replica counting.
#
# This case stops one data node, waits past the down threshold + several
# monitor/scheduler ticks, then asserts:
#   A. no migration task targets a non-Data node (manager / client);
#   B. no migration task names a tier no live data disk provides;
#   C. the task queue does not grow unboundedly between two polls;
#   D. managers stay healthy and raft keeps a leader throughout.
# Restarts the data node in cleanup.
source "$(dirname "$0")/../lib/common.sh"

# Victim = the data node holding the most bytes, so the down node actually
# has replicas to lose (a victim with no chunks exercises nothing — the
# pre-CH storm needed a chunk on the down node WITH a surviving second
# replica as copy source).
VICTIM=$(mgr_get_any "/api/v1/nodes" | python3 -c "
import json,sys
ds=[n for n in json.load(sys.stdin) if n.get('role')=='Data']
ds.sort(key=lambda n: n.get('used_capacity',0), reverse=True)
print(ds[0]['hostname'] if ds else '')
")
[ -n "$VICTIM" ] || VICTIM=$(data_list | awk '{print $NF}')

victim_health_is() {
    mgr_get_any "/api/v1/nodes" | python3 -c "
import json,sys
for n in json.load(sys.stdin):
    if n.get('hostname') == '$VICTIM':
        sys.exit(0 if n.get('health') == '$1' else 1)
sys.exit(1)
"
}
victim_down()    { victim_health_is "Down"; }
victim_healthy() { victim_health_is "Healthy"; }

case_cleanup() {
    ssh_run "$VICTIM" 'sudo systemctl start dtfs-data' || true
    # Wait for re-register so later cases see a full roster.
    wait_for_soft 90 victim_healthy || true
}
trap case_cleanup EXIT

count_chunks() {
    mgr_get_any "/api/v1/files" | python3 -c "
import json,sys
d=json.load(sys.stdin)
print(sum(f.get('chunk_count',0) for f in d.get('files',[])))
"
}
have_chunks() { [ "$(count_chunks)" -gt 0 ]; }

migr_count() {
    mgr_get_any "/api/v1/migrations" | python3 -c "
import json,sys
try:
    print(json.load(sys.stdin).get('count',0))
except Exception:
    print(-1)
"
}

# ── Prereqs ──────────────────────────────────────────────────────
nodes_json=$(mgr_get_any "/api/v1/nodes") || skip "no manager reachable"
echo "$nodes_json" | grep -q '"role":"Data"' || skip "no data nodes registered"

# Need at least one file with chunks so the down node actually holds replicas.
if ! have_chunks; then
    client_run "head -c 4M /dev/urandom > $MOUNT/ch-storm-seed.bin && sync" \
        || skip "cannot seed a test file via the FUSE mount"
    wait_for 60 "seed file committed chunks" have_chunks
fi
total_chunks=$(count_chunks)

# Snapshot the non-Data node ids and the union of live data-disk tiers.
non_data_ids=$(echo "$nodes_json" | python3 -c "
import json,sys
print(' '.join(str(n['node_id']) for n in json.load(sys.stdin)
               if n.get('role') != 'Data'))
")
live_tiers=$(echo "$nodes_json" | python3 -c "
import json,sys
ts=set()
for n in json.load(sys.stdin):
    if n.get('role') != 'Data':
        continue
    for d in n.get('disks', []):
        if d.get('status') not in ('failed','removed'):
            ts.add(d.get('tier'))
print(' '.join(sorted(ts)))
")

baseline=$(migr_count)
[ "$baseline" -ge 0 ] || skip "migrations endpoint unavailable"

# ── Act: stop one data node, let Down detection + several ticks pass ─
ssh_run "$VICTIM" 'sudo systemctl stop dtfs-data' || skip "cannot stop dtfs-data on $VICTIM"

wait_for 90 "victim node reported Down" victim_down

sleep 45   # > monitor tick (10s) + scheduler tick (30s): storm window opens
poll1=$(migr_count)
tasks1=$(mgr_get_any "/api/v1/migrations")
sleep 45   # a second window — pre-fix the count keeps climbing here
poll2=$(migr_count)

# ── Assert D: managers healthy, raft has a leader ────────────────
for h in $(mgrs_list); do
    curl "${CURL_OPTS[@]}" "http://$h:$MGR_PORT/api/v1/health" \
        | grep -q '"healthy"' || fail "manager $h unhealthy after node loss"
done
mgr_get_any "/api/v1/raft/state" | python3 -c "
import json,sys
d=json.load(sys.stdin)
sys.exit(0 if d.get('current_leader') is not None else 1)
" || fail "raft lost its leader after a data-node stop"

# ── Assert A+B: no garbage-targeted / impossible-tier tasks ──────
echo "$tasks1" | python3 -c "
import json,sys
non_data={s for s in '''$non_data_ids'''.split() if s}
live={s for s in '''$live_tiers'''.split() if s}
d=json.load(sys.stdin)
bad_target=[t for t in d.get('tasks',[]) if str(t.get('target_node')) in non_data]
bad_tier=[t for t in d.get('tasks',[]) if t.get('tier') not in live]
if bad_target:
    print('task targets non-Data node:', bad_target[0]); sys.exit(1)
if bad_tier:
    print('task names a tier with no live disk:', bad_tier[0]); sys.exit(2)
" || fail "garbage migration tasks present (Bug CH regression)"

# ── Assert C: bounded queue, no runaway growth ───────────────────
# Legit backfill is at most one task per (chunk, short tier); allow slack 2x.
bound=$(( total_chunks * 2 + baseline + 16 ))
assert_ge "$bound" "$poll1" "bound vs migration count after first window"
assert_ge "$bound" "$poll2" "bound vs migration count after second window"
# Pre-fix signature: every monitor pass re-adds ~per-chunk tasks, so the
# delta between two 45s windows would exceed the whole chunk count.
growth=$(( poll2 - poll1 ))
assert_ge "$total_chunks" "$growth" "chunk count vs task growth between polls"

echo "INFO: chunks=$total_chunks baseline=$baseline poll1=$poll1 poll2=$poll2 victim=$VICTIM"
pass

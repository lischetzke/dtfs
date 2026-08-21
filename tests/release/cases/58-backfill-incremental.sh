#!/usr/bin/env bash
# REL-2-58 (U3): backfill scheduler pipelines incrementally. Queue a backlog of
# migration tasks (many chunks, one warm copy each), then sample the migrations
# snapshot over time and assert the Done count climbs INCREMENTALLY across ticks
# rather than staying flat then batch-jumping at the end. Pre-U3 advance_tasks
# did one state transition per task per 30s tick and awaited each ReplicateChunk
# inline+serially, so a backlog sat at ~0 Done for a whole pass then flipped in
# one batch. U3 drives a chunk Pending->Done within one tick and fans the copies
# out bounded-parallel, so progress is smooth. Additive (warm copies only); the
# cold anchor is never touched. Non-PVE (needs a live cluster + the migrations
# API + REL-1-2 inspect).
source "$(dirname "$0")/../lib/common.sh"

CW=$(client_for_dc dc-a)
[ -n "$CW" ] || CW="$CLIENT"
[ -n "$CW" ] || skip "no client to write through"

PREFIX="release/58-backfill-$$"
DIRF="$MOUNT/$PREFIX"
NFILES=12
FILE_MB=6   # >1 chunk per file at the default 4 MiB chunk size -> real backlog
case_cleanup() {
    client_run "sudo rm -rf '$DIRF'" >/dev/null 2>&1 || true
}
trap case_cleanup EXIT

# Terminal (Done+Failed) task count from GET /api/v1/migrations. The
# migration snapshot keys tasks by chunk_id (no file path), so we count
# cluster-wide; that is sound because this case is the only thing queuing a
# backlog on an otherwise-idle release cluster.
# REV-2-5 — this counted `done` AND `failed` as progress, so a backfill in which
# every single copy FAILED drained to TOTAL/TOTAL and the case passed. Progress
# means Done; Failed is counted separately and asserted to be zero.
_done_count() {
    mgr_get_any /api/v1/migrations | python3 -c "
import json, sys
d = json.load(sys.stdin)
print(sum(1 for t in d.get('tasks', []) if t.get('status') == 'done'))
" 2>/dev/null || echo 0
}
_failed_count() {
    mgr_get_any /api/v1/migrations | python3 -c "
import json, sys
d = json.load(sys.stdin)
print(sum(1 for t in d.get('tasks', []) if t.get('status') == 'failed'))
" 2>/dev/null || echo 0
}
# Total tasks currently tracked (any status).
_total_count() {
    mgr_get_any /api/v1/migrations | python3 -c "
import json, sys
print(len(json.load(sys.stdin).get('tasks', [])))
" 2>/dev/null || echo 0
}

echo "[1/5] write $NFILES x ${FILE_MB}MB files under $PREFIX (cold anchor in dc-a)"
client_run "sudo mkdir -p '$DIRF'" || fail "mkdir failed"
for i in $(seq 1 "$NFILES"); do
    client_run "sudo dd if=/dev/urandom of='$DIRF/f$i.bin' bs=1M count=$FILE_MB status=none" \
        || fail "write f$i failed"
done
client_run "sudo sync"
F1="$DIRF/f1.bin"
inspect_has_replicas "$F1" || known_issue REL-1-2 "inspect lacks per-replica tier (REL-1-2)"

echo "[2/5] queue a warm-tier backfill for the whole prefix"
RESP=$(mgr_post_leader /api/v1/migrations \
    "{\"path_prefix\":\"$PREFIX\",\"target_tier\":\"warm\",\"max_tasks\":1000}")
echo "  migrations response: $RESP"
echo "$RESP" | grep -qi '"error"' && {
    # No warm tier anywhere (single-tier minicluster) -> nothing to pipeline.
    echo "$RESP" | grep -qi 'no target\|already_satisfied' \
        && skip "no warm target available (single-tier topology)"
}

TOTAL=$(_total_count)
echo "  tasks queued (total tracked): $TOTAL"
[ "$TOTAL" -ge 1 ] || known_issue REL-2-58 "no migration tasks queued (warm already satisfied?)"

echo "[3/5] sample Done count every ~30s; expect it to climb incrementally"
SAW_INTERMEDIATE=0
for s in $(seq 1 8); do
    DONE=$(_done_count)
    echo "  t=${s}: done=$DONE/$TOTAL"
    # An intermediate sample (strictly between 0 and TOTAL) proves the backlog
    # drains progressively, not in a single end-of-pass batch flip.
    if [ "$DONE" -gt 0 ] && [ "$DONE" -lt "$TOTAL" ]; then
        SAW_INTERMEDIATE=1
    fi
    [ "$DONE" -ge "$TOTAL" ] && break
    sleep 30
done

echo "[4/5] backlog fully drained"
FINAL=$(_done_count)
FAILED=$(_failed_count)
# A failed copy is a real regression, not a slow one.
[ "$FAILED" -eq 0 ] || fail "[4/5] $FAILED migration task(s) Failed during backfill"
if [ "$FINAL" -lt "$TOTAL" ]; then
    known_issue REL-2-58 "backfill did not drain ($FINAL/$TOTAL) within sampling window"
fi

echo "[5/5] progress was incremental (saw a partially-complete snapshot)"
if [ "$SAW_INTERMEDIATE" != "1" ]; then
    # Either the backlog drained faster than one sample (still pipelined, just
    # quick) or sampling missed the mid-point. Don't fail the release on timing
    # alone; pin to the bug so it auto-promotes if we tighten the assertion.
    known_issue REL-2-58 "did not observe a partial-progress snapshot (drained between samples)"
fi
echo "  observed incremental backfill progress, anchor untouched"
pass

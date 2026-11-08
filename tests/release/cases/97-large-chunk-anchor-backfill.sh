#!/usr/bin/env bash
# WI-04: the manager's outer ReplicateChunk deadline was a fixed 60s while
# the data node's own inner transfer budget for the SAME copy was a fixed
# 120s (outer < inner for identical work), and per-target copy fan-out was
# never bounded (only the global MIGRATION_COPY_CONCURRENCY=6 cap existed).
# Every anchor-repair copy targets the single anchor DC's data node, so at
# 6-way fan-out a chunk above ~50 MB never finished a copy before the 60s
# manager deadline fired and aborted it -- and because ALL six queued copies
# hit the same wall together, the retries collapsed in lockstep too. This
# case drives an anchor backfill with 128 MB chunks (well above that
# breakeven) and asserts it actually converges: cluster-wide anchor_pending
# drains to 0, no migration task lands Failed, and no NEW migration.failed
# audit entry appears during the window.
#
# Honest caveat: on an unshaped lab this also passes pre-fix -- the link is
# fast enough that the old fixed deadlines never bite. This case gates the
# steady-state behaviour; the actual pre-fix reproduction needs the LELI
# netshape profile (§ A.7) applied to the anchor DC's uplink, which is a
# manual live-verification step (B.3), not part of this script.
#
# SCOPE: steady-state coverage only. This is NOT a pre-fix regression gate --
# on an unshaped lab it passes pre-fix. The WI-04 deadline invariant is gated
# by the dtfs-common unit tests (b4_* in crates/dtfs-common/src/replication.rs);
# the pre-fix reproduction needs the LELI netshape profile (A.7) on the anchor
# DC uplink and is a manual live-verification step (B.3).
#
# Not in QUICK_CASES: mutates the cluster-wide default chunk size and moves
# hundreds of MB through the FUSE mount.
source "$(dirname "$0")/../lib/common.sh"

ORIG_CHUNK_MB=""
ANCHOR_SET=0
ANCHOR_DC=""
ORIG_MANDATORY_JSON=""
PREFIX="release/97-anchor-$$"
DIRF="$MOUNT/$PREFIX"
FILE_MB=384          # 3 chunks @ 128 MB -- "several chunks", well above the
                      # ~50 MB (6-way) / ~300 MB (solo) breakeven in between.

restore_chunk_size() {
    [ -n "$ORIG_CHUNK_MB" ] || return 0
    mgr_post_leader /api/v1/config \
        "{\"config_json\":\"{\\\"defaults\\\":{\\\"chunk_size_mb\\\":$ORIG_CHUNK_MB}}\",\"write_to_disk\":false}" \
        >/dev/null 2>&1 || true
}
restore_anchor() {
    [ "$ANCHOR_SET" = "1" ] || return 0
    mgr_post_leader "/api/v1/volumes/$VOL/set-mandatory" "$ORIG_MANDATORY_JSON" >/dev/null 2>&1 || true
}
case_cleanup() {
    client_run "sudo rm -rf '$DIRF'" >/dev/null 2>&1 || true
    restore_chunk_size
    restore_anchor
}
trap case_cleanup EXIT INT TERM

echo "[1/6] resolve an anchor datacenter for \$VOL (use existing, else set one)"
VOLS=$(mgr_get_any /api/v1/volumes) || fail "volumes unreachable"
ANCHOR_DC=$(echo "$VOLS" | python3 -c "
import json,sys
d = json.load(sys.stdin)
for r in d.get('volumes', []):
    if r.get('volume_id') == '$VOL':
        a = r.get('anchor') or {}
        print(a.get('datacenter') or '')
        break
" 2>/dev/null)
if [ -z "$ANCHOR_DC" ]; then
    NODES=$(mgr_get_any /api/v1/nodes) || fail "nodes unreachable"
    ANCHOR_DC=$(echo "$NODES" | python3 -c "
import json,sys
d = json.load(sys.stdin)
def node_dc(n):
    # NodeResponse.location is a STRING: 'region/datacenter/rack/host'
    # (api.rs NodeResponse::from_entry); '?' marks unknown segments.
    parts = str(n.get('location') or '').split('/')
    return (parts[1] if len(parts) > 1 else '').strip()
for n in d:
    if str(n.get('role','')).lower() == 'data':
        dc = node_dc(n)
        if dc and dc != '?':
            print(dc); break
" 2>/dev/null)
    [ -n "$ANCHOR_DC" ] || skip "no data node exposes a datacenter to anchor to"
    ORIG_MANDATORY_JSON=$(echo "$VOLS" | python3 -c "
import json,sys
d = json.load(sys.stdin)
m = {'hot_replicas':2,'warm_replicas':2,'cold_replicas':0}
for r in d.get('volumes', []):
    if r.get('volume_id') == '$VOL':
        mr = r.get('mandatory_replicas') or {}
        m = {
            'hot_replicas': mr.get('hot', 2),
            'warm_replicas': mr.get('warm', 2),
            'cold_replicas': mr.get('cold', 0),
        }
        break
print(json.dumps(m))
" 2>/dev/null)
    [ -n "$ORIG_MANDATORY_JSON" ] || ORIG_MANDATORY_JSON='{"hot_replicas":2,"warm_replicas":2,"cold_replicas":0}'
    RESP=$(mgr_post_leader "/api/v1/volumes/$VOL/set-anchor" \
        "{\"datacenter\":\"$ANCHOR_DC\",\"mandatory_replicas\":1}")
    echo "$RESP" | grep -qi '"error"' && skip "set-anchor rejected: $RESP"
    mgr_post_leader "/api/v1/volumes/$VOL/set-mandatory" \
        '{"hot_replicas":0,"warm_replicas":0,"cold_replicas":1}' >/dev/null
    ANCHOR_SET=1
fi
echo "  anchor datacenter: $ANCHOR_DC"

echo "[2/6] set the cluster default chunk size to 128 MB (live override)"
CFG=$(mgr_get_any /api/v1/config) || fail "config unreachable"
ORIG_CHUNK_MB=$(echo "$CFG" | python3 -c "
import json,sys
print(json.load(sys.stdin)['defaults']['chunk_size_mb'])
" 2>/dev/null)
case "$ORIG_CHUNK_MB" in
    ''|*[!0-9]*) fail "could not read defaults.chunk_size_mb from /api/v1/config" ;;
esac
mgr_post_leader /api/v1/config \
    '{"config_json":"{\"defaults\":{\"chunk_size_mb\":128}}","write_to_disk":false}' >/dev/null \
    || fail "config write rejected"
echo "  chunk_size_mb: $ORIG_CHUNK_MB -> 128"

echo "[3/6] snapshot the pre-window migration.failed audit count"
BEFORE_FAILED=$(mgr_get_any '/api/v1/audit?action=migration.failed&limit=500' \
    | python3 -c "import json,sys; print(len(json.load(sys.stdin)))" 2>/dev/null || echo 0)

echo "[4/6] write a ${FILE_MB}MB file (3x 128MB chunks) from a non-anchor client"
CW=$(client_for_dc dc-b)
[ -n "$CW" ] || CW="$CLIENT"
[ -n "$CW" ] || skip "no client to write through"
ssh_run "$CW" "sudo mkdir -p '$DIRF'" || fail "mkdir failed"
ssh_run "$CW" "sudo dd if=/dev/urandom of='$DIRF/big.bin' bs=1M count=$FILE_MB status=none" \
    || fail "write failed"
ssh_run "$CW" "sudo sync"

# RESTORE EARLY: defaults.chunk_size_mb only matters at write time -- the
# write above already used 128 MB chunks. Restore now, before the long
# step-5 wait, so a hard kill during the wait never strands the cluster at
# 128 MB. Blank ORIG_CHUNK_MB so case_cleanup's restore_chunk_size is a no-op.
restore_chunk_size
echo "  chunk_size_mb restored to $ORIG_CHUNK_MB"
ORIG_CHUNK_MB=""

echo "[5/6] wait for cluster-wide anchor_pending to drain to 0"
anchor_drained() {
    local ap
    ap=$(mgr_get_any /api/v1/stats | python3 -c "
import json,sys
print(json.load(sys.stdin).get('anchor_pending', 999999))
" 2>/dev/null)
    [ "$ap" = "0" ]
}
wait_for 600 "anchor_pending drains to 0" anchor_drained

echo "[6/6] no migration task Failed, and no NEW migration.failed audit entry"
FAILED=$(mgr_get_any /api/v1/migrations | python3 -c "
import json,sys
d = json.load(sys.stdin)
print(sum(1 for t in d.get('tasks', []) if t.get('status') == 'failed'))
" 2>/dev/null || echo 0)
[ "${FAILED:-0}" -eq 0 ] || fail "$FAILED migration task(s) Failed during 128MB anchor backfill"

AFTER_FAILED=$(mgr_get_any '/api/v1/audit?action=migration.failed&limit=500' \
    | python3 -c "import json,sys; print(len(json.load(sys.stdin)))" 2>/dev/null || echo 0)
[ "$AFTER_FAILED" -gt "$BEFORE_FAILED" ] && \
    fail "migration.failed audit count grew ($BEFORE_FAILED -> $AFTER_FAILED) during the backfill window"

echo "  anchor backfill converged: anchor_pending=0 failed_tasks=0 migration.failed_delta=0"
pass

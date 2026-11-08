#!/usr/bin/env bash
# CR-3-3 (MGR-1): the leader tiering engine's ReportChunkReads intercept
# (forward_client_write) must resolve the REAL file_path + the replica's
# REAL tier before feeding the tiering engine, and `evaluate()`'s CO-4-6
# AV-volume skip must fire for it. Pre-fix the intercept always passed
# `file_path=""` and `DiskTier::Hot`, so `path_to_volume.get("")` was
# always `None` and the CO-4-6 skip never triggered: an AV-cache volume's
# chunks were still fed into the legacy temperature-migration path,
# bypassing the promote/cooldown machinery and (for chunks with zero
# total reads) misclassifying as Frozen -> scheduling a doomed Tape
# migration every tick ("No suitable target node" spam).
#
# Shape of the probe: enable an active AV cache policy (warm cache +
# mandatory cold) on the volume, write + repeatedly read a file so the
# data node reports chunk reads via heartbeat, then confirm the audit
# log carries NO legacy "Bug BX-4 auto-scheduled re-replication" or Tape
# migration entries for this file's chunks — only the AV cache-engine's
# own promote/demote audit rows (or none, if the promotion hasn't fired
# yet) should appear.
#
# The unit-level regression is
# metadata::tests::cr_3_3_known_chunk_resolves_real_path_and_tier and
# tiering::audit_co_tests::cr_3_3_legacy_path_should_skip_truth_table.
source "$(dirname "$0")/../lib/common.sh"
source "$(dirname "$0")/../lib/dc.sh"

PREFIX=/release/106-cr33-$$/
PATHF="${MOUNT}${PREFIX}f.bin"; LOCAL=/tmp/106-src-$$.bin
APIF="${PATHF#$MOUNT}"
SAVED=0

restore_policy() {
    [ "$SAVED" = "1" ] || return 0
    mgr_post_leader /api/v1/volumes/$VOL/set-mandatory '{"hot_replicas":2,"warm_replicas":2,"cold_replicas":1}' >/dev/null 2>&1 || true
    mgr_post_leader /api/v1/volumes/$VOL/set-cache '{"warm_min":0,"warm_max":0,"per_dc":false}' >/dev/null 2>&1 || true
}
case_cleanup() {
    restore_policy
    client_run "sudo rm -rf ${MOUNT}${PREFIX}" >/dev/null 2>&1 || true
    rm -f "$LOCAL"
}
trap case_cleanup EXIT

CW=$(client_for_dc dc-a)
[ -n "$CW" ] || CW="$CLIENT"
[ -n "$CW" ] || skip "no client available"

echo "[1/5] active AV cache policy: mandatory cold=1 + warm cache (promote on read)"
mgr_post_leader /api/v1/volumes/$VOL/set-mandatory '{"hot_replicas":0,"warm_replicas":0,"cold_replicas":1}' >/dev/null; SAVED=1
mgr_post_leader /api/v1/volumes/$VOL/set-cache '{"warm_min":1,"warm_max":1,"per_dc":false,"promote_reads":2,"promote_window_secs":3600,"cooldown_secs":0}' >/dev/null

echo "[2/5] baseline audit cursor (avoid matching pre-existing rows)"
BASELINE=$(mgr_get_any '/api/v1/audit?limit=1' | python3 -c "
import json,sys
try:
    d=json.load(sys.stdin)
except Exception:
    d=[]
rows = d.get('entries', d) if isinstance(d, dict) else d
print((rows or [{}])[0].get('timestamp', 0))
" 2>/dev/null || echo 0)

echo "[3/5] write file, then read it several times to generate ReportChunkReads"
head -c $((4 * 1024 * 1024)) /dev/urandom > "$LOCAL"
scp "${SSH_OPTS[@]}" "$LOCAL" "debian@$CW:/tmp/106-src.bin" >/dev/null 2>&1 || skip "cannot stage src"
ssh_run "$CW" "sudo mkdir -p ${MOUNT}${PREFIX} && sudo cp /tmp/106-src.bin '$PATHF' && sudo sync" || fail "write failed"
ssh_run "$CW" "sudo rm -f /tmp/106-src.bin"
wait_for_soft 45 inspect_has_replicas "$APIF" || known_issue REL-1-2 "inspect lacks per-replica tier (REL-1-2)"
for _ in 1 2 3 4; do ssh_run "$CW" "sudo cat '$PATHF' > /dev/null"; sleep 2; done

echo "[4/5] give the leader a few heartbeat + tick cycles to process reads"
sleep 15

echo "[5/5] audit must carry NO legacy Bug BX-4 / Tape-migration rows for this file's chunks"
AUDIT=$(mgr_get_any '/api/v1/audit?limit=500')
BADROWS=$(echo "$AUDIT" | python3 -c "
import json,sys
try:
    d=json.load(sys.stdin)
except Exception:
    d=[]
rows = d.get('entries', d) if isinstance(d, dict) else d
baseline=int(sys.argv[2]) if sys.argv[2].isdigit() else 0
prefix=sys.argv[1]
bad=0
for r in (rows or []):
    ts=r.get('timestamp', 0)
    try:
        ts=int(ts)
    except Exception:
        ts=0
    if ts and baseline and ts <= baseline:
        continue
    action=str(r.get('action',''))
    result=str(r.get('result',''))
    if 'Bug BX-4' in action or 'Bug BX-4' in result:
        if prefix in result:
            bad += 1
    if 'tape' in result.lower() and prefix in result:
        bad += 1
print(bad)
" "$PREFIX" "$BASELINE" 2>/dev/null || echo 0)

if [ "${BADROWS:-0}" -gt 0 ]; then
    fail "CR-3-3 REGRESSION: legacy temperature path fired for an AV-cache-volume chunk ($BADROWS matching audit rows)"
fi
echo "  no legacy tiering rows for this file's chunks (AV cache engine owns it)"
pass

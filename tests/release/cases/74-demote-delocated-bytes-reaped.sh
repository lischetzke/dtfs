#!/usr/bin/env bash
# REL-3-67 (CO-1-3): the tiering demote dispatch de-locates a warm cache
# replica with a Raft-remove-FIRST ordering — RemoveChunkLocation (carrying
# the volume `enforce_anchor`) commits BEFORE the DeleteChunkReplica byte
# delete. If the byte-delete RPC then fails (or the manager crashes between
# the two), the chunk's location entry is already gone but the bytes linger
# on the demoted node's disk: a chunk "on a node's disk with no location
# entry naming that node". That leftover is reclaimed by the CO-3-2 orphan
# reaper after the durable-mtime grace (audits `orphan.reaped cause=delocated`).
#
# Pre-CO-1-3 the demote dispatch deleted the bytes FIRST and then committed
# the Raft remove; an RPC failure / crash in that window left the reverse
# hazard — bytes gone but the SM still advertising the replica as live, so a
# read could be routed to a node that no longer has the chunk.
#
# This case exercises the happy-path end-to-end (demote de-locates the warm
# copy via Raft-remove-first, then the reaper mops the on-disk bytes) which
# also re-covers CO-3-2. The distinctive CO-1-3 failure-path semantics (Raft
# remove commits, byte delete fails -> reaper reclaims; NEVER the reverse) are
# not reliably provokable in a live run (we cannot force a mid-demote RPC
# failure), so they are pinned by the scheduler/tiering unit tests
# (`co_1_3_delocate_order_tests`, `co_1_3_demote_removes_before_byte_delete_*`);
# where the topology can't build the cache copy we report KNOWN_ISSUE CO-1-3.
#
# Flow:
#   1. Short orphan grace + scrub interval on every manager (so the reap is
#      observable in a release run); SKIP if not configurable.
#   2. Anchor cold=1 (dc-a) + short-window warm cache so idle-demote fires.
#   3. Write a file -> cold anchor lands; read it repeatedly -> a warm cache
#      copy promotes on some node. Record that node's host + confirm it holds
#      the chunk bytes on disk.
#   4. Go idle past promote_window -> the engine demotes the warm copy. Assert
#      the warm SM location entry disappears (de-located, Raft-remove-first).
#   5. Assert the de-located on-disk bytes on the demoted host are reaped after
#      grace+scrub, and the cold anchor + file survive (no over-reap).
# Cleanup restores the manager grace config + volume policy + removes seeds.
#
# NOT in QUICK_CASES: a rolling manager restart + idle-demote + grace/scrub
# wait pushes the runtime well past the quick budget.
source "$(dirname "$0")/../lib/common.sh"
source "$(dirname "$0")/../lib/dc.sh"

MGR_TOML=/etc/dtfs/manager.toml
BAK=$MGR_TOML.case67.bak
GRACE=30          # orphan grace (s) — short so the reap is observable
SCRUB_INT=60      # orphan scrub interval (s); run_orphan_scrub floors at 60

PREFIX=/release/67-co13-$$/
PATHF="${MOUNT}${PREFIX}f.bin"
LOCAL=/tmp/67-src-$$.bin

audit_json() { mgr_get_any /api/v1/audit; }
has_delocated_reap() {  # any orphan.reaped with cause=delocated
    audit_json | python3 -c "
import json,sys
try:
    d=json.load(sys.stdin)
except Exception:
    sys.exit(1)
rows=d if isinstance(d,list) else d.get('entries',d.get('events',[]))
for e in rows:
    blob=json.dumps(e)
    if 'orphan.reaped' in blob and 'cause=delocated' in blob:
        sys.exit(0)
sys.exit(1)
"
}

restart_manager() {  # <host>
    ssh_run "$1" 'sudo systemctl restart dtfs-manager' || return 1
    wait_for 60 "manager $1 API back" \
        curl "${CURL_OPTS[@]}" -o /dev/null "http://$1:$MGR_PORT/api/v1/health"
}

CONFIGS_TOUCHED=0
POLICY_SAVED=0
restore_policy() {
    [ "$POLICY_SAVED" = "1" ] || return 0
    mgr_post_leader /api/v1/volumes/$VOL/set-mandatory '{"mandatory_replicas":{"hot":2,"warm":2,"cold":1}}' >/dev/null 2>&1 || true
    mgr_post_leader /api/v1/volumes/$VOL/set-cache '{"warm_min":0,"warm_max":0,"per_dc":false}' >/dev/null 2>&1 || true
}
case_cleanup() {
    restore_policy
    if [ "$CONFIGS_TOUCHED" = "1" ]; then
        for h in $(mgrs_list); do
            ssh_run "$h" "if sudo test -f $BAK; then \
                sudo mv $BAK $MGR_TOML && sudo systemctl restart dtfs-manager; fi" \
                >/dev/null 2>&1 || true
        done
        wait_for_soft 90 cluster_api_up || true
    fi
    client_run "sudo rm -rf ${MOUNT}${PREFIX}" >/dev/null 2>&1 || true
    rm -f "$LOCAL"
}
trap case_cleanup EXIT

# ── Prereqs ──────────────────────────────────────────────────────
nodes_json=$(mgr_get_any /api/v1/nodes) || skip "no manager reachable"
echo "$nodes_json" | grep -q '"role":"Data"' || skip "no data nodes registered"
for h in $(mgrs_list); do
    ssh_run "$h" "sudo test -f $MGR_TOML" \
        || skip "$MGR_TOML not found on $h (cannot set short orphan grace)"
done
CW=$(client_for_dc dc-a); [ -n "$CW" ] || CW="$CLIENT"
[ -n "$CW" ] || skip "no client available"

# ── 1. Short orphan grace + scrub interval on every manager ──────
echo "[1/5] set scrub.orphan_grace_secs=$GRACE / orphan_interval_secs=$SCRUB_INT"
CONFIGS_TOUCHED=1
LEADER=$(mgr_leader) || LEADER=""
for h in $(mgrs_list); do
    ssh_run "$h" "{ sudo test -f $BAK || sudo cp $MGR_TOML $BAK; } && \
        sudo sed -i '/^orphan_grace_secs/d;/^orphan_interval_secs/d' $MGR_TOML && \
        if ! sudo grep -q '^\[scrub\]' $MGR_TOML; then printf '\n[scrub]\n' | sudo tee -a $MGR_TOML >/dev/null; fi && \
        sudo sed -i -e '/^\[scrub\]/a orphan_grace_secs = $GRACE' \
                    -e '/^\[scrub\]/a orphan_interval_secs = $SCRUB_INT' $MGR_TOML && \
        sudo grep -q '^orphan_grace_secs' $MGR_TOML" \
        || skip "could not set short orphan grace on $h (config schema mismatch)"
done
for h in $(mgrs_list); do
    [ "$h" = "$LEADER" ] && continue
    restart_manager "$h" || fail "manager $h did not come back after restart"
done
[ -n "$LEADER" ] && { restart_manager "$LEADER" || fail "leader $LEADER did not come back"; }
wait_for 90 "raft re-elected a leader" mgr_leader

# ── 2. Anchor cold=1 (dc-a) + short-window warm cache ────────────
echo "[2/5] anchor cold=1 (dc-a) + short-window warm cache so idle-demote fires fast"
mgr_post_leader /api/v1/volumes/$VOL/set-anchor '{"datacenter":"dc-a","tier":"cold","mandatory_replicas":1}' >/dev/null
mgr_post_leader /api/v1/volumes/$VOL/set-mandatory '{"mandatory_replicas":{"hot":0,"warm":0,"cold":1}}' >/dev/null; POLICY_SAVED=1
mgr_post_leader /api/v1/volumes/$VOL/set-cache '{"warm_min":1,"warm_max":1,"per_dc":false,"promote_reads":1,"promote_window_secs":30,"cooldown_secs":0}' >/dev/null

# ── 3. Write + promote a warm cache copy; record its host ────────
echo "[3/5] write file -> cold anchor; read repeatedly -> promote a warm cache copy"
head -c $((8 * 1024 * 1024)) /dev/urandom > "$LOCAL"
scp "${SSH_OPTS[@]}" "$LOCAL" "debian@$CW:/tmp/67-src.bin" >/dev/null 2>&1 || skip "cannot stage src"
ssh_run "$CW" "sudo mkdir -p ${MOUNT}${PREFIX} && sudo cp /tmp/67-src.bin '$PATHF' && sudo sync" || fail "write failed"
ssh_run "$CW" "sudo rm -f /tmp/67-src.bin"
inspect_has_replicas "$PATHF" || known_issue REL-1-2 "inspect lacks per-replica tier (REL-1-2)"
_anchor_ok() { [ "$(count_replicas_in_dc_tier "$PATHF" dc-a cold)" -ge 1 ]; }
wait_for 60 "cold anchor present" _anchor_ok

FIRST_CID=$(chunk_ids_for_path "$PATHF" | head -1)
[ -n "$FIRST_CID" ] || skip "could not read chunk ids for $PATHF"
echo "  first chunk: $FIRST_CID"

for _ in 1 2 3; do ssh_run "$CW" "sudo cat '$PATHF' > /dev/null"; sleep 2; done
_warm_present() { [ "$(count_replicas_in_tier "$PATHF" warm)" -ge 1 ]; }
if ! wait_for_soft 90 _warm_present; then
    known_issue CO-1-3 "warm cache copy did not promote within 90s; cannot exercise demote de-location"
fi

# Identify the node_id holding the warm copy, map it to a host, confirm bytes.
WARM_ID=$(mgr_get_any "/api/v1/files/inspect?path=$PATHF" | python3 -c "
import json,sys
d=json.load(sys.stdin)
for c in d.get('chunks',[]):
    if c.get('chunk_id')=='$FIRST_CID' and str(c.get('tier','')).lower()=='warm':
        print(c.get('node_id','')); break
")
[ -n "$WARM_ID" ] || known_issue CO-1-3 "could not resolve the warm replica node_id (inspect shape)"
WARM_HOST=$(echo "$nodes_json" | python3 -c "
import json,sys
for n in json.load(sys.stdin):
    if str(n.get('node_id'))=='$WARM_ID':
        print(n.get('hostname','')); break
")
[ -n "$WARM_HOST" ] || known_issue CO-1-3 "could not map warm node_id $WARM_ID to a host"
if ! wait_for_soft 60 host_has_chunk "$WARM_HOST" "$FIRST_CID"; then
    known_issue CO-1-3 "warm host $WARM_HOST never showed the chunk bytes on disk"
fi
echo "  warm cache copy on $WARM_HOST (node $WARM_ID) holds $FIRST_CID on disk"

# ── 4. Go idle -> engine demotes; assert the warm SM entry de-locates ──
echo "[4/5] go idle past the 30s promote_window -> engine demotes (Raft-remove-first de-location)"
sleep 50
warm_delocated() {  # SM no longer names the warm node for this chunk
    mgr_get_any "/api/v1/files/inspect?path=$PATHF" | python3 -c "
import json,sys
d=json.load(sys.stdin)
for c in d.get('chunks',[]):
    if str(c.get('node_id'))=='$WARM_ID' and c.get('chunk_id')=='$FIRST_CID':
        sys.exit(1)   # still named -> not yet de-located
sys.exit(0)
"
}
wait_for 120 "idle warm cache de-located from the SM" warm_delocated
echo "  warm location entry de-located from the SM (RemoveChunkLocation committed)"

# ── 5. De-located bytes reaped; anchor + file survive ────────────
echo "[5/5] de-located on-disk bytes on $WARM_HOST must be reaped after grace+scrub"
warm_bytes_reaped() { ! host_has_chunk "$WARM_HOST" "$FIRST_CID"; }
# grace=30s + one 60s scrub interval + 30s scrub warmup + slack.
if ! wait_for 240 "de-located bytes reaped from $WARM_HOST" warm_bytes_reaped; then
    fail "phantom on-disk replica on $WARM_HOST was NOT reaped after demote de-location"
fi
echo "  $WARM_HOST no longer holds $FIRST_CID on disk"
wait_for_soft 30 has_delocated_reap \
    && echo "  observed orphan.reaped cause=delocated audit" \
    || echo "  NOTE: no cause=delocated audit row surfaced (audit feed may be truncated)"

# The cold anchor MUST survive every demote tick, and the file stays readable.
[ "$(count_replicas_in_dc_tier "$PATHF" dc-a cold)" -ge 1 ] \
    || fail "CO-1-3/anchor REGRESSION: cold anchor stranded by demote de-location"
ssh_run "$CW" "sudo cat '$PATHF' > /dev/null" \
    || fail "file unreadable after demote de-location reap (over-aggressive reap or stranded anchor)"
echo "  cold anchor + file survived demote de-location + reap"
pass

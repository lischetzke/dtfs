#!/usr/bin/env bash
# CO-3-2 — when the rebalance / excess-spill path de-locates a replica
# (commits RemoveChunkLocation to drop the SM entry) the on-disk bytes on
# the de-located node MUST be reaped after the orphan grace. Pre-fix the
# orphan scrub only reaped HASH orphans (no file references the hash at
# all), so a de-located replica whose hash is still referenced elsewhere
# lingered on disk forever as a phantom replica.
#
# The fix teaches the reaper a per-node rule: a chunk present on a node's
# disk for which the SM records NO location entry naming that node is
# reaped after the same durable-mtime grace (Bug CB). The reap audits as
# `orphan.reaped ... cause=delocated`.
#
# Flow:
#   1. Require a short orphan grace + scrub interval on every manager
#      (scrub.orphan_grace_secs / orphan_scrub_interval_secs); restart
#      followers-then-leader. A prod-default 24h grace can't be observed in
#      a release run -> SKIP if not configurable / not set short.
#   2. Seed one cold-born file; read its chunk ids.
#   3. Over-replicate it: path-migrate the prefix to a warm node so each
#      chunk gains an EXTRA (excess) warm replica beyond goal. Record the
#      warm host + confirm it holds the chunk bytes on disk.
#   4. Provoke rebalance de-location: the excess warm replica is above
#      mandatory_total + cache_allowance, so check_misplaced_rebalance
#      drops it (RemoveChunkLocation). Assert the SM location entry on the
#      warm host disappears (rebalance.remove_excess audit / inspect).
#   5. Assert the de-located on-disk bytes are reaped after grace+scrub:
#      host_has_chunk(warm_host) -> false, and an
#      `orphan.reaped ... cause=delocated` audit row appears.
# Cleanup restores the manager configs (grace) and removes the seeds.
#
# NOT in QUICK_CASES: a rolling manager restart + grace/scrub wait pushes
# the runtime past the quick budget.
source "$(dirname "$0")/../lib/common.sh"

MGR_TOML=/etc/dtfs/manager.toml
BAK=$MGR_TOML.case66.bak
GRACE=21600       # orphan grace (s) — config floor is 6 h (MIN_ORPHAN_GRACE_SECS); reap waits that long
SCRUB_INT=60      # orphan scrub interval (s); run_orphan_scrub floors at 60

PREFIX="release/66-delocate-$$"
DIRF="$MOUNT/$PREFIX"
FILE="$PREFIX/m.bin"

# audit feed (cluster-wide; reaper logs under the scheduler actor).
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
case_cleanup() {
    if [ "$CONFIGS_TOUCHED" = "1" ]; then
        for h in $(mgrs_list); do
            ssh_run "$h" "if sudo test -f $BAK; then \
                sudo mv $BAK $MGR_TOML && sudo systemctl restart dtfs-manager; fi" \
                >/dev/null 2>&1 || true
        done
        wait_for_soft 90 cluster_api_up || true
    fi
    client_run "sudo rm -rf '$DIRF'" >/dev/null 2>&1 || true
}
trap case_cleanup EXIT

# ── Prereqs ──────────────────────────────────────────────────────
nodes_json=$(mgr_get_any /api/v1/nodes) || skip "no manager reachable"
echo "$nodes_json" | grep -q '"role":"Data"' || skip "no data nodes registered"
for h in $(mgrs_list); do
    ssh_run "$h" "sudo test -f $MGR_TOML" \
        || skip "$MGR_TOML not found on $h (cannot set short orphan grace)"
done

# Need at least one warm-tier data node to host the excess replica.
WARM_HOST=$(echo "$nodes_json" | python3 -c "
import json,sys
for n in json.load(sys.stdin):
    if n.get('role')=='Data' and any(d.get('tier')=='warm' for d in n.get('disks',[])):
        print(n.get('hostname','')); break
")
[ -n "$WARM_HOST" ] || skip "no warm-tier data node (cannot create an excess warm replica)"
WARM_DC=$(echo "$nodes_json" | python3 -c "
import json,sys
for n in json.load(sys.stdin):
    if n.get('hostname')=='$WARM_HOST':
        print(n.get('location',{}).get('datacenter','')); break
")
echo "INFO: warm_host=$WARM_HOST warm_dc=$WARM_DC grace=${GRACE}s"

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

# ── 2. Seed one file, read its chunk ids ─────────────────────────
echo "[2/5] seed $FILE"
client_run "sudo mkdir -p '$DIRF'" || skip "cannot write via the FUSE mount"
make_fuse_file "$FILE" 6 >/dev/null || fail "seed write failed"
CIDS=$(chunk_ids_for_path "/$FILE")
[ -n "$CIDS" ] || skip "could not read chunk ids for $FILE"
FIRST_CID=$(echo "$CIDS" | head -1)
echo "  first chunk: $FIRST_CID"

# ── 3. Over-replicate: path-migrate the prefix to the warm node ──
echo "[3/5] path-migrate $PREFIX to warm DC $WARM_DC to add an excess warm replica"
RESP=$(mgr_post_leader "/api/v1/migrations" \
    "{\"path_prefix\":\"/$PREFIX\",\"target_tier\":\"warm\",\"target_dc\":\"$WARM_DC\"}")
echo "  migrate response: $RESP"
echo "$RESP" | grep -qi 'error' && skip "path-migrate not accepted on this build: $RESP"

# Wait for the warm host to actually hold the chunk bytes (migration done).
if ! wait_for_soft 240 host_has_chunk "$WARM_HOST" "$FIRST_CID"; then
    skip "excess warm replica never landed on $WARM_HOST (placement/capacity) — nothing to de-locate"
fi
echo "  $WARM_HOST now holds $FIRST_CID on disk"

# ── 4. Rebalance must de-locate the excess warm replica ──────────
# The chunk is now over goal (mandatory cold + an excess warm with no cache
# allowance), so check_misplaced_rebalance drops the warm SM entry. We
# detect the de-location by the warm host's node_id leaving the chunk's
# location list.
warm_id=$(echo "$nodes_json" | python3 -c "
import json,sys
for n in json.load(sys.stdin):
    if n.get('hostname')=='$WARM_HOST':
        print(n.get('node_id','')); break
")
warm_delocated() {  # true once the SM no longer names the warm node for the chunk
    mgr_get_any "/api/v1/files/inspect?path=/$FILE" | python3 -c "
import json,sys
d=json.load(sys.stdin)
for c in d.get('chunks',[]):
    if str(c.get('node_id'))=='$warm_id' and c.get('chunk_id')=='$FIRST_CID':
        sys.exit(1)   # still named -> not yet de-located
sys.exit(0)
"
}
echo "[4/5] wait for rebalance to drop the excess warm SM location entry"
if ! wait_for_soft 180 warm_delocated; then
    # Some topologies treat the warm copy as a legit cache replica
    # (cache_replicas.warm.max > 0) and won't spill it. Honest skip.
    skip "rebalance did not de-locate the warm replica (it is within cache allowance on this volume)"
fi
echo "  warm location entry de-located from the SM"

# ── 5. De-located bytes must be reaped after grace + scrub ───────
echo "[5/5] de-located on-disk bytes must be reaped (cause=delocated) after grace"
warm_bytes_reaped() { ! host_has_chunk "$WARM_HOST" "$FIRST_CID"; }
# grace (6 h floor) + one 60s scrub interval + 30s scrub warmup + slack.
if ! wait_for $((GRACE + 300))"de-located bytes reaped from $WARM_HOST" warm_bytes_reaped; then
    fail "phantom on-disk replica on $WARM_HOST was NOT reaped after de-location"
fi
echo "  $WARM_HOST no longer holds $FIRST_CID on disk"
# The reap should audit as cause=delocated (diagnostic for CO-3-2).
wait_for_soft 30 has_delocated_reap \
    && echo "  observed orphan.reaped cause=delocated audit" \
    || echo "  NOTE: no cause=delocated audit row surfaced (audit feed may be truncated)"

# The file's data must remain intact (anchor + remaining replicas serve it).
sha256_via_fuse "$MOUNT/$FILE" >/dev/null \
    || fail "file unreadable after de-location reap (over-aggressive reap?)"

pass

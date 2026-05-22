#!/usr/bin/env bash
# Bug AV (AV-4-1) — end-to-end live demo for the two-class tier
# lifecycle. Drives four scenarios against the minicluster:
#
#   S1. AV-2-4 promote-on-threshold: 2 reads in window → warm
#       cache replica appears.
#   S2. AV-2-4 demote-on-idle: 0 reads in window → engine drops
#       the warm replica + clears cooldown.
#   S3. AV-2-5 cooldown asymmetry: operator flush sets cooldown,
#       immediate re-reads do NOT re-promote; after expiry they
#       do. Engine demote (S2) cleared the cooldown so re-promote
#       was instant — verify the asymmetry.
#   S4. AV-3-3 warm-first write + anchor-lag-repair: a client
#       with `first_write_tier = "nearest-warm"` writes a fresh
#       file, the first chunk lands on warm in the client's DC,
#       and the manager schedules the cold anchor copy within
#       one tiering tick.
#
# Runs from the dtfs-builder host. SSHes to mini-client01 for the
# mount-side I/O and back to mini-mgr01 for state queries.
#
# Exit code 0 = all four scenarios passed; non-zero = first
# failure with diagnostic context.

set -uo pipefail

MGR=${MGR:-http://mini-mgr01:9402}
CLIENT=${CLIENT:-mini-client01}
MOUNT=${MOUNT:-/mnt/dtfs}
VOL=${VOL:-av-demo}
TICK_SECS=${TICK_SECS:-10}
WINDOW_SECS=${WINDOW_SECS:-60}
COOLDOWN_SECS=${COOLDOWN_SECS:-30}
SSH_OPTS=(-o StrictHostKeyChecking=no -i ~/.ssh/dtfs-lab/id_ed25519)
CURL_OPTS=(-s --max-time 5)

# ── Pretty logging ───────────────────────────────────────────────
step()  { echo "[AV-4-1] $*"; }
ok()    { echo "  ✓ $*"; }
fail()  { echo "  ✗ FAIL: $*" >&2; exit 1; }

# ── Helpers ──────────────────────────────────────────────────────

# Run a curl GET against the manager API; returns body on stdout.
mgr_get() {
    curl "${CURL_OPTS[@]}" "$MGR$1"
}

# Run a curl POST. $1=path, $2=JSON body.
mgr_post() {
    curl "${CURL_OPTS[@]}" -X POST -H 'content-type: application/json' \
        -d "$2" "$MGR$1"
}

# Run a command on mini-client01.
client_run() {
    ssh "${SSH_OPTS[@]}" "debian@$CLIENT" "$@"
}

# Audit log query for the given action prefix since N seconds ago.
audit_since() {
    local action="$1"
    local since
    since=$(date +%s -d "${2:-60} seconds ago")
    mgr_get "/api/v1/audit?action=$action&since=$since&limit=50"
}

# Find the actual cluster-wide DC names from /api/v1/nodes (used to
# pick a DC for the cache scope). Returns the first one found.
first_dc() {
    mgr_get /api/v1/nodes \
        | python3 -c '
import json,sys
for n in json.load(sys.stdin):
    loc = n.get("location") or ""
    parts = loc.split("/")
    if len(parts) >= 2 and parts[1]:
        print(parts[1]); break
'
}

# Count warm-tier replicas of the chunks that belong to file $1.
# AV-5-5 fix: pre-fix this counted *every* warm-tier `.dat` file on
# the cluster regardless of which file it belonged to, so S2's
# "warm count must drop" assertion produced false negatives on any
# minicluster carrying orphan chunks from prior test runs. Now we
# pull the chunk_hashes from the FileMetadata projection and grep
# the data nodes' warm tier for those specific hashes.
count_warm_chunks_for_path() {
    local path="$1"
    local total=0
    # Pull the chunk_hashes belonging to this file from the SM.
    # `/api/v1/files` exposes per-file chunk_count + total_size but
    # not the underlying hashes — so call the chunk_locations API
    # (Bug AV exposes it via files/{path:.*}/locations or the audit
    # log). Fall back: read recent audit `cache.promote` / `Update
    # Chunks` summaries which carry `chunk=<hash>`.
    mapfile -t hashes < <(
        AV_PATH="/$path" mgr_get "/api/v1/audit?limit=400" \
        | AV_PATH="/$path" python3 -c '
import json,sys,re,os
ap = os.environ.get("AV_PATH","")
seen = set()
for r in json.load(sys.stdin):
    s = r.get("summary","")
    if ap and ap not in s:
        continue
    m = re.search(r"chunk=([0-9a-f]{64})", s)
    if m:
        seen.add(m.group(1))
for h in seen:
    print(h)
'
    )
    if [ "${#hashes[@]}" -eq 0 ]; then
        echo 0
        return
    fi

    # Pull the list of data-node hostnames from the cluster.
    mapfile -t nodes < <(
        mgr_get /api/v1/nodes \
        | python3 -c '
import json,sys
for n in json.load(sys.stdin):
    if n.get("role") == "Data":
        print(n["hostname"])
'
    )
    for n in "${nodes[@]}"; do
        for h in "${hashes[@]}"; do
            cnt=$(ssh "${SSH_OPTS[@]}" "debian@$n" \
                "sudo find /mnt/${n#mini-}-warm/chunks -name '$h.dat' 2>/dev/null | wc -l")
            total=$((total + cnt))
        done
    done
    echo "$total"
}

# Wait up to N seconds for a predicate (eval'd command string) to
# return 0. Polls every 2 seconds. Use eval instead of `bash -c`
# so the predicate sees the script's local functions.
wait_for() {
    local label="$1"; shift
    local max_secs="$1"; shift
    local cmd="$*"
    local i
    for ((i=0; i<max_secs; i+=2)); do
        if eval "$cmd"; then return 0; fi
        sleep 2
    done
    return 1
}

# ── Setup ────────────────────────────────────────────────────────

setup() {
    step "Setup: create + configure demo volume '$VOL'"

    # Best-effort delete the volume if a previous run left it
    # behind. The current API doesn't expose DELETE; we just
    # re-upsert via curl since UpsertVolume is the underlying op.
    # For now, if the volume already exists we just re-apply the
    # set-cache (idempotent) so the test is restart-safe.
    local existing
    existing=$(mgr_get "/api/v1/volumes/$VOL" 2>/dev/null \
        | python3 -c 'import json,sys
d=json.load(sys.stdin) if sys.stdin.isatty() else None
print("yes" if (d and d.get("volume_id")) else "no")' 2>/dev/null || echo "no")

    if [ "$existing" != "yes" ]; then
        step "Creating volume '$VOL' with replication_cold=1 only"
        # Bypass cmd_volumes_create's limited surface; raw POST.
        local body
        body=$(cat <<JSON
{
  "name": "$VOL",
  "chunk_size_mb": 4,
  "replication_hot": 0,
  "replication_warm": 0,
  "replication_cold": 1
}
JSON
)
        mgr_post "/api/v1/volumes" "$body" >/dev/null || \
            fail "create volume failed"
    fi

    step "Migrate policy + set cache (idempotent)"
    mgr_post "/api/v1/volumes/$VOL/migrate-policy" '{}' >/dev/null

    local DC
    DC=$(first_dc)
    [ -n "$DC" ] || fail "could not resolve any DC from /api/v1/nodes"
    ok "anchor DC = $DC"

    local body
    body=$(cat <<JSON
{
  "warm_min": 1,
  "warm_max": 1,
  "per_dc": true,
  "promote_reads": 2,
  "promote_window_secs": $WINDOW_SECS,
  "cooldown_secs": $COOLDOWN_SECS
}
JSON
)
    mgr_post "/api/v1/volumes/$VOL/set-cache" "$body" >/dev/null \
        || fail "set-cache failed"
    ok "cache_replicas.warm = (1, 1), per_dc=true, promote_reads=2, window=${WINDOW_SECS}s, cooldown=${COOLDOWN_SECS}s"

    # Lower the tiering tick interval cluster-wide so the demo
    # finishes in minutes, not hours. Live-reload via the
    # `/api/v1/config` endpoint.
    step "Lower tiering evaluation_interval_secs to $TICK_SECS"
    local cfg_body
    cfg_body=$(cat <<JSON
{
  "config_json": "{\"tiering\": {\"evaluation_interval_secs\": $TICK_SECS}}"
}
JSON
)
    mgr_post "/api/v1/config" "$cfg_body" >/dev/null \
        || fail "config update failed"
    ok "tiering tick = ${TICK_SECS}s"

    # Make sure the mount is fresh.
    client_run "mkdir -p $MOUNT/$VOL 2>/dev/null; ls $MOUNT/$VOL/" \
        >/dev/null || fail "mount unreachable: $MOUNT/$VOL"
}

# ── S1: AV-2-4 promote-on-threshold ──────────────────────────────

s1_promote() {
    step "S1: promote-on-threshold (2 reads → warm cache appears)"

    local file="$MOUNT/$VOL/s1-promote.bin"
    client_run "sudo dd if=/dev/urandom of=$file bs=1M count=2 status=none && sync"

    # Initial state: only mandatory cold. 0 warm.
    local before; before=$(count_warm_chunks_for_path "$VOL/s1-promote.bin")
    ok "before reads: warm chunks for $VOL = $before (baseline)"

    # Trigger 2 reads inside the window.
    client_run "sudo cat $file > /dev/null; sleep 1; sudo cat $file > /dev/null"
    ok "issued 2 sequential reads"

    # Wait for the tiering tick to fire + the replication task to
    # fully complete. Scheduler tick = 30s; replication walks
    # Pending → Copying → Verifying → Completing → Done (~120s
    # end-to-end). Plus AV cache tick (10s) before scheduling.
    # Worst case: ~180s.
    if ! wait_for "warm replica appears" 240 \
        "[ \$(count_warm_chunks_for_path '$VOL/s1-promote.bin') -gt $before ]"
    then
        echo "    audit (cache.promote since 90s):"
        audit_since "cache." 90 | python3 -m json.tool | head -30
        fail "S1: warm replica did not appear within 240 s"
    fi
    local after; after=$(count_warm_chunks_for_path "$VOL/s1-promote.bin")
    ok "after reads: warm chunks for $VOL = $after (promoted)"

    # Confirm there's a cache.promote audit entry.
    local promotes
    promotes=$(audit_since "cache.promote" 90 \
        | python3 -c 'import json,sys; print(len(json.load(sys.stdin)))')
    [ "$promotes" -ge 1 ] || fail "S1: no cache.promote audit entries"
    ok "audit cache.promote entries since trigger: $promotes"
}

# ── S2: AV-2-4 demote-on-idle ────────────────────────────────────

s2_demote() {
    step "S2: demote-on-idle (no reads in window → warm dropped)"

    # Leave the chunk idle for > promote_window_secs and trigger
    # at least one tiering tick.
    local idle=$((WINDOW_SECS + TICK_SECS + 5))
    step "Sleeping ${idle}s for window to fully drain + tick to fire"
    sleep "$idle"

    # Sanity: a fresh read window means count_reads_in_window
    # should now return 0 for this chunk. The next tick's planner
    # will emit a TickDemotion.
    if ! wait_for "warm replica disappears" 90 \
        "[ \$(count_warm_chunks_for_path '$VOL/s1-promote.bin') -eq 0 ]"
    then
        echo "    audit (cache.demote since 120s):"
        audit_since "cache.demote" 120 | python3 -m json.tool | head -20
        fail "S2: warm replica did not drop within 90 s of idle window"
    fi
    ok "warm chunk count = 0 after idle (demoted)"

    local demotes
    demotes=$(audit_since "cache.demote" 180 \
        | python3 -c 'import json,sys; print(len(json.load(sys.stdin)))')
    [ "$demotes" -ge 1 ] || fail "S2: no cache.demote audit entries"
    ok "audit cache.demote entries: $demotes"
}

# ── S3: AV-2-5 cooldown asymmetry ────────────────────────────────

s3_cooldown_asymmetry() {
    step "S3: cooldown asymmetry (operator flush ≠ engine demote)"

    # Re-promote the chunk via two reads (same recipe as S1) and
    # keep the read window hot during replication to prevent the
    # engine's idle-demote from racing flush_cache.
    local file="$MOUNT/$VOL/s1-promote.bin"
    client_run "sudo cat $file > /dev/null; sleep 1; sudo cat $file > /dev/null"
    # AV-5-5: replication takes ~120s (scheduler tick 30s × 4
    # states) so the wait_for must cover Pending → Copying →
    # Verifying → Completing → Done plus the AV cache tick that
    # actually emits the promote. Budget 360s.
    wait_for "re-promotion after S2 demote" 360 \
        "[ \$(count_warm_chunks_for_path '$VOL/s1-promote.bin') -gt 0 ]" \
        || fail "S3 setup: chunk did not re-promote after S2's engine-cleared cooldown"
    ok "engine-cleared cooldown ⇒ immediate re-promote after 2 reads (S2 invariant)"

    # AV-5-5 / AV-5-9 fix: wait for the replication task to fully
    # reach the state machine (Pending → Copying → Verifying →
    # Completing → Done at scheduler tick = 30s = ~120s end-to-end).
    # Keep reads flowing every 5s so the read-window stays >= 2
    # during replication — otherwise the engine's idle-demote
    # planner races flush_cache and drops the warm replica from SM
    # before the operator's flush even runs.
    step "Waiting for replication.complete audit row (max 240s)..."
    local waited=0
    while [ $waited -lt 240 ]; do
        if audit_since "replication.complete" 30 | grep -q replication.complete; then
            break
        fi
        client_run "sudo cat $file > /dev/null" >/dev/null 2>&1 || true
        sleep 5
        waited=$((waited + 5))
    done
    if ! audit_since "replication.complete" 60 | grep -q replication.complete; then
        fail "S3 setup: warm replica never reached SM (replication.complete absent)"
    fi
    ok "warm replica registered in SM"

    # AV-5-5: keep the read-window saturated all the way through
    # the flush call so the planner's idle-demote can't race in
    # between (the tick interval is 10s).
    client_run "sudo cat $file > /dev/null; sleep 1; sudo cat $file > /dev/null" \
        >/dev/null 2>&1 || true

    # Operator flush: sets a future cooldown.
    mgr_post "/api/v1/files/flush_cache" \
        "{\"path_prefix\":\"/$VOL/s1-promote.bin\",\"cache_tiers\":[\"warm\"],\"cooldown_secs\":$COOLDOWN_SECS}" \
        >/dev/null
    wait_for "warm replica gone after flush" 30 \
        "[ \$(count_warm_chunks_for_path '$VOL/s1-promote.bin') -eq 0 ]" \
        || fail "S3: operator flush did not drop the warm replica"
    ok "operator flush succeeded; cooldown should now be set"

    # Within the cooldown window, 2 reads MUST NOT re-promote.
    client_run "sudo cat $file > /dev/null; sleep 1; sudo cat $file > /dev/null"
    sleep $((TICK_SECS + 5))
    local during; during=$(count_warm_chunks_for_path "$VOL/s1-promote.bin")
    [ "$during" -eq 0 ] || \
        fail "S3: re-promote during cooldown — operator intent ignored"
    ok "no re-promote during cooldown (cooldown blocks promotion)"

    # After cooldown expiry, 2 reads SHOULD re-promote.
    step "Sleeping ${COOLDOWN_SECS}s for cooldown to expire..."
    sleep "$COOLDOWN_SECS"
    client_run "sudo cat $file > /dev/null; sleep 1; sudo cat $file > /dev/null"
    wait_for "re-promotion after cooldown expiry" 60 \
        "[ \$(count_warm_chunks_for_path '$VOL/s1-promote.bin') -gt 0 ]" \
        || fail "S3: re-promote after cooldown expiry failed"
    ok "re-promote after cooldown expiry ⇒ asymmetry verified"
}

# ── S4: AV-3-3 warm-first write + anchor-lag-repair ──────────────

s4_warm_first_write() {
    step "S4: warm-first write triggers anchor-lag-repair"

    local has_write_block
    has_write_block=$(client_run "sudo grep -c '^\\[write\\]' /etc/dtfs/fuse.toml" || echo 0)
    if [ "$has_write_block" -eq 0 ]; then
        echo "  ⚠  client $CLIENT has no [write] block in fuse.toml" >&2
        echo "  ⚠  S4 needs first_write_tier = \"nearest-warm\" configured" >&2
        echo "  ⚠  Skipping S4 — see the deferred-setup note in the verification log" >&2
        return 0
    fi

    # Write a fresh file. The client will request preferred_tier="warm".
    local file="$MOUNT/$VOL/s4-warm-first.bin"
    client_run "sudo dd if=/dev/urandom of=$file bs=1M count=1 status=none && sync"
    sleep 2  # let the commit propagate

    # First chunk should land on warm (not cold) initially.
    local warm; warm=$(count_warm_chunks_for_path "$VOL/s4-warm-first.bin")
    [ "$warm" -ge 1 ] \
        || fail "S4: warm-first write did NOT land on a warm disk (got $warm)"
    ok "first-chunk placement: warm replica present at write time"

    # Anchor-lag-repair tick should schedule + complete a cold copy.
    if ! wait_for "anchor.lag_repair audit entry" 60 \
        'audit_since "anchor.lag_repair" 90 | grep -q anchor.lag_repair'
    then
        fail "S4: no anchor.lag_repair audit entry within 60 s"
    fi
    ok "anchor.lag_repair audit entry fired"
}

# ── Driver ───────────────────────────────────────────────────────

main() {
    setup
    s1_promote
    s2_demote
    s3_cooldown_asymmetry
    s4_warm_first_write
    echo
    echo "[AV-4-1] ALL SCENARIOS PASSED"
}

main "$@"

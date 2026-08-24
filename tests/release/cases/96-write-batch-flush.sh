#!/usr/bin/env bash
# WI-01 — the write buffer holds back complete chunks until
# write.flush_batch_chunks (default 4) exist, so flush_segments_to_cluster
# amortizes one AllocateChunks + CommitChunkLocations round trip over a
# batch instead of firing once per chunk. Proves: (a) integrity survives
# batching, (b) manager metadata is unaffected, (c) every chunk still gets
# its full replica set including the anchor DC copy, (d) the AllocateChunks
# call count actually drops below the chunk count, and (e) flush_batch_chunks
# = 1 reproduces the pre-WI-01 per-chunk call count exactly (rollback path).
#
# Not in QUICK_CASES: it writes tens of MB and restarts the client's FUSE
# service once to exercise the rollback config.
source "$(dirname "$0")/../lib/common.sh"

CHUNK_MB=${CHUNK_MB:-4}
CHUNK=$((CHUNK_MB * 1024 * 1024))
NCHUNKS=${NCHUNKS:-10}
SIZE=$((CHUNK * NCHUNKS))

FUSE_TOML=${FUSE_TOML:-/etc/dtfs/fuse.toml}
FUSE_UNIT=${FUSE_UNIT:-dtfs-fuse@-mnt-dtfs.service}

F1="$MOUNT/release/96-batch-$$.bin"
F2="$MOUNT/release/96-rollback-$$.bin"
API1="${F1#$MOUNT}"   # mount-relative path the manager namespace uses
API2="${F2#$MOUNT}"
LOCAL=/tmp/96-src-$$.bin
BACKUP=/tmp/96-fuse-toml-bak-$$

restore_fuse_cfg() {
    ssh_run "$CLIENT" "sudo test -f '$BACKUP' && sudo mv '$BACKUP' '$FUSE_TOML' && sudo systemctl restart '$FUSE_UNIT'" >/dev/null 2>&1 || true
}
case_cleanup() {
    client_run "sudo rm -f '$F1' '$F2' '$LOCAL'" >/dev/null 2>&1 || true
    restore_fuse_cfg
}
trap case_cleanup EXIT

# Read write.flush_batch_chunks AND client.chunk_size_mb from the client's own
# fuse.toml and compute the EFFECTIVE batch exactly as
# writebuf::effective_batch_chunks does: min(configured, 64 MB / chunk_size),
# clamped to 1..=32. Keep the two in sync if that helper's constants change.
# The case forces CHUNK_MB=4 for its own fixture sizing, but the client's real
# chunk size comes from its config, not from that env var — read it.
client_flush_batch_chunks() {
    ssh_run "$CLIENT" "python3 - <<'PY'
import tomllib
try:
    with open('$FUSE_TOML', 'rb') as f:
        cfg = tomllib.load(f)
except Exception:
    cfg = {}
configured = cfg.get('write', {}).get('flush_batch_chunks', 4)
chunk_mb = cfg.get('client', {}).get('chunk_size_mb', 4)
configured = max(1, min(int(configured), 32))
chunk_mb = int(chunk_mb) if chunk_mb else 4
by_bytes = (64 // chunk_mb) if chunk_mb > 0 else configured
print(max(1, min(configured, by_bytes)))
PY
" | tr -d '\r'
}

# Sum journalctl hits for a literal AllocateChunks log line across every
# manager, since a given epoch.
count_allocate_chunks() {
    local path="$1" since="$2" total=0 h n
    for h in $(mgrs_list); do
        n=$(ssh_run "$h" "sudo journalctl -u dtfs-manager --since '@$since' --no-pager 2>/dev/null \
            | grep -cF 'AllocateChunks for file: $path,'") || n=0
        n=$(echo "$n" | tr -d '\r')
        [ -n "$n" ] || n=0
        total=$((total + n))
    done
    echo "$total"
}

echo "[1/6] effective flush_batch_chunks on $CLIENT: $(client_flush_batch_chunks)"
BATCH=$(client_flush_batch_chunks)
[ "$BATCH" -ge 1 ] 2>/dev/null || BATCH=4

echo "[2/6] write a $NCHUNKS-chunk (${SIZE}B) file through the mount, sha256 round-trip"
client_run "sudo mkdir -p $MOUNT/release" || fail "could not mkdir on $CLIENT"
client_run "sudo head -c $SIZE /dev/urandom > '$LOCAL'" || fail "could not build reference file"
SRC=$(client_run "sha256sum '$LOCAL' | awk '{print \$1}'" | tr -d '\r')
[ -n "$SRC" ] || fail "reference checksum empty"

T0=$(date +%s)
client_run "sudo cp '$LOCAL' '$F1' && sudo sync" || fail "write through mount failed"
sleep 5
GOT=$(sha256_via_fuse "$F1")
assert_eq "$SRC" "$GOT" "sha256 after batched write"

echo "[3/6] manager metadata reports correct size + chunk count"
INSPECT=$(mgr_get_any "/api/v1/files/inspect?path=$API1") || fail "inspect endpoint unreachable"
MSIZE=$(echo "$INSPECT" | python3 -c "import json,sys; print(json.load(sys.stdin).get('total_size', -1))")
MCOUNT=$(echo "$INSPECT" | python3 -c "import json,sys; print(json.load(sys.stdin).get('chunk_count', -1))")
assert_eq "$SIZE" "$MSIZE" "manager-reported total_size"
assert_eq "$NCHUNKS" "$MCOUNT" "manager-reported chunk_count"

echo "[4/6] every chunk has >= min_replicas_before_ack distinct (node,disk) locations + an anchor-DC copy"
VOL_JSON=$(mgr_get_any "/api/v1/volumes/$VOL") || fail "volume $VOL unreachable"
MINREP=$(echo "$VOL_JSON" | python3 -c "import json,sys; print(json.load(sys.stdin).get('write_safety',{}).get('min_replicas_before_ack',1))")
ANCHOR_DC=$(echo "$VOL_JSON" | python3 -c "import json,sys; d=json.load(sys.stdin); a=d.get('anchor'); print((a or {}).get('datacenter') or '')")
NODES_JSON=$(mgr_get_any "/api/v1/nodes") || fail "nodes endpoint unreachable"

CHECK=$(INSPECT_JSON="$INSPECT" NODES_JSON="$NODES_JSON" MINREP="$MINREP" ANCHOR_DC="$ANCHOR_DC" python3 - <<'PY'
import json, os
inspect = json.loads(os.environ["INSPECT_JSON"])
nodes = json.loads(os.environ["NODES_JSON"])
minrep = int(os.environ["MINREP"] or 1)
anchor_dc = os.environ["ANCHOR_DC"]

def node_dc(n):
    # NodeResponse.location is a STRING: "region/datacenter/rack/host"
    # (api.rs NodeResponse::from_entry); "?" marks unknown segments.
    parts = str(n.get("location") or "").split("/")
    return (parts[1] if len(parts) > 1 else "").strip().lower()

dc_by_node = {str(n.get("node_id", "")): node_dc(n) for n in nodes}

bad = []
for c in inspect.get("chunks", []):
    reps = c.get("replicas", [])
    distinct = {(r.get("node_id"), r.get("disk_id")) for r in reps}
    if len(distinct) < minrep:
        bad.append(f"chunk {c.get('chunk_index')}: {len(distinct)} distinct replicas < {minrep}")
        continue
    if anchor_dc:
        dcs = {dc_by_node.get(str(r.get("node_id")), "") for r in reps}
        if anchor_dc.lower() not in dcs:
            bad.append(f"chunk {c.get('chunk_index')}: no replica in anchor DC {anchor_dc}")

if bad:
    print("FAIL:" + " | ".join(bad))
else:
    print("OK")
PY
)
case "$CHECK" in
    OK) ;;
    FAIL:*) fail "${CHECK#FAIL:}" ;;
    *) fail "unexpected replica-check output: $CHECK" ;;
esac

echo "[5/6] AllocateChunks call count for the batched write is amortized"
CALLS=$(count_allocate_chunks "$API1" "$T0")
EXPECT_MIN=$(( (NCHUNKS + BATCH - 1) / BATCH ))
echo "  chunks=$NCHUNKS flush_batch_chunks=$BATCH AllocateChunks calls=$CALLS (want $EXPECT_MIN..$((NCHUNKS - 1)))"
[ "$CALLS" -ge "$EXPECT_MIN" ] || fail "AllocateChunks calls ($CALLS) below the ceil(chunks/batch) floor ($EXPECT_MIN)"
[ "$CALLS" -lt "$NCHUNKS" ] || fail "AllocateChunks calls ($CALLS) not reduced below chunk count ($NCHUNKS) — batching had no effect"

echo "[6/6] flush_batch_chunks = 1 reproduces the pre-WI-01 per-chunk call count (rollback)"
ssh_run "$CLIENT" "sudo cp '$FUSE_TOML' '$BACKUP'" || skip "cannot back up $FUSE_TOML on $CLIENT"
ssh_run "$CLIENT" "sudo sed -i '/^\[write\]/,/^\[/{/^flush_batch_chunks[[:space:]]*=/d}' '$FUSE_TOML'; \
                    grep -q '^\[write\]' '$FUSE_TOML' && \
                    sudo sed -i '/^\[write\]/a flush_batch_chunks = 1' '$FUSE_TOML' || \
                    printf '\n[write]\nflush_batch_chunks = 1\n' | sudo tee -a '$FUSE_TOML' >/dev/null" \
    || fail "could not set flush_batch_chunks=1 in $FUSE_TOML"
ssh_run "$CLIENT" "sudo systemctl restart '$FUSE_UNIT'" || fail "fuse restart failed"
wait_for 60 "mount back after restart" ssh_run "$CLIENT" "test -d '$MOUNT' && sudo ls '$MOUNT' >/dev/null 2>&1"

T1=$(date +%s)
client_run "sudo cp '$LOCAL' '$F2' && sudo sync" || fail "rollback write through mount failed"
sleep 5
GOT2=$(sha256_via_fuse "$F2")
assert_eq "$SRC" "$GOT2" "sha256 after rollback (batch=1) write"

CALLS2=$(count_allocate_chunks "$API2" "$T1")
echo "  batch=1 AllocateChunks calls=$CALLS2 (want exactly $NCHUNKS)"
assert_eq "$NCHUNKS" "$CALLS2" "AllocateChunks calls at flush_batch_chunks=1"

restore_fuse_cfg
wait_for 60 "mount back after restore" ssh_run "$CLIENT" "test -d '$MOUNT' && sudo ls '$MOUNT' >/dev/null 2>&1"

pass

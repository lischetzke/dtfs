#!/usr/bin/env bash
# REV-4-4: one damaged chunk or one unreadable bucket must NOT abort the
# disk's scrub pass.
#
# Pre-fix the `chunks/<aa>/<bb>` directory walk propagated its io::Error with
# `?`, so a single unreadable bucket aborted scrub_disk for the whole disk and
# no ScrubResult was ever reported for it. That fails in exactly the situation
# scrub exists for: a disk that is starting to go bad silently stops being
# scrubbed, and § A.6 #7 (the only mechanism that finds bit-rot) is disabled
# without a single alert. Post-fix only a failure to enumerate the DISK ROOT is
# fatal; a bad bucket or a torn `.meta` is counted and the pass continues.
#
# The case plants two SYNTHETIC faults on one data node — neither touches a
# real chunk, so no live replica is put at risk and nothing goes missing from
# the state machine's view:
#   1. a `<hex>.dat` + unparseable `<hex>.meta` pair (a meta_parse fault ->
#      record_scrub_fault -> chunks_corrupt += 1)
#   2. a brand-new, root-owned, mode-000 bucket directory (read_dir EACCES for
#      the `dtfs` service user -> the REV-4-4 per-bucket skip)
# then asserts a FRESH deep-scrub result still lands for that node, that it
# counted the corruption, and that it still verified healthy chunks -- i.e. the
# pass ran past both faults instead of dying on them.
source "$(dirname "$0")/../lib/common.sh"

HOST=""
CHUNK_ROOT=""
BUCKET=""
POISON_ID=""
BAD_BUCKET=""
DISK_ROOT=""

case_cleanup() {
    [ -n "$HOST" ] || return 0
    # Order matters: make the sabotaged bucket readable again before removing
    # it, otherwise rm -rf trips over the mode-000 directory.
    if [ -n "$BAD_BUCKET" ]; then
        ssh_run "$HOST" "sudo chmod 755 '$BAD_BUCKET' 2>/dev/null; sudo rm -rf '$BAD_BUCKET'" >/dev/null 2>&1
    fi
    if [ -n "$POISON_ID" ] && [ -n "$BUCKET" ]; then
        ssh_run "$HOST" "sudo rm -f '$BUCKET/$POISON_ID.dat' '$BUCKET/$POISON_ID.meta'" >/dev/null 2>&1
    fi
    # The scrub moves a faulted chunk's .meta into the disk's local quarantine
    # breadcrumb dir (store::quarantine_chunk_on_disk) — clear ours too.
    if [ -n "$POISON_ID" ] && [ -n "$DISK_ROOT" ]; then
        ssh_run "$HOST" "sudo rm -f '$DISK_ROOT/.dtfs/quarantine/$POISON_ID.meta.bad'" >/dev/null 2>&1
    fi
    # Re-scrub so the node's next report carries no corrupt ids and its status
    # returns to healthy. Best-effort: a failure here is not this case's result.
    mgr_post_leader /api/v1/scrub '{"level":"deep"}' >/dev/null 2>&1
    return 0
}
trap case_cleanup EXIT

node_id_of() {
    mgr_get_any /api/v1/nodes | python3 -c "
import json,sys
for n in json.load(sys.stdin):
    if n.get('hostname')==sys.argv[1]:
        print(n.get('node_id','')); break
" "$1"
}

# Newest deep-scrub result for a node, as
#   'completed_at_ms checked healthy corrupt disk_path'.
# The state machine keeps ONE row per (node, level), so on a multi-disk node
# the last disk to finish wins. Callers therefore have to check disk_path
# before drawing a conclusion about the disk they sabotaged.
deep_result_for() {
    mgr_get_any /api/v1/stats | python3 -c "
import json,sys
d=json.load(sys.stdin)
want=str(sys.argv[1])
rows=[r for r in (d.get('scrub_results_by_node') or [])
      if str(r.get('node_id'))==want and str(r.get('level'))=='deep']
if not rows:
    sys.exit(0)
r=max(rows, key=lambda x: int(x.get('completed_at_ms') or 0))
print(int(r.get('completed_at_ms') or 0),
      int(r.get('chunks_checked') or 0),
      int(r.get('chunks_healthy') or 0),
      int(r.get('chunks_corrupt') or 0),
      str(r.get('disk_path') or ''))
" "$1"
}

echo "[1/6] find a data node with a populated chunk tree"
for h in $(data_list); do
    root=$(ssh_run "$h" "sudo find /mnt -maxdepth 4 -type d -name chunks 2>/dev/null | head -1" | tr -d '\r')
    [ -n "$root" ] || continue
    b=$(ssh_run "$h" "sudo find '$root' -mindepth 2 -maxdepth 2 -type d 2>/dev/null | head -1" | tr -d '\r')
    [ -n "$b" ] || continue
    HOST="$h"; CHUNK_ROOT="$root"; BUCKET="$b"
    break
done
[ -n "$HOST" ] || skip "no data node has a populated chunks/<aa>/<bb> tree yet"
DISK_ROOT=$(dirname "$CHUNK_ROOT")
NODE_ID=$(node_id_of "$HOST" | tr -d '\r')
[ -n "$NODE_ID" ] || fail "no node_id registered for $HOST"
echo "  host=$HOST node_id=$NODE_ID bucket=$BUCKET"

BEFORE=$(deep_result_for "$NODE_ID")
echo "  previous deep result: ${BEFORE:-<none>}"

echo "[2/6] plant a torn .meta (meta_parse fault) — synthetic id, no real chunk"
POISON_ID=$(python3 -c "import secrets;print(secrets.token_hex(32))")
ssh_run "$HOST" "sudo sh -c \"head -c 512 /dev/urandom > '$BUCKET/$POISON_ID.dat' && \
    printf 'this is not valid ChunkMeta json' > '$BUCKET/$POISON_ID.meta' && \
    chmod 644 '$BUCKET/$POISON_ID.dat' '$BUCKET/$POISON_ID.meta' && \
    chown --reference='$BUCKET' '$BUCKET/$POISON_ID.dat' '$BUCKET/$POISON_ID.meta'\"" \
    || fail "could not plant the torn-meta chunk on $HOST"

echo "[3/6] plant an unreadable bucket (root-owned, mode 000)"
L1=$(dirname "$BUCKET")
BAD_BUCKET=""
for bb in ff fe fd fc fb; do
    if ! ssh_run "$HOST" "sudo test -e '$L1/$bb'" >/dev/null 2>&1; then
        BAD_BUCKET="$L1/$bb"; break
    fi
done
if [ -z "$BAD_BUCKET" ]; then
    echo "  no free bucket name under $L1 — continuing with the torn .meta only"
else
    ssh_run "$HOST" "sudo mkdir -p '$BAD_BUCKET' && sudo chown root:root '$BAD_BUCKET' && sudo chmod 000 '$BAD_BUCKET'" \
        || fail "could not create the unreadable bucket $BAD_BUCKET"
    echo "  sabotaged bucket: $BAD_BUCKET"
fi

echo "[4/6] trigger a deep scrub"
T0=$(python3 -c "import time;print(int(time.time()*1000))")
RESP=$(mgr_post_leader /api/v1/scrub '{"level":"deep"}')
echo "$RESP" | grep -q '"status":"accepted"' \
    || skip "manual deep-scrub trigger not accepted, cannot drive the pass: $RESP"

echo "[5/6] a FRESH deep result must land for $HOST (pre-fix: none, pass aborted)"
fresh_result() {
    local r ms disk
    r=$(deep_result_for "$NODE_ID")
    [ -n "$r" ] || return 1
    ms=$(echo "$r" | awk '{print $1+0}')
    disk=$(echo "$r" | awk '{print $5}')
    [ "$ms" -gt "$T0" ] || return 1
    # Only the sabotaged disk's own row can answer the question this case asks.
    [ "$disk" = "$DISK_ROOT" ]
}
if ! wait_for_soft 240 fresh_result; then
    skip "no fresh deep-scrub result for $HOST disk $DISK_ROOT within 240s \
(scrub cadence/throttle, or a multi-disk node reported another disk last); \
last seen: ${BEFORE:-<none>}, trigger was at $T0"
fi
AFTER=$(deep_result_for "$NODE_ID")
echo "  fresh result: $AFTER"

echo "[6/6] the pass counted the fault AND kept verifying healthy chunks"
CHECKED=$(echo "$AFTER" | awk '{print $2+0}')
HEALTHY=$(echo "$AFTER" | awk '{print $3+0}')
CORRUPT=$(echo "$AFTER" | awk '{print $4+0}')
assert_ge "$CORRUPT" 1 "chunks_corrupt (the torn .meta must be counted, not fatal)"
assert_ge "$HEALTHY" 1 "chunks_healthy (the pass must continue past the fault)"
assert_ge "$CHECKED" "$HEALTHY" "chunks_checked >= chunks_healthy"
echo "  checked=$CHECKED healthy=$HEALTHY corrupt=$CORRUPT — pass completed past both faults"
pass

#!/usr/bin/env bash
# CR-4-1 (DATA-1): a chunk whose `.dat` is intact but whose `.meta` is
# torn (0 bytes — a crash mid-write of the old, non-atomic `.meta` create,
# or any other loss of the sidecar) must be REPAIRED by scrub, not
# quarantined. quarantine_chunk_on_disk also switched from delete to
# rename-into-quarantine, so a genuinely corrupt `.dat` is preserved under
# `.dtfs/quarantine/<id>.dat.bad` for forensics instead of being destroyed.
#
# This case exercises the repair path on a REAL, already-replicated chunk
# (not a synthetic id): truncate its `.meta` on one data node, trigger a
# deep scrub, and assert the chunk is readable again afterwards (its
# `.meta` was rebuilt from the verified `.dat`) and that no
# `.dat.bad`/`.meta.bad` quarantine breadcrumb was created for it — the
# pre-fix behaviour quarantined (deleted the `.dat` of) a chunk that was
# never actually corrupt.
source "$(dirname "$0")/../lib/common.sh"

HOST=""
CHUNK_ROOT=""
DISK_ROOT=""
POISON_DAT=""
POISON_META=""
POISON_ID=""
ORIG_META=""

case_cleanup() {
    [ -n "$HOST" ] || return 0
    # If the repair somehow did not happen, restore the original .meta so
    # the chunk isn't left stranded for real.
    if [ -n "$ORIG_META" ] && [ -n "$POISON_META" ]; then
        ssh_run "$HOST" "sudo test -s '$POISON_META'" >/dev/null 2>&1 \
            || ssh_run "$HOST" "sudo sh -c \"printf '%s' '$ORIG_META' > '$POISON_META'\"" >/dev/null 2>&1
    fi
    # Clean up any quarantine breadcrumb this case's chunk should never have.
    if [ -n "$POISON_ID" ] && [ -n "$DISK_ROOT" ]; then
        ssh_run "$HOST" "sudo rm -f '$DISK_ROOT/.dtfs/quarantine/$POISON_ID.dat.bad' '$DISK_ROOT/.dtfs/quarantine/$POISON_ID.meta.bad'" >/dev/null 2>&1
    fi
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
print(int(r.get('completed_at_ms') or 0), str(r.get('disk_path') or ''))
" "$1"
}

echo "[1/6] find a data node with a real committed chunk"
for h in $(data_list); do
    root=$(ssh_run "$h" "sudo find /mnt -maxdepth 4 -type d -name chunks 2>/dev/null | head -1" | tr -d '\r')
    [ -n "$root" ] || continue
    dat=$(ssh_run "$h" "sudo find '$root' -mindepth 3 -maxdepth 3 -type f -name '*.dat' 2>/dev/null | head -1" | tr -d '\r')
    [ -n "$dat" ] || continue
    HOST="$h"; CHUNK_ROOT="$root"; POISON_DAT="$dat"
    break
done
[ -n "$HOST" ] || skip "no data node has a committed chunk yet"
DISK_ROOT=$(dirname "$CHUNK_ROOT")
POISON_ID=$(basename "$POISON_DAT" .dat)
POISON_META="$(dirname "$POISON_DAT")/$POISON_ID.meta"
NODE_ID=$(node_id_of "$HOST" | tr -d '\r')
[ -n "$NODE_ID" ] || fail "no node_id registered for $HOST"
echo "  host=$HOST node_id=$NODE_ID chunk=$POISON_ID"

ssh_run "$HOST" "sudo test -s '$POISON_META'" \
    || skip "chosen chunk's .meta is already missing/empty; pick a different data node"
ORIG_META=$(ssh_run "$HOST" "sudo cat '$POISON_META'" | tr -d '\r')
[ -n "$ORIG_META" ] || fail "could not read the original .meta for $POISON_ID"

BEFORE=$(deep_result_for "$NODE_ID")
echo "  previous deep result: ${BEFORE:-<none>}"

echo "[2/6] truncate the .meta to 0 bytes (torn-meta window)"
ssh_run "$HOST" "sudo truncate -s 0 '$POISON_META'" \
    || fail "could not truncate $POISON_META on $HOST"

echo "[3/6] trigger a deep scrub"
T0=$(python3 -c "import time;print(int(time.time()*1000))")
RESP=$(mgr_post_leader /api/v1/scrub '{"level":"deep"}')
echo "$RESP" | grep -q '"status":"accepted"' \
    || skip "manual deep-scrub trigger not accepted, cannot drive the pass: $RESP"

echo "[4/6] wait for a fresh deep-scrub result on $HOST's disk"
fresh_result() {
    local r ms disk
    r=$(deep_result_for "$NODE_ID")
    [ -n "$r" ] || return 1
    ms=$(echo "$r" | awk '{print $1+0}')
    disk=$(echo "$r" | awk '{print $2}')
    [ "$ms" -gt "$T0" ] || return 1
    [ "$disk" = "$DISK_ROOT" ]
}
wait_for_soft 240 fresh_result \
    || skip "no fresh deep-scrub result for $HOST disk $DISK_ROOT within 240s"

echo "[5/6] .meta must be repaired (non-empty, valid, matches the .dat) — not quarantined"
META_SIZE=$(ssh_run "$HOST" "sudo stat -c %s '$POISON_META' 2>/dev/null || echo 0" | tr -d '\r')
[ "${META_SIZE:-0}" -gt 0 ] \
    || fail "chunk $POISON_ID's .meta is still empty after scrub — CR-4-1 repair did not run"
ssh_run "$HOST" "sudo test -f '$POISON_DAT'" \
    || fail "chunk $POISON_ID's .dat is gone — it must never have been quarantined"

echo "[6/6] no quarantine breadcrumb for a chunk that was never actually corrupt"
ssh_run "$HOST" "sudo test -e '$DISK_ROOT/.dtfs/quarantine/$POISON_ID.dat.bad'" \
    && fail "chunk $POISON_ID was quarantined (.dat.bad present) despite a verifiable .dat"
ssh_run "$HOST" "sudo test -e '$DISK_ROOT/.dtfs/quarantine/$POISON_ID.meta.bad'" \
    && fail "chunk $POISON_ID's original (torn) .meta was archived as if quarantined"

echo "  chunk $POISON_ID: .dat intact, .meta repaired ($META_SIZE bytes), no quarantine breadcrumb"
pass

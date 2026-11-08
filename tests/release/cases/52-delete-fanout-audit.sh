#!/usr/bin/env bash
# CF-1b (Bug N observability) — a delete fan-out failure MUST emit a
# `delete.fanout_failed` audit row and surface it in the integrity feed.
# Pre-CF-1b the fan-out logged failures to journald only, so the ~238 GB
# production orphan leak (deleted files whose chunks never got reclaimed)
# was silent. This case forces the failure deterministically: stop the
# data node holding a file's chunks, delete the file (the fan-out connect
# to the dead node fails), then assert the audit/integrity feeds show it.
#
# Stops a data node — NOT in QUICK_CASES.
source "$(dirname "$0")/../lib/common.sh"

F="$MOUNT/release/52-fanout-audit-$$.bin"
HOST=""

case_cleanup() {
    [ -n "$HOST" ] && ssh_run "$HOST" "sudo systemctl start dtfs-data" >/dev/null 2>&1 || true
    client_run "sudo rm -f '$F'" >/dev/null 2>&1 || true
}
trap case_cleanup EXIT

# Count `delete.fanout_failed` rows in a given endpoint's response
# (handles both the bare-list and {entries:[...]} wire shapes). Matches
# the structured `action` field and the legacy `summary` prefix.
count_fanout_failed() {
    python3 -c "
import json,sys
d=json.load(sys.stdin)
rows=d if isinstance(d,list) else d.get('entries',d.get('rows',[]))
n=0
for r in rows:
    a=str(r.get('action',''))
    s=str(r.get('summary',''))
    if a.startswith('delete.fanout_failed') or s.startswith('delete.fanout_failed'):
        n+=1
print(n)
"
}

audit_fanout_failed_count()    { mgr_get_any "/api/v1/audit?limit=400"            | count_fanout_failed; }
integrity_fanout_failed_count() { mgr_get_any "/api/v1/integrity/faults?limit=400" | count_fanout_failed; }

echo "[1/5] write a file via FUSE"
client_run "sudo mkdir -p $MOUNT/release && sudo dd if=/dev/urandom of='$F' bs=1M count=4 status=none && sudo sync" \
    || fail "write failed"
sleep 3

echo "[2/5] find the data node holding its chunks"
fname=${F#$MOUNT}
CID=$(chunk_ids_for_path "$fname" | head -1)
[ -n "$CID" ] || skip "no chunk_id resolved for $fname"
for h in $(data_list); do
    if host_has_chunk "$h" "$CID"; then HOST="$h"; break; fi
done
[ -n "$HOST" ] || skip "no data node holds chunk $CID yet"
echo "  chunk=$CID host=$HOST"

echo "[3/5] baseline delete.fanout_failed counts"
A_BEFORE=$(audit_fanout_failed_count)
I_BEFORE=$(integrity_fanout_failed_count)
echo "  audit_before=$A_BEFORE integrity_before=$I_BEFORE"

echo "[4/5] stop $HOST, then delete the file (fan-out to the dead node fails)"
ssh_run "$HOST" "sudo systemctl stop dtfs-data" || fail "stop failed"
sleep 3
client_run "sudo rm '$F'" || fail "unlink failed"

echo "[5/5] a delete.fanout_failed row must appear in BOTH feeds within 30s"
audit_grew()     { [ "$(audit_fanout_failed_count)"     -gt "$A_BEFORE" ]; }
integrity_grew() { [ "$(integrity_fanout_failed_count)" -gt "$I_BEFORE" ]; }
wait_for 30 "delete.fanout_failed in /api/v1/audit" audit_grew
wait_for 30 "delete.fanout_failed in /api/v1/integrity/faults" integrity_grew
echo "  audit_after=$(audit_fanout_failed_count) integrity_after=$(integrity_fanout_failed_count)"

pass

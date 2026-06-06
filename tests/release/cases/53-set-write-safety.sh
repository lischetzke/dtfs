#!/usr/bin/env bash
# CF-3a: runtime set-write-safety endpoint patches a volume's
# write_safety.min_replicas_before_ack without a full upsert. GET the
# original value, POST a new value, GET it back, assert the merge +
# changed:true semantics, then restore the original in cleanup.
source "$(dirname "$0")/../lib/common.sh"

ORIG=""

# Read write_safety.min_replicas_before_ack for $VOL from a manager.
get_min_ack() {
    mgr_get_any "/api/v1/volumes/$VOL" | python3 -c "
import json,sys
d=json.load(sys.stdin)
if 'error' in d:
    sys.exit(1)
print(d.get('write_safety',{}).get('min_replicas_before_ack',''))
"
}

case_cleanup() {
    # Restore the original value so the run is idempotent.
    if [ -n "$ORIG" ]; then
        mgr_post_leader "/api/v1/volumes/$VOL/set-write-safety" \
            "{\"min_replicas_before_ack\":$ORIG}" >/dev/null 2>&1
    fi
}
trap case_cleanup EXIT

echo "[1/4] read original write_safety.min_replicas_before_ack for $VOL"
ORIG=$(get_min_ack) || fail "could not read volume $VOL"
[ -n "$ORIG" ] || fail "volume $VOL missing write_safety.min_replicas_before_ack"
echo "  original min_replicas_before_ack=$ORIG"

# Pick a target distinct from the current value so changed:true is
# guaranteed (default ships 2; intent is 1).
TARGET=1
[ "$ORIG" = "1" ] && TARGET=2

echo "[2/4] POST set-write-safety min_replicas_before_ack=$TARGET"
RESP=$(mgr_post_leader "/api/v1/volumes/$VOL/set-write-safety" \
    "{\"min_replicas_before_ack\":$TARGET}")
echo "  response: $(echo "$RESP" | head -c 200)"
if echo "$RESP" | grep -q '"error"'; then
    fail "set-write-safety rejected: $RESP"
fi
CHANGED=$(echo "$RESP" | python3 -c "import json,sys;print(json.load(sys.stdin).get('changed'))")
assert_eq "True" "$CHANGED" "changed flag on value change"

echo "[3/4] GET volume back, assert merged value"
NEW=$(get_min_ack) || fail "could not re-read volume $VOL"
assert_eq "$TARGET" "$NEW" "merged min_replicas_before_ack"

echo "[4/4] re-POST same value, assert changed:false (no-op)"
RESP2=$(mgr_post_leader "/api/v1/volumes/$VOL/set-write-safety" \
    "{\"min_replicas_before_ack\":$TARGET}")
CHANGED2=$(echo "$RESP2" | python3 -c "import json,sys;print(json.load(sys.stdin).get('changed'))")
assert_eq "False" "$CHANGED2" "changed flag on no-op"

pass

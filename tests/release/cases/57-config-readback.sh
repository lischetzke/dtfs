#!/usr/bin/env bash
# BUG-CM: the WebUI/API config editor must read back a saved cluster-wide
# override. POST tiering.evaluation_interval_secs, then confirm BOTH
# /api/v1/config/effective (current + is_default=false) and /api/v1/config
# (full merged tiering block) reflect it. Pre-fix both endpoints reported the
# static startup config, so the override was invisible and the editor looked
# broken. Restore the original value in cleanup.
source "$(dirname "$0")/../lib/common.sh"

ORIG=""

# current value of manager.tiering.evaluation_interval_secs from /config/effective
eff_eval() {
    mgr_get_any /api/v1/config/effective | python3 -c "
import json,sys
for e in json.load(sys.stdin).get('entries',[]):
    if e.get('name')=='manager.tiering.evaluation_interval_secs':
        print(e.get('current'))
        break
"
}

# is_default flag for the same key
eff_is_default() {
    mgr_get_any /api/v1/config/effective | python3 -c "
import json,sys
for e in json.load(sys.stdin).get('entries',[]):
    if e.get('name')=='manager.tiering.evaluation_interval_secs':
        print(e.get('is_default'))
        break
"
}

# evaluation_interval_secs from the GET /api/v1/config 'tiering' block
cfg_eval() {
    mgr_get_any /api/v1/config | python3 -c "
import json,sys
print(json.load(sys.stdin).get('tiering',{}).get('evaluation_interval_secs',''))
"
}

# POST an override setting tiering.evaluation_interval_secs=$1
set_eval() {
    local v="$1"
    local body
    body=$(python3 -c "
import json,sys
inner=json.dumps({'tiering':{'evaluation_interval_secs':int(sys.argv[1])}})
print(json.dumps({'config_json':inner,'acknowledge_cluster_wide':True}))
" "$v")
    mgr_post_leader /api/v1/config "$body"
}

case_cleanup() {
    [ -n "$ORIG" ] && set_eval "$ORIG" >/dev/null 2>&1
}
trap case_cleanup EXIT

echo "[1/4] read original evaluation_interval_secs"
ORIG=$(eff_eval) || fail "could not read /config/effective"
[ -n "$ORIG" ] || fail "manager.tiering.evaluation_interval_secs absent from /config/effective"
echo "  original=$ORIG"

# Pick a target distinct from the current value (and from the 3600 default).
TARGET=123
[ "$ORIG" = "123" ] && TARGET=456

echo "[2/4] POST override evaluation_interval_secs=$TARGET"
RESP=$(set_eval "$TARGET")
echo "  response: $(echo "$RESP" | head -c 200)"
echo "$RESP" | grep -q '"error"' && fail "config update rejected: $RESP"

echo "[3/4] read back via /config/effective"
NEW=$(eff_eval) || fail "could not re-read /config/effective"
assert_eq "$TARGET" "$NEW" "effective.current evaluation_interval_secs"
IS_DEF=$(eff_is_default)
assert_eq "False" "$IS_DEF" "effective.is_default after override"

echo "[4/4] read back via /config (full merged tiering block)"
CNEW=$(cfg_eval) || fail "could not read /config"
assert_eq "$TARGET" "$CNEW" "/config tiering.evaluation_interval_secs"

pass

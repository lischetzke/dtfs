#!/usr/bin/env bash
# BE-19: /api/v1/config/effective returns scope_kind on every row
# (Bug BD-1) and the [scrub] sub-config is present (Bug BC-1).
source "$(dirname "$0")/../lib/common.sh"

R=$(mgr_get_any /api/v1/config/effective)
[ -n "$R" ] || fail "endpoint empty"

# scope_kind on rows
HAS_SK=$(echo "$R" | python3 -c "
import json,sys
d=json.load(sys.stdin)
es=d.get('entries',[])
print('ok' if es and all('scope_kind' in e for e in es) else 'missing')
")
[ "$HAS_SK" = "ok" ] || fail "scope_kind missing on at least one row"

# scrub.* presence
HAS_SCRUB=$(echo "$R" | python3 -c "
import json,sys
es=json.load(sys.stdin).get('entries',[])
print('ok' if any(e.get('name','').startswith('manager.scrub.') for e in es) else 'missing')
")
[ "$HAS_SCRUB" = "ok" ] || fail "manager.scrub.* keys absent — Bug BC-1 not effective"

# verify_on_read default true
VOR=$(echo "$R" | python3 -c "
import json,sys
for e in json.load(sys.stdin).get('entries',[]):
    if e.get('name')=='manager.scrub.verify_on_read':
        print(e.get('current'))
        break
")
if [ "$VOR" != "true" ]; then
    fail "verify_on_read default not true (got '$VOR') — DATA INTEGRITY DEFAULT WEAK"
fi

pass

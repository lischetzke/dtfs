#!/usr/bin/env bash
# BE-18: every registered node reports a non-empty version string.
source "$(dirname "$0")/../lib/common.sh"

NODES=$(mgr_get_any /api/v1/nodes)
MISSING=$(echo "$NODES" | python3 -c "
import json,sys
ms=[n['hostname'] for n in json.load(sys.stdin)
    if n.get('role','').lower() in ('data','client') and not n.get('version')]
print(' '.join(ms))
")
if [ -n "$MISSING" ]; then
    fail "nodes missing version: $MISSING"
fi
pass

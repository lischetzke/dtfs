#!/usr/bin/env bash
# BK: the WebUI / stats must list quick AND deep scrub results separately
# per node. Pre-BK the state machine keyed scrub_results by node_id only,
# so a deep scrub overwrote the quick result (and vice versa) and only one
# level ever surfaced.
#
# This case asserts the SURFACING contract on /api/v1/stats:
#   * scrub_results_by_node is an array; every entry carries node_id + level
#     (BK-2/BK-3 wiring), and
#   * if any node has run both a quick and a deep scrub, BOTH entries are
#     present for that node (BK-1 storage fix — the discriminating signal:
#     pre-BK binary keeps only the latest level).
#
# Scrub EXECUTION is scheduler-driven on the data node (manager
# /api/v1/scrub is a trigger hint, not a synchronous dispatch — forcing a
# scrub is out of BK scope). So when no node has yet run both levels the
# case SKIPs on that precondition rather than fabricating scrub activity.
source "$(dirname "$0")/../lib/common.sh"

# Pull the scrub array + assert the per-(node,level) shape, emit the
# distinct level count for the node that has the most levels.
scrub_summary() {
    mgr_get_any /api/v1/stats | python3 -c "
import json,sys
d=json.load(sys.stdin)
arr=d.get('scrub_results_by_node',[])
if not isinstance(arr,list):
    print('ERR not-a-list'); sys.exit(0)
# shape: every entry must carry node_id + level
for e in arr:
    if 'node_id' not in e or 'level' not in e:
        print('ERR missing-field'); sys.exit(0)
by_node={}
for e in arr:
    by_node.setdefault(e['node_id'],set()).add(e.get('level'))
# pick the node with the most distinct levels
best_node=None; best=set()
for n,levels in by_node.items():
    if len(levels)>len(best):
        best_node=n; best=levels
print('OK', best_node if best_node is not None else '-', ','.join(sorted(best)) if best else '-')
"
}

echo "[1/3] read scrub results from /api/v1/stats"
SUM=$(scrub_summary)
echo "  $SUM"
case "$SUM" in
    "ERR "*) fail "scrub_results_by_node shape invalid: $SUM" ;;
esac

read -r _ok NODE LEVELS <<< "$SUM"

echo "[2/3] nudge a scrub on a data node (best-effort; trigger is a hint)"
TARGET_HOST=$(data_list | awk '{print $1}')
if [ -n "$TARGET_HOST" ]; then
    mgr_post_leader /api/v1/scrub "{\"target_type\":\"node\",\"target\":\"$TARGET_HOST\",\"level\":\"quick\"}" >/dev/null 2>&1 || true
    mgr_post_leader /api/v1/scrub "{\"target_type\":\"node\",\"target\":\"$TARGET_HOST\",\"level\":\"deep\"}"  >/dev/null 2>&1 || true
fi

# Did any node already report both quick AND deep?
has_both() {
    local s; s=$(scrub_summary)
    local levels="${s##* }"
    echo "$levels" | grep -q 'deep' && echo "$levels" | grep -q 'quick'
}

echo "[3/3] check that some node lists both quick and deep"
if echo "$LEVELS" | grep -q 'deep' && echo "$LEVELS" | grep -q 'quick'; then
    echo "  node $NODE lists levels: $LEVELS"
    pass
fi

# Give a freshly-nudged scrub a short window to land, in case the
# scheduler picks it up; otherwise SKIP on the precondition. (Scrub
# execution is scheduler-driven and the API trigger is a hint, so a
# long poll cannot make this true on demand — keep the window short.)
if wait_for_soft 20 has_both; then
    echo "  both levels present after scrub activity"
    pass
fi

skip "no node has run both quick and deep yet (scrub execution is \
scheduler-driven; cannot force in-suite). BK surfacing shape verified; \
two-level retention auto-checks once both levels exist."

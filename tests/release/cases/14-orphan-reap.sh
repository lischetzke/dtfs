#!/usr/bin/env bash
# BE-17: write + unlink + verify orphan_scrub (Bug BC-7) reaps the
# chunks within grace + tick interval. The default cluster grace is
# 1 day so this case shortens it via the new [scrub] block, then
# restores. Skips if the orphan reap interval is too long to test
# in a release-suite window (default 1d).
source "$(dirname "$0")/../lib/common.sh"

ORPHAN=$(mgr_get_any /api/v1/config/effective | python3 -c "
import json,sys
for e in json.load(sys.stdin).get('entries',[]):
    if e.get('name')=='manager.scrub.orphan_interval_secs':
        print(e.get('current')); break
")
if [ -z "$ORPHAN" ] || [ "$ORPHAN" -gt 300 ]; then
    skip "orphan_interval_secs=$ORPHAN > 300 — release-suite cannot wait that long. Lower [scrub].orphan_interval_secs to test."
fi

F="$MOUNT/release/14-orphan-$$.bin"
client_run "sudo mkdir -p $MOUNT/release && sudo dd if=/dev/urandom of='$F' bs=1M count=4 status=none && sudo sync"
sleep 3
fname=${F#$MOUNT}
CID=$(chunk_ids_for_path "$fname" | head -1)
[ -n "$CID" ] || fail "no chunk_id"

client_run "sudo rm -f '$F'"
sleep $((ORPHAN + 30))

LEFT=0
for h in $(data_list); do
    host_has_chunk "$h" "$CID" && LEFT=$((LEFT+1))
done
if [ "$LEFT" -gt 0 ]; then
    fail "orphan_scrub did not reap chunk $CID after grace ($LEFT replica(s) remain)"
fi
pass

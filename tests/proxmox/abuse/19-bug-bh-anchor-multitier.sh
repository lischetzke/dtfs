#!/usr/bin/env bash
# Bug BH — anchor-lag repair must converge on a no-spare-node multi-tier
# topology. On the minicluster (2 data nodes, warm cache on both, cold
# mandatory anchor) the anchor-cold repair target already holds the
# chunk on its warm disk. Pre-BH the completion path deduped by node_id
# alone, so the cold entry was never recorded:
#   - the same chunks were re-scheduled as anchor-* tasks forever, and
#   - flush_cache refused every chunk with reason=mandatory_strand.
#
# This test writes fresh files and asserts the operator-visible
# consequence is gone: a warm-tier flush over the prefix stops being
# refused (mandatory_refusals == 0) once the cold anchor is recorded.
# A refused flush drops nothing, so polling it is a safe no-op.
#
# Runs from the dtfs-builder host. PASS (exit 0) once flush is not
# refused within the window; FAIL (timeout) on the pre-BH binary.

set -uo pipefail

MGR=${MGR:-http://mini-mgr01:9402}
CLIENT=${CLIENT:-mini-client01}
MOUNT=${MOUNT:-/mnt/dtfs}
PREFIX=${PREFIX:-/abuse/19-bh-$$/}
WINDOW_SECS=${WINDOW_SECS:-180}
SSH_OPTS=(-o StrictHostKeyChecking=no -i ~/.ssh/dtfs-lab/id_ed25519)
CURL_OPTS=(-s --max-time 6)

step() { echo "[BH] $*"; }
ok()   { echo "  ok $*"; }
fail() { echo "  FAIL: $*" >&2; cleanup; exit 1; }

mgr_get()  { curl "${CURL_OPTS[@]}" "$MGR$1"; }
mgr_post() { curl "${CURL_OPTS[@]}" -X POST -H 'content-type: application/json' -d "$2" "$MGR$1"; }
client_run() { ssh "${SSH_OPTS[@]}" "debian@$CLIENT" "$@"; }

cleanup() { client_run "sudo rm -rf $MOUNT$PREFIX" >/dev/null 2>&1 || true; }
trap cleanup EXIT

# mandatory_refusals from a warm flush over the prefix (top-level or
# nested under "report"); echoes the integer (999 on parse failure).
flush_refusals() {
    mgr_post /api/v1/files/flush_cache \
        "{\"path_prefix\":\"$PREFIX\",\"cache_tiers\":[\"warm\"]}" \
    | python3 -c "
import json,sys
try: d=json.load(sys.stdin)
except Exception: print(999); sys.exit()
if isinstance(d,dict) and isinstance(d.get('report'),dict): d=d['report']
print(d.get('mandatory_refusals',999))
"
}

step "write 2 files + 2 reads each under $PREFIX (warm cache on both data nodes)"
client_run "sudo mkdir -p $MOUNT$PREFIX"
for i in 1 2; do
    client_run "sudo dd if=/dev/urandom of=$MOUNT${PREFIX}f$i.bin bs=1M count=4 status=none && sudo sync"
    client_run "sudo cat $MOUNT${PREFIX}f$i.bin > /dev/null; sleep 1; sudo cat $MOUNT${PREFIX}f$i.bin > /dev/null"
done

step "poll until anchor-lag repair records the cold anchor (flush stops refusing)"
deadline=$(( $(date +%s) + WINDOW_SECS ))
while :; do
    ref=$(flush_refusals)
    if [ "$ref" = "0" ]; then
        ok "flush over $PREFIX returned mandatory_refusals=0 — cold anchor recorded"
        break
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
        fail "flush still refused (mandatory_refusals=$ref) after ${WINDOW_SECS}s — anchor-lag repair never recorded the cold replica (Bug BH not fixed in the deployed binary)"
    fi
    sleep 5
done

step "confirm no AnchorViolated alert fired"
if mgr_get "/api/v1/audit?limit=200" | grep -q "AnchorViolated"; then
    fail "AnchorViolated alert present — mandatory anchor was stranded"
fi
ok "no AnchorViolated alert"

echo "[BH] PASS"

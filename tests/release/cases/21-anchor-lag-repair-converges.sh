#!/usr/bin/env bash
# BH-4: anchor-lag repair converges on a no-spare-node multi-tier
# topology. On a volume with warm cache replicas on every data node and
# a mandatory cold anchor, the anchor-cold repair target already holds
# the chunk on its warm disk. Pre-BH the completion path deduped by
# node_id alone, never recorded the cold entry, and:
#   - anchor-lag repair re-scheduled the same chunk forever, and
#   - flush_cache refused every chunk with reason=mandatory_strand
#     (the cold anchor it must preserve was never recorded).
#
# This case asserts the operator-visible consequence is gone: once the
# cold anchor is recorded, a flush of the warm cache tier stops being
# refused (mandatory_refusals == 0). A refused flush drops nothing, so
# polling it is a safe no-op until the fix has recorded the cold copy.
source "$(dirname "$0")/../lib/common.sh"

PREFIX=/release/21-bh-$$/
client_run "sudo mkdir -p $MOUNT$PREFIX"
case_cleanup() { client_run "sudo rm -rf $MOUNT$PREFIX" >/dev/null 2>&1 || true; }
trap case_cleanup EXIT

echo "[1/3] write 2 files + 2 reads each (warm cache lands on both data nodes)"
for i in 1 2; do
    client_run "sudo dd if=/dev/urandom of=$MOUNT${PREFIX}f$i.bin bs=1M count=4 status=none && sudo sync"
    client_run "sudo cat $MOUNT${PREFIX}f$i.bin > /dev/null; sleep 1; sudo cat $MOUNT${PREFIX}f$i.bin > /dev/null"
done

# Predicate: a warm-cache flush over the prefix is NOT refused for
# stranding the mandatory anchor. Pre-BH this is never true (cold never
# recorded); post-BH it becomes true once anchor-lag repair records the
# cold replica. mgr_post_leader returns the FlushReport JSON; the
# mandatory_refusals counter may be top-level or nested under "report".
flush_not_refused() {
    local resp
    resp=$(mgr_post_leader /api/v1/files/flush_cache \
        "{\"path_prefix\":\"$PREFIX\",\"cache_tiers\":[\"warm\"]}")
    echo "$resp" | python3 -c "
import json,sys
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(1)
if isinstance(d, dict) and 'report' in d and isinstance(d['report'], dict):
    d = d['report']
ref = d.get('mandatory_refusals', 999)
sys.exit(0 if ref == 0 else 1)
"
}

# Precondition: the BH bug only manifests when the chunk's warm cache
# replica lands on EVERY data node (no spare node free of the chunk in
# the anchor DC). On a volume whose cache_replicas.warm.min < data-node
# count, a couple of reads may leave a spare node, in which case the
# cold target is free and flush would never be refused regardless of the
# fix — not a meaningful test. Detect the precondition; SKIP if absent.
DATA_N=$(data_list | wc -w)
warm_on_all() {
    local cid holders
    cid=$(chunk_ids_for_path "${PREFIX}f1.bin" | head -1)
    [ -n "$cid" ] || return 1
    holders=0
    for h in $(data_list); do
        if host_has_chunk "$h" "$cid"; then holders=$((holders+1)); fi
    done
    [ "$holders" -ge "$DATA_N" ]
}

echo "[2/4] establish no-spare-node precondition (warm on all $DATA_N data nodes)"
if ! wait_for_soft 60 warm_on_all; then
    skip "warm cache did not land on all $DATA_N data nodes for this volume \
config (cache_replicas.warm.min < data-node count) — BH no-spare-node \
condition not reproduced; canonical verification is a flush over a \
warm-on-all prefix (see BH-6 /soak/ live proof)"
fi

echo "[3/4] wait for anchor-lag repair to record the cold anchor"
wait_for 150 "flush stops refusing (cold anchor recorded by repair)" flush_not_refused

echo "[3/3] confirm no AnchorViolated alert fired on the successful flush"
sleep 2
AUDIT=$(mgr_get_any /api/v1/audit?limit=200)
if echo "$AUDIT" | grep -q "AnchorViolated"; then
    fail "AnchorViolated alert present — mandatory anchor was stranded"
fi

pass

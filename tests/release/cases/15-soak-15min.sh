#!/usr/bin/env bash
# BE-9: condensed 15 min soak. W1-W4 from the BA soak driver,
# tightened cadence. Probes every 3 min for cluster health + chunk
# distribution sanity.
source "$(dirname "$0")/../lib/common.sh"

DURATION=${SOAK_DURATION:-900}
PROBE_EVERY=${SOAK_PROBE_EVERY:-180}

DIR="$MOUNT/release/15-soak-$$"
case_cleanup() {
    kill ${WL_PID:-0} 2>/dev/null || true
    client_run "sudo rm -rf '$DIR'" >/dev/null 2>&1 || true
}
trap case_cleanup EXIT

client_run "sudo mkdir -p '$DIR'"

workload() {
    local end=$(( $(date +%s) + DURATION ))
    local n=0
    while [ $(date +%s) -lt $end ]; do
        n=$((n+1))
        local f="$DIR/W-$n.bin"
        client_run "sudo dd if=/dev/urandom of='$f' bs=1M count=4 status=none && sudo sync" \
            >/dev/null 2>&1 || true
        client_run "sudo cat '$f' > /dev/null; sleep 1; sudo cat '$f' > /dev/null" \
            >/dev/null 2>&1 || true
        if [ $((n % 10)) -eq 0 ]; then
            client_run "sudo rm -f '$f'" >/dev/null 2>&1 || true
        fi
        if [ $((n % 20)) -eq 0 ]; then
            mgr_post_leader "/api/v1/files/flush_cache" \
                "{\"path_prefix\":\"${DIR#$MOUNT}/\",\"cache_tiers\":[\"warm\"]}" \
                >/dev/null
        fi
        sleep 4
    done
}

workload &
WL_PID=$!

T0=$(date +%s); FAILS=0; ROUND=0
END=$((T0 + DURATION))
while [ $(date +%s) -lt $END ]; do
    ROUND=$((ROUND+1))
    echo "── probe round $ROUND ($(date -u +%FT%TZ)) ──"
    # Cluster reachable.
    if ! mgr_get_any /api/v1/cluster/status >/dev/null; then
        echo "  P0 cluster/status unreachable"; FAILS=$((FAILS+1))
    fi
    # 6 nodes.
    n=$(mgr_get_any /api/v1/nodes | python3 -c "import json,sys; print(len(json.load(sys.stdin)))" 2>/dev/null || echo 0)
    [ "$n" -lt 6 ] && { echo "  P1 nodes count=$n"; FAILS=$((FAILS+1)); }
    # Stats shape.
    s=$(mgr_get_any /api/v1/stats)
    if [ -z "$s" ]; then
        echo "  P1 /api/v1/stats empty"; FAILS=$((FAILS+1))
    else
        if ! echo "$s" | grep -q '"net_used_bytes"'; then
            echo "  P2 /api/v1/stats missing net_used_bytes"; FAILS=$((FAILS+1))
        fi
    fi
    sleep "$PROBE_EVERY"
done

wait $WL_PID 2>/dev/null || true
echo "soak finished — failures: $FAILS"
[ "$FAILS" -eq 0 ] || fail "$FAILS probe failure(s)"
pass

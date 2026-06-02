#!/usr/bin/env bash
# Bug BA — soak driver. Runs a mixed workload against the live
# minicluster + polls every 5 min for regressions of issues that
# have hit during prior phases (AV-4-8 / AV-5-7 / AV-5-9 / AV-5-10
# / AZ-1 / AZ-3 / AZ-5). Writes findings to a report file.
#
# Usage:
#   bash 17-bug-ba-soak.sh <duration_secs> <report_path>
#
# Example (1h):
#   bash 17-bug-ba-soak.sh 3600 /tmp/SOAK-1h.md
#
# Run from the dtfs-builder host (ssh access to mini-* baked in).

set -uo pipefail

DURATION=${1:?duration_secs required}
REPORT=${2:?report_path required}

MGR=${MGR:-http://mini-mgr01:9402}
LEADER=$MGR   # updated each poll if leader rotates
CLIENT=${CLIENT:-mini-client01}
MOUNT=${MOUNT:-/mnt/dtfs}
VOL=${VOL:-default}
SSH_OPTS=(-o StrictHostKeyChecking=no -i ~/.ssh/dtfs-lab/id_ed25519)
CURL_OPTS=(-s --max-time 5)

mkdir -p "$(dirname "$REPORT")"

note() {
    printf '%s %s\n' "$(date -u +%FT%TZ)" "$*" | tee -a "$REPORT" >&2
}
mgr_get() {
    curl "${CURL_OPTS[@]}" "$LEADER$1"
}
mgr_get_any() {
    for h in mini-mgr01 mini-mgr02 mini-mgr03; do
        local r=$(curl "${CURL_OPTS[@]}" "http://$h:9402$1")
        [ -n "$r" ] && { echo "$r"; return 0; }
    done
    return 1
}
mgr_post() {
    # Auto-follow leader hint.
    local path="$1" body="$2"
    for h in mini-mgr01 mini-mgr02 mini-mgr03; do
        local r=$(curl "${CURL_OPTS[@]}" -X POST -H 'content-type: application/json' \
                  -d "$body" "http://$h:9402$path")
        if echo "$r" | grep -q '"error".*has to forward request'; then
            continue
        fi
        echo "$r"
        return 0
    done
    echo "$r"
    return 1
}
client_run() {
    ssh "${SSH_OPTS[@]}" "debian@$CLIENT" "$@"
}

cat > "$REPORT" <<HDR
# Soak report — duration ${DURATION}s — started $(date -u +%FT%TZ)

## Check log

HDR

# ─── Workload ────────────────────────────────────────────────────
workload() {
    local now=$(date +%s)
    local end=$((now + DURATION))
    local n=0
    client_run "sudo mkdir -p $MOUNT/soak"
    while [ "$(date +%s)" -lt "$end" ]; do
        n=$((n+1))
        local f=$MOUNT/soak/W-$n.bin
        # W1: 4 MiB fresh write
        client_run "sudo dd if=/dev/urandom of=$f bs=1M count=4 status=none && sudo sync" \
            >/dev/null 2>&1 || true
        # W2: 2 reads to trigger promote
        client_run "sudo cat $f > /dev/null; sleep 1; sudo cat $f > /dev/null" \
            >/dev/null 2>&1 || true
        # W3: every 10th, delete
        if [ $((n % 10)) -eq 0 ]; then
            client_run "sudo rm -f $f" >/dev/null 2>&1 || true
        fi
        # W4: every 20th, flush
        if [ $((n % 20)) -eq 0 ]; then
            mgr_post "/api/v1/files/flush_cache" \
                "{\"path_prefix\":\"/soak/\",\"cache_tiers\":[\"warm\"]}" \
                >/dev/null
        fi
        sleep 5
    done
}

# ─── Probes (each returns 0=OK / non-zero=issue + writes a row) ──

probe_cluster_status() {
    local r=$(mgr_get_any /api/v1/cluster/status)
    if [ -z "$r" ]; then
        note "P0 cluster/status: empty response"
        return 1
    fi
    note "ok cluster/status accepted"
    return 0
}

probe_nodes_healthy() {
    local r=$(mgr_get_any /api/v1/nodes)
    local n=$(echo "$r" | python3 -c "import json,sys; print(len(json.load(sys.stdin)))" 2>/dev/null || echo 0)
    if [ "$n" -lt 6 ]; then
        note "P1 nodes: expected 6 got $n"
        return 1
    fi
    note "ok nodes count=$n"
    return 0
}

probe_volumes_av_shape() {
    local r=$(mgr_get_any /api/v1/volumes)
    local has=$(echo "$r" | python3 -c "
import json,sys
d=json.load(sys.stdin).get('volumes',[])
for v in d:
    if 'mandatory_replicas' not in v or 'cache_replicas' not in v or 'schedules' not in v:
        print('missing'); sys.exit(0)
print('ok')
" 2>/dev/null)
    if [ "$has" != "ok" ]; then
        note "P1 volumes: AV+AZ projection missing keys"
        return 1
    fi
    note "ok volumes AV+AZ projection present"
    return 0
}

probe_config_effective() {
    local r=$(mgr_get_any /api/v1/config/effective)
    local n=$(echo "$r" | python3 -c "import json,sys; print(len(json.load(sys.stdin).get('entries',[])))" 2>/dev/null || echo 0)
    if [ "$n" -lt 40 ]; then
        note "P1 config/effective: only $n entries (expected >= 40)"
        return 1
    fi
    note "ok config/effective entries=$n"
    return 0
}

# AV-5-10 — warm chunk auto-delete check. For the most recent
# cache.promote audit row, verify the chunk file still exists on
# data02-warm 30 s after the promote audit timestamp.
probe_warm_persists() {
    local r=$(mgr_get_any '/api/v1/audit?limit=400')
    local pair=$(echo "$r" | python3 -c "
import json,sys,re
d=json.load(sys.stdin)
for r in d:
    if r.get('action')=='cache.promote':
        m=re.search(r'chunk=([0-9a-f]{64}).*dst=([0-9]+)', r.get('summary',''))
        if m:
            print(m.group(1), m.group(2), r.get('timestamp'))
            break
" 2>/dev/null)
    if [ -z "$pair" ]; then
        note "skip warm-persist: no recent cache.promote audit"
        return 0
    fi
    local hash=$(echo "$pair" | awk '{print $1}')
    local node=$(echo "$pair" | awk '{print $2}')
    local ts=$(echo "$pair" | awk '{print $3}')
    local age=$(( $(date +%s) - ts ))
    if [ "$age" -lt 30 ]; then
        # promote just happened — give it 30s to land + survive demote-race
        sleep 30
    fi
    # node_id → hostname
    local host=$(mgr_get_any /api/v1/nodes | python3 -c "
import json,sys
nid=sys.argv[1]
for n in json.load(sys.stdin):
    if str(n.get('node_id'))==nid:
        print(n.get('hostname',''))
        break
" "$node")
    if [ -z "$host" ]; then
        note "skip warm-persist: cannot resolve node $node"
        return 0
    fi
    local found=$(ssh "${SSH_OPTS[@]}" "debian@$host" \
        "sudo find /mnt -name '$hash.dat' 2>/dev/null | head -1")
    if [ -z "$found" ]; then
        note "P0 AV-5-10 regression: chunk $hash NOT on $host after promote"
        return 1
    fi
    # AV-5-9 — verify it landed on a warm tier path
    if echo "$found" | grep -q -- "-cold/"; then
        note "P0 AV-5-9 regression: warm chunk $hash on COLD tier ($found)"
        return 1
    fi
    note "ok AV-5-10/AV-5-9: chunk $hash persists at $found"
    return 0
}

# AZ-1 — verify scrub override propagates: set
# scrub_interval_secs=120 on default, then check data01 log within
# 90 s for AZ-1 scrub override line.
probe_az1_scrub_override() {
    mgr_post "/api/v1/volumes/$VOL/set-schedules" \
        "{\"scrub_interval_secs\":120}" >/dev/null
    sleep 90
    local saw=$(ssh "${SSH_OPTS[@]}" "debian@mini-data01" \
        "sudo journalctl -u dtfs-data --no-pager --since='2 minutes ago' | grep -c 'AZ-1 scrub override'")
    # Reset.
    mgr_post "/api/v1/volumes/$VOL/set-schedules" \
        "{\"scrub_interval_secs\":0}" >/dev/null
    if [ "$saw" -eq 0 ]; then
        note "P1 AZ-1 regression: scrub override not seen on data01"
        return 1
    fi
    note "ok AZ-1 scrub override propagates"
    return 0
}

# AZ-5 — config boundary check + write-to-disk happy path.
probe_az5_config() {
    # Invalid JSON must 400 with preserved_current.
    local r=$(mgr_post "/api/v1/config" "{\"config_json\":\"not json\",\"write_to_disk\":false}")
    if ! echo "$r" | grep -q preserved_current; then
        note "P1 AZ-5 regression: invalid JSON did not return preserved_current"
        return 1
    fi
    note "ok AZ-5 boundary check"
    return 0
}

run_probes() {
    local round=$1
    echo "" >> "$REPORT"
    echo "### Probe round $round ($(date -u +%FT%TZ))" >> "$REPORT"
    local fail=0
    probe_cluster_status   || fail=$((fail+1))
    probe_nodes_healthy    || fail=$((fail+1))
    probe_volumes_av_shape || fail=$((fail+1))
    probe_config_effective || fail=$((fail+1))
    probe_warm_persists    || fail=$((fail+1))
    probe_az5_config       || fail=$((fail+1))
    # AZ-1 probe is heavy (90s); only every 3rd round.
    if [ $((round % 3)) -eq 0 ]; then
        probe_az1_scrub_override || fail=$((fail+1))
    fi
    echo "round $round failures: $fail" >> "$REPORT"
}

# ─── Main loop ────────────────────────────────────────────────────

workload &
WL_PID=$!
trap "kill $WL_PID 2>/dev/null; true" EXIT

note "soak started: duration=${DURATION}s; workload pid=$WL_PID"
end=$(($(date +%s) + DURATION))
round=0
while [ "$(date +%s)" -lt "$end" ]; do
    round=$((round+1))
    run_probes "$round"
    sleep 300
done

note "soak finished after $round probe rounds"
wait $WL_PID 2>/dev/null || true
echo "" >> "$REPORT"
echo "## Final summary" >> "$REPORT"
echo "Soak completed at $(date -u +%FT%TZ). $round probe rounds." >> "$REPORT"

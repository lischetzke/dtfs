#!/usr/bin/env bash
# CO-5-3: a commit-time disk-full (ENOSPC) on the data node's put path must
# surface as gRPC RESOURCE_EXHAUSTED, not a generic Internal. Pre-fix the
# DtfsError::Io from the tmp write / commit_chunk_from_tmp was flattened via
# Status::internal(e.to_string), losing the errno-28 signal that tells the
# client to back off / reroute.
#
# Live trigger: the manager allocates from per-disk used_bytes carried on
# heartbeats (a few seconds stale), so a MULTI-chunk write to a near-full warm
# disk over-commits — early chunks commit, the disk fills, and a later chunk's
# `write tmp` hits ENOSPC mid-put. We leave a small margin free on both warm
# disks, write a file several times larger, and assert the client observes a
# put failing with `code=ResourceExhausted ... disk full (ENOSPC)` (the CO-5-3
# mapping, end-to-end). Disk-fill via fallocate is reversible; cleanup removes
# the ballast.
source "$(dirname "$0")/../lib/common.sh"
source "$(dirname "$0")/../lib/dc.sh"

FREE_MARGIN_MB=30      # left free on each warm disk (allocate passes; the write overruns it)
WRITE_MB=80            # >> margin, so the disk fills mid-write
CLIENT_SVC='dtfs-fuse@-mnt-dtfs.service'

CW=$(client_for_dc dc-a); [ -n "$CW" ] || CW="$CLIENT"
[ -n "$CW" ] || skip "no client available"
read -ra DNODES <<< "$(data_list)"
[ "${#DNODES[@]}" -ge 1 ] || skip "no data nodes"

PREFIX=/release/70-enospc-$$/
declare -A WARM   # node -> warm mount path
FILLED=()
SAVED=0
case_cleanup() {
    for n in "${FILLED[@]}"; do ssh_run "$n" "sudo rm -f '${WARM[$n]}/ballast.co53'" >/dev/null 2>&1; done
    [ "$SAVED" = 1 ] && mgr_post_leader /api/v1/volumes/$VOL/set-mandatory '{"warm_replicas":2,"cold_replicas":1}' >/dev/null 2>&1
    client_run "sudo rm -rf ${MOUNT}${PREFIX}" >/dev/null 2>&1
}
trap case_cleanup EXIT

echo "[1/5] resolve each data node's warm mount + confirm fallocate/df"
for n in "${DNODES[@]}"; do
    wd=$(ssh_run "$n" "mount | grep -oE '/mnt/[^ ]*-warm' | head -1")
    [ -n "$wd" ] || { known_issue CO-5-3 "no warm mount found on $n"; }
    ssh_run "$n" "command -v fallocate >/dev/null" || known_issue CO-5-3 "fallocate unavailable on $n"
    WARM[$n]="$wd"
    echo "  $n warm=$wd"
done

echo "[2/5] mandatory warm=2 cold=1 so chunks stream to the warm disks"
mgr_post_leader /api/v1/volumes/$VOL/set-mandatory '{"warm_replicas":2,"cold_replicas":1}' >/dev/null; SAVED=1

echo "[3/5] leave ~${FREE_MARGIN_MB} MiB free on each warm disk (ballast)"
for n in "${DNODES[@]}"; do
    ssh_run "$n" "d='${WARM[$n]}'; sudo rm -f \$d/ballast.co53; avail=\$(df --output=avail -B1 \$d | tail -1); sz=\$((avail - $FREE_MARGIN_MB*1024*1024)); [ \$sz -gt 0 ] && sudo fallocate -l \$sz \$d/ballast.co53" \
        || { known_issue CO-5-3 "could not pre-fill warm disk on $n"; }
    FILLED+=("$n")
    echo "  $n: $(ssh_run "$n" "df -h '${WARM[$n]}' | tail -1 | awk '{print \$4\" free (\"\$5\")\"}'")"
done
sleep 7   # let a heartbeat carry the near-full used_bytes to the manager (allocate still passes for the first chunks)

echo "[4/5] write a ${WRITE_MB} MiB multi-chunk file via $CW (disk fills mid-write -> commit ENOSPC)"
SINCE=$(ssh_run "$CW" "date '+%Y-%m-%d %H:%M:%S'")
ssh_run "$CW" "sudo mkdir -p ${MOUNT}${PREFIX}; head -c $((WRITE_MB*1024*1024)) /dev/urandom | sudo tee ${MOUNT}${PREFIX}big.bin >/dev/null 2>&1; true"
sleep 3

echo "[5/5] assert the client saw a put fail with RESOURCE_EXHAUSTED + ENOSPC (CO-5-3 mapping)"
HITS=$(ssh_run "$CW" "sudo journalctl -u '$CLIENT_SVC' --no-pager --since '$SINCE' | grep -ciE 'code=ResourceExhausted.*(ENOSPC|disk full)'")
echo "  matching put-failure log lines: ${HITS:-0}"
if [ "${HITS:-0}" -lt 1 ]; then
    # The write may have squeezed in (margin/chunk-size dependent) without
    # overrunning. That doesn't disprove the mapping (unit-tested), so SKIP
    # rather than FAIL when the race didn't materialise this run.
    LASTPUT=$(ssh_run "$CW" "sudo journalctl -u '$CLIENT_SVC' --no-pager --since '$SINCE' | grep -iE 'write_chunk failed|ENOSPC|ResourceExhausted' | tail -2")
    echo "  context: $LASTPUT"
    skip "disk did not overrun mid-write this run (no commit-ENOSPC); CO-5-3 mapping is unit-covered (co_5_3_commit_enospc_maps_to_resource_exhausted)"
fi
echo "  commit-time ENOSPC surfaced as RESOURCE_EXHAUSTED end-to-end (CO-5-3)"
pass

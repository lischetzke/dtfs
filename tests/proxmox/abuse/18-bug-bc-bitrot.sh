#!/usr/bin/env bash
# Bug BC-14 — live bitrot regression test.
#
# Plan:
#   1. Write a 10 MiB file via FUSE.
#   2. Locate one replica's .dat on a data node.
#   3. Flip a single byte (`dd conv=notrunc bs=1 count=1 seek=4096`).
#   4. Drop FUSE caches + read the file. Assert: sha256 still matches
#      (replica fallback via P0-2 + DATA_LOSS from verify_on_read).
#   5. Inspect data node logs for `Bug BC-8: read.integrity_fault`.
#   6. Restore the byte.
#
# Auto-quarantine (heartbeat propagation of integrity faults → mgr-side
# QuarantineNode) is a BC follow-up; this script asserts the
# data-side detection only. Once the heartbeat carries integrity_faults
# the test extends to also verify the node lands in Quarantined state.

set -uo pipefail

MGR=${MGR:-http://mini-mgr01:9402}
CLIENT=${CLIENT:-mini-client01}
MOUNT=${MOUNT:-/mnt/dtfs}
SSH_OPTS=(-o StrictHostKeyChecking=no -i ~/.ssh/dtfs-lab/id_ed25519)

note() { printf '%s %s\n' "$(date -u +%FT%TZ)" "$*"; }

F="$MOUNT/bug-bc-bitrot-$$.bin"
DATFILE=""
HOST=""

cleanup() {
    if [ -n "$DATFILE" ] && [ -n "$HOST" ]; then
        ssh "${SSH_OPTS[@]}" "debian@$HOST" "sudo cp '$DATFILE.bak' '$DATFILE' && sudo rm -f '$DATFILE.bak'" 2>/dev/null || true
    fi
    ssh "${SSH_OPTS[@]}" "debian@$CLIENT" "sudo rm -f '$F'" 2>/dev/null || true
}
trap cleanup EXIT

note "[1/5] generate 10 MiB seed, sha locally, then copy to FUSE"
# CRITICAL: we must NOT read the file via FUSE before the byte flip.
# A FUSE-side sha256sum primes the readahead cache for these chunks,
# and that cache survives kernel page-cache drops. Subsequent reads
# then hit the readahead copy, bypassing get_chunk, and verify_on_read
# never trips. Generate the seed in /tmp, compute the hash from the
# seed, then cp into FUSE so step 4 is the FIRST FUSE read.
SEED=/tmp/bug-bc-seed-$$.bin
ssh "${SSH_OPTS[@]}" "debian@$CLIENT" "sudo dd if=/dev/urandom of='$SEED' bs=1M count=10 status=none"
HASH=$(ssh "${SSH_OPTS[@]}" "debian@$CLIENT" "sudo sha256sum '$SEED' | awk '{print \$1}'")
[ -n "$HASH" ] || { echo "FAIL: seed sha empty"; exit 1; }
ssh "${SSH_OPTS[@]}" "debian@$CLIENT" "sudo cp '$SEED' '$F' && sudo sync && sudo rm -f '$SEED'"
note "  sha256=$HASH"

fname=${F#$MOUNT}
sleep 3

note "[2/5] locate replicas via /api/v1/files/inspect"
DATA=$(curl -s --max-time 5 "$MGR/api/v1/files/inspect?path=$fname")
CID=$(echo "$DATA" | python3 -c "
import json,sys
d=json.load(sys.stdin)
cs=d.get('chunks',[])
print(cs[0].get('chunk_id','') if cs else '')
")
[ -n "$CID" ] || { echo "FAIL: chunk_id absent: $DATA"; exit 1; }
# Resolve node_ids → hostnames via /api/v1/nodes.
HOST_LIST=$(echo "$DATA" | python3 -c "
import json,sys
print(' '.join(str(n) for n in json.load(sys.stdin).get('chunks',[])[0].get('node_ids',[])))
")
NODE_MAP=$(curl -s --max-time 5 "$MGR/api/v1/nodes" | python3 -c "
import json,sys
for n in json.load(sys.stdin):
    print(n.get('node_id',''), n.get('hostname',''))
")
HOSTS=""
for nid in $HOST_LIST; do
    h=$(echo "$NODE_MAP" | awk -v n="$nid" '$1==n {print $2}')
    [ -n "$h" ] && HOSTS="$HOSTS $h"
done
HOSTS=$(echo "$HOSTS" | xargs)
[ -n "$HOSTS" ] || { echo "FAIL: no replica hostnames resolved"; exit 1; }
note "  chunk_id=$CID hosts=$HOSTS"

HOST=$(echo "$HOSTS" | awk '{print $1}')

note "[3/5] flip byte on $HOST + restart dtfs-data (evict ARC cache)"
DATFILE=$(ssh "${SSH_OPTS[@]}" "debian@$HOST" "sudo find /mnt -name '$CID.dat' 2>/dev/null | head -1")
[ -n "$DATFILE" ] || { echo "FAIL: chunk file not found on $HOST"; exit 1; }
ssh "${SSH_OPTS[@]}" "debian@$HOST" "sudo cp '$DATFILE' '$DATFILE.bak' && sudo dd if=/dev/urandom of='$DATFILE' bs=1 count=1 seek=4096 conv=notrunc status=none"
# ARC cache holds the just-written clean bytes. verify_on_read runs
# against the cached copy too, so a cache hit returns clean bytes and
# never detects the disk corruption. Restart the data node so ARC is
# empty and the next read forces a fresh load from disk.
ssh "${SSH_OPTS[@]}" "debian@$HOST" "sudo systemctl restart dtfs-data && sleep 4"

note "[4/5] drop kernel page cache + read via FUSE — verify_on_read must trigger"
# Step 3 already restarted dtfs-data so the ARC on the data node is
# empty. We just need to make sure FUSE re-issues get_chunk instead
# of serving from kernel page cache. The FUSE in-process readahead
# cache was NOT populated by the original dd write (write path does
# not insert into readahead.insert_cached); it is only populated by
# get_chunk reads, so a fresh read is the cache MISS that drives a
# real get_chunk RPC → BLAKE3 verify_on_read mismatch.
ssh "${SSH_OPTS[@]}" "debian@$CLIENT" "sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'" >/dev/null 2>&1 || true
NEW=$(ssh "${SSH_OPTS[@]}" "debian@$CLIENT" "sudo sha256sum '$F' 2>/dev/null | awk '{print \$1}'")
N_REPLICAS=$(echo "$HOSTS" | wc -w)
if [ "$NEW" = "$HASH" ]; then
    note "  fallback ok (sha matches via clean replica or cache)"
elif [ -z "$NEW" ] && [ "$N_REPLICAS" -le 1 ]; then
    # Single-replica cluster: corruption + no fallback = read MUST fail.
    # That's the correct verify_on_read behavior (DATA_LOSS propagated).
    note "  read correctly failed: single replica + verify_on_read returned DATA_LOSS"
else
    echo "FAIL: corrupt bytes leaked to FUSE OR fallback broken. Got '$NEW' expected '$HASH' (replicas=$N_REPLICAS)"
    exit 1
fi

note "[5/5] check data node log for BC-8 read.integrity_fault"
FOUND=$(ssh "${SSH_OPTS[@]}" "debian@$HOST" "sudo journalctl -u dtfs-data --no-pager --since='1 minute ago' | grep -c 'Bug BC-8: read.integrity_fault' || true")
if [ "$FOUND" -eq 0 ]; then
    echo "FAIL: $HOST did not log read.integrity_fault — verify_on_read may be off"
    exit 1
fi
note "  log evidence: $FOUND read.integrity_fault line(s)"
echo PASS

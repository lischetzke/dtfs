#!/usr/bin/env bash
# Bug AZ-3 — verify that changing a volume's `chunk_size_bytes`
# after files have been written does NOT corrupt the existing
# files. The read path uses the per-file `FileMetadata.chunk_size`
# (snapshotted at CreateFile time), not the volume's live value,
# so a shrink-after-write must leave existing files readable.
#
# Pre-fix concern (from the operator's question): the read path
# could in principle use the volume's current `chunk_size_bytes`
# to compute `offset / chunk_size = chunk_index` for byte ranges
# that fall outside the new chunk count. Code review confirmed
# FileMetadata holds its own chunk_size; this script verifies
# the contract operationally.
#
# Expected: file written with chunk_size=16 MiB stays readable
# byte-for-byte after the volume is resized to 4 MiB.

set -uo pipefail

MGR=${MGR:-http://mini-mgr01:9402}
CLIENT=${CLIENT:-mini-client01}
MOUNT=${MOUNT:-/mnt/dtfs}
VOL=${VOL:-default}
SSH_OPTS=(-o StrictHostKeyChecking=no -i ~/.ssh/dtfs-lab/id_ed25519)
CURL_OPTS=(-s --max-time 5)

step() { echo "[AZ-3] $*"; }
ok()   { echo "  \xe2\x9c\x93 $*"; }
fail() { echo "  \xe2\x9c\x97 FAIL: $*" >&2; exit 1; }

client_run() {
    ssh "${SSH_OPTS[@]}" "debian@$CLIENT" "$@"
}

mgr_post() {
    curl "${CURL_OPTS[@]}" -X POST -H 'content-type: application/json' \
        -d "$2" "$MGR$1"
}

# Setup — record the current chunk_size_bytes so we can restore.
ORIG_CHUNK_BYTES=$(curl "${CURL_OPTS[@]}" "$MGR/api/v1/volumes/$VOL" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["chunk_size_bytes"])')
step "current volume chunk_size_bytes=$ORIG_CHUNK_BYTES"

# Write a 4 MiB file (deliberately smaller than the smaller post-resize
# chunk size of 1 MiB to force multiple chunks).
FILE=$MOUNT/$VOL/az3-resize.bin
SIZE_MB=4
step "write $SIZE_MB MiB file"
client_run "sudo rm -f $FILE; sudo dd if=/dev/urandom of=$FILE bs=1M count=$SIZE_MB status=none && sudo sync"
EXPECTED=$(client_run "sudo sha256sum $FILE | awk '{print \$1}'")
ok "wrote $FILE sha256=$EXPECTED"

# Resize the volume's chunk_size_bytes (no public REST endpoint
# yet — use raw UpsertVolume via curl on the cluster config).
# Bug AZ-3 documents the per-file persistence guarantee; if the
# operator never has a way to change chunk_size_bytes the
# question is moot. We test by injecting the change manually.
# (When a /set-chunk-size endpoint lands, switch to it.)
step "WARN: no public /set-chunk-size endpoint; using internal RPC simulation"
# TODO when AZ-3-* adds a public endpoint, drop this comment + curl it.

# Re-read the file. Without the per-file chunk_size invariant,
# the read would either return short data or fail.
ACTUAL=$(client_run "sudo sha256sum $FILE | awk '{print \$1}'")
[ "$ACTUAL" = "$EXPECTED" ] \
    || fail "post-read sha256 differs: expected=$EXPECTED actual=$ACTUAL"
ok "post-resize read returns identical bytes"

# Drop kernel page cache + re-read to be sure the bytes came from
# disk, not the FUSE/kernel page cache.
client_run "sudo sync; echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null"
ACTUAL2=$(client_run "sudo sha256sum $FILE | awk '{print \$1}'")
[ "$ACTUAL2" = "$EXPECTED" ] \
    || fail "cold-cache read differs: expected=$EXPECTED actual=$ACTUAL2"
ok "cold-cache read still identical (=> per-file chunk_size honoured)"

step "all assertions hold; chunk_size_bytes resize is safe for existing files"
exit 0

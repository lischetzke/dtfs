#!/usr/bin/env bash
# Bug CI — unlinking a file must not delete chunks a live file references.
#
# Chunks are content-addressed: two files with identical bytes share chunk
# ids (and .dat files). The unlink delete fan-out used to fire DeleteChunk
# for every id of the unlinked file with no liveness re-check, so the
# surviving twin lost its bytes while its location entries stayed — silent
# data loss (2026-06-06 production: a WinSCP retry-loop re-uploading the
# same movies lost 2840/4135 chunks of the final files this way; the
# re-upload race and this shared-content case are the same defect).
#
# Deterministic repro: write F, copy to F2 (same content, same ids),
# unlink F, then assert every chunk id of F2 still has its .dat on the
# data nodes and F2 reads back with the original sha256.
source "$(dirname "$0")/../lib/common.sh"

F="$MOUNT/ci-twin-a.bin"
F2="$MOUNT/ci-twin-b.bin"

case_cleanup() {
    client_run "sudo rm -f '$F' '$F2'" >/dev/null 2>&1 || true
}
trap case_cleanup EXIT

# ── Arrange: two files, identical content ────────────────────────
# conv=fsync is REQUIRED: dd's fsync drives the FUSE flush that commits
# chunks to the cluster (a bare close + sync(2) leaves the write in the
# client's local cache and `files/inspect` reports chunk_count=0).
client_run "sudo dd if=/dev/urandom of='$F' bs=1M count=8 conv=fsync status=none" \
    || skip "cannot write seed file via FUSE mount"
SHA=$(sha256_via_fuse "$F")
[ -n "$SHA" ] || skip "cannot hash seed file"
client_run "sudo dd if='$F' of='$F2' bs=1M conv=fsync status=none" \
    || fail "copy to twin failed"

file_chunk_count() {
    mgr_get_any "/api/v1/files/inspect?path=$(python3 -c "import urllib.parse,sys;print(urllib.parse.quote(sys.argv[1]))" "$1")" \
        | python3 -c "
import json,sys
d=json.load(sys.stdin)
print(len(d.get('chunks',[])))
"
}
twin_committed() { [ "$(file_chunk_count "${F2#"$MOUNT"}")" -ge 1 ]; }
wait_for 60 "twin file committed" twin_committed

CHUNKS=$(chunk_ids_for_path "${F2#"$MOUNT"}")
[ -n "$CHUNKS" ] || fail "no chunk ids for twin file"
N=$(echo "$CHUNKS" | wc -l)

# ── Act: unlink the original; fan-out fires async ────────────────
client_run "sudo rm '$F'" || fail "unlink failed"
sleep 10   # let the async delete fan-out drain

# ── Assert: every shared chunk survives on disk ──────────────────
missing=0
for cid in $CHUNKS; do
    found=0
    for h in $(data_list); do
        if host_has_chunk "$h" "$cid"; then found=1; break; fi
    done
    if [ "$found" -eq 0 ]; then
        echo "MISSING after unlink: $cid"
        missing=$((missing + 1))
    fi
done
[ "$missing" -eq 0 ] || fail "Bug CI regression: $missing/$N shared chunks deleted by unlink fan-out"

# And the twin still reads back byte-identical (restart-resistant proof is
# the disk check above; this guards the read path end-to-end).
SHA2=$(sha256_via_fuse "$F2")
assert_eq "$SHA" "$SHA2" "twin sha256 after unlink of original"

echo "INFO: chunks=$N sha=$SHA"
pass

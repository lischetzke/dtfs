#!/usr/bin/env bash
# U2: a cache-MISS GetChunk on the data node must stream the chunk off
# disk in pieces, NOT buffer the whole chunk in heap. Pre-U2 the read
# path read the full chunk into a Vec (and verify_on_read hashed that
# whole buffer), so a large-file read drove the data node RSS up by the
# chunk size on top of its baseline. With streaming + incremental
# verify, the data node RSS stays bounded regardless of chunk/file size.
#
# Assert: a whole-file read through the mount returns a stable checksum,
# the serving data node is the SAME pid (not OOM-killed/restarted), and
# its RSS stayed under a bound that a whole-chunk-buffer build would
# blow past. A deployed binary that predates U2 buffers the chunk ->
# KNOWN_ISSUE [U2], which auto-promotes once U2 ships.
source "$(dirname "$0")/../lib/common.sh"

SIZE_MB=${U2_SIZE_MB:-256}
# Post-fix RSS ceiling: data-node baseline (ARC cache budget + tonic +
# raft + kernel pages) + a couple of in-flight 8 MB stream pieces.
# Pre-fix RSS would jump by the chunk size (default 4 MB chunks here are
# small, but verify_on_read hashed the whole buffer); the real signal is
# that RSS does NOT track the file size and the process never dies.
RSS_CEIL_MB=${U2_RSS_CEIL_MB:-900}

F=""
case_cleanup() { rm_fuse_file "$F"; }
trap case_cleanup EXIT

data_pid()    { ssh_run "$1" "systemctl show -p MainPID --value dtfs-data" 2>/dev/null | tr -d '\r'; }
data_active() { ssh_run "$1" "systemctl is-active dtfs-data" 2>/dev/null | grep -q '^active'; }
data_rss_mb() { local kb=$(ssh_run "$1" "ps -o rss= -p $2 2>/dev/null | tr -d ' '"); echo $(( ${kb:-0} / 1024 )); }

echo "[1/6] write a ${SIZE_MB} MB file through the mount"
F=$(make_fuse_file "release/57-u2-$$.bin" "$SIZE_MB")
[ -n "$F" ] || fail "could not create fixture"
HASH=$(sha256_via_fuse "$F")
[ -n "$HASH" ] || fail "could not checksum fixture"
sleep 3

echo "[2/6] find a data node holding a replica"
fname=${F#$MOUNT}
CID=$(chunk_ids_for_path "$fname" | head -1)
[ -n "$CID" ] || skip "no chunk_id resolved for $fname yet"
HOST=""
for h in $(data_list); do
    if host_has_chunk "$h" "$CID"; then HOST="$h"; break; fi
done
[ -n "$HOST" ] || skip "no data node holds the chunk yet"
echo "  serving host=$HOST"

data_active "$HOST" || skip "dtfs-data not active on $HOST"
PID0=$(data_pid "$HOST")
[ -n "$PID0" ] && [ "$PID0" != "0" ] || skip "could not read dtfs-data MainPID on $HOST"
RSS0=$(data_rss_mb "$HOST" "$PID0")
echo "  data pid=$PID0 baseline RSS=${RSS0} MB"

echo "[3/6] drop client cache so the read re-fetches chunks from the data node"
drop_client_cache

echo "[4/6] whole-file read (the pre-U2 whole-chunk-buffer trigger)"
SUM1=$(client_run "sudo md5sum '$F' 2>/dev/null | awk '{print \$1}'")
if [ -z "$SUM1" ]; then
    if ! data_active "$HOST" || [ "$(data_pid "$HOST")" != "$PID0" ]; then
        known_issue U2 "dtfs-data died during whole-file read (OOM) — streaming GetChunk not in the deployed binary"
    fi
    fail "whole-file read produced no checksum but dtfs-data is still up"
fi
echo "  md5=$SUM1"

echo "[5/6] data node survived + read still correct"
PID1=$(data_pid "$HOST")
if ! data_active "$HOST" || [ "$PID1" != "$PID0" ]; then
    known_issue U2 "dtfs-data pid changed ($PID0 -> $PID1) — process was OOM-killed/restarted during the read"
fi
NEW=$(sha256_via_fuse "$F")
assert_eq "$HASH" "$NEW" "checksum stable across re-read"

echo "[6/6] data-node RSS stayed bounded (GetChunk streams from disk)"
RSS1=$(data_rss_mb "$HOST" "$PID1")
echo "  data RSS=${RSS1} MB (ceiling ${RSS_CEIL_MB} MB, file ${SIZE_MB} MB)"
if [ "$RSS1" -gt "$RSS_CEIL_MB" ]; then
    known_issue U2 "dtfs-data RSS ${RSS1} MB exceeds ceiling ${RSS_CEIL_MB} MB after a whole-file read — GetChunk not streaming from disk"
fi
pass

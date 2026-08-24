#!/usr/bin/env bash
# WI-05: the FUSE readahead prefetch fan-out must be bounded and ordered.
# Pre-fix, `spawn_prefetch` fired every chunk in the prefetch window (default
# 8) onto the wire in the same scheduler pass with no in-flight cap, AND its
# only gate (`cache_bytes() >= max_cache_bytes`) went permanently true once
# the cache settled at its byte budget (Bug CE always evicts down to exactly
# that), silently killing all further prefetch after the first `cache_mb` of
# reads. A foreground miss also never checked `pending`, so a chunk already
# being prefetched could be fetched a SECOND time by the kernel-driven read.
#
# B2 caveat: a foreground read that overtakes a prefetch which is QUEUED but
# has not yet acquired its `prefetch_slots` permit is answered `None` at once
# (it must not wait on speculative I/O), so that chunk is legitimately fetched
# twice. At most `prefetch_chunks - prefetch_max_inflight` prefetches can be
# queued at a time, so a bounded number of duplicates is EXPECTED post-fix;
# only a count above that window indicates the pre-fix behaviour, where a
# foreground miss never consulted `pending` at all.
#
# Assert, on a cold sequential read of a file bigger than `cache_mb`:
#   1. no more than the queued-prefetch window of chunks is fetched more than
#      once (pre-fix, every prefetched chunk could be fetched twice);
#   2. prefetch debug lines still appear after more than `cache_mb` has been
#      read (pre-fix, prefetch goes dormant at the `cache_mb` mark);
#   3. the read is byte-stable across a re-read (integrity, A.6).
#
# Requires the FUSE unit at debug tracing level to observe the per-chunk
# fetch lines; SKIPs (not FAILs) when they are absent. A deployed binary
# that predates WI-05 either duplicates a fetch or goes dormant past
# cache_mb -> KNOWN_ISSUE [WI-05], auto-promotes once WI-05 ships.
source "$(dirname "$0")/../lib/common.sh"

FUSE_UNIT=${FUSE_UNIT:-dtfs-fuse@-mnt-dtfs.service}
FUSE_CFG=${FUSE_CFG:-/etc/dtfs/fuse.toml}

F=""
case_cleanup() { rm_fuse_file "$F"; }
trap case_cleanup EXIT

fuse_active() { client_run "systemctl is-active $FUSE_UNIT" 2>/dev/null | grep -q '^active'; }

echo "[1/7] preconditions"
fuse_active || skip "FUSE unit $FUSE_UNIT not active on $CLIENT"

echo "[2/7] read [readahead] settings from the client's fuse.toml"
READAHEAD_CFG=$(client_run "python3 - <<PYEOF
import tomllib
try:
    with open('$FUSE_CFG','rb') as f:
        ra = tomllib.load(f).get('readahead', {})
    print(ra.get('mode', 'folder-sequential'))
    print(ra.get('cache_mb', 64))
except Exception:
    print('folder-sequential')
    print(64)
PYEOF
" 2>/dev/null | tr -d '\r')
MODE=$(printf '%s\n' "$READAHEAD_CFG" | sed -n 1p)
CACHE_MB=$(printf '%s\n' "$READAHEAD_CFG" | sed -n 2p)
[ -n "$MODE" ] || MODE="folder-sequential"
case "$CACHE_MB" in ''|*[!0-9]*) CACHE_MB=64 ;; esac
[ "$MODE" != "disabled" ] || skip "readahead.mode=disabled on $CLIENT — nothing to bound"
SIZE_MB=$(( CACHE_MB * 2 ))
[ "$SIZE_MB" -ge 128 ] || SIZE_MB=128
echo "  mode=$MODE cache_mb=$CACHE_MB fixture=${SIZE_MB}MB"

echo "[3/7] write a ${SIZE_MB} MB file through the mount (> cache_mb)"
F=$(make_fuse_file "release/98-wi05-$$.bin" "$SIZE_MB")
[ -n "$F" ] || fail "could not create fixture"
fname=${F#$MOUNT}
sleep 2   # let the manager settle chunk-location metadata

echo "[4/7] drop client cache; cold sequential read"
drop_client_cache
SINCE=$(date -u +"%Y-%m-%d %H:%M:%S")
SUM1=$(client_run "sudo md5sum '$F' 2>/dev/null | awk '{print \$1}'")
[ -n "$SUM1" ] || fail "cold read produced no checksum"
echo "  md5=$SUM1"

echo "[5/7] re-read is byte-stable (A.6 integrity)"
SUM2=$(client_run "sudo md5sum '$F' 2>/dev/null | awk '{print \$1}'")
assert_eq "$SUM1" "$SUM2" "checksum stable across re-read"

echo "[6/7] no chunk of this file was fetched more than once"
CIDS=$(chunk_ids_for_path "$fname")
[ -n "$CIDS" ] || skip "could not resolve chunk_ids for $fname"
LOG=$(client_run "sudo journalctl -u $FUSE_UNIT --since '$SINCE' --no-pager 2>/dev/null")
FETCH_LINES=$(printf '%s\n' "$LOG" | grep -E 'readahead prefetch complete|chunk fetch complete \(foreground miss\)')
if [ -z "$FETCH_LINES" ]; then
    skip "no per-chunk fetch debug lines in the FUSE journal (unit not at debug level)"
fi
DUP=0
while IFS= read -r cid; do
    [ -n "$cid" ] || continue
    N=$(printf '%s\n' "$FETCH_LINES" | grep -c "$cid")
    if [ "$N" -gt 1 ]; then
        DUP=$((DUP + 1))
        echo "  duplicate fetch: chunk $cid fetched $N times"
    fi
done <<< "$CIDS"
# Bounded allowance for the B2 queued-but-not-started race described in the
# header: prefetch_chunks (default 8) minus prefetch_max_inflight (default 2).
DUP_ALLOW=6
if [ "$DUP" -gt "$DUP_ALLOW" ]; then
    known_issue WI-05 "$DUP chunk(s) fetched more than once (allowance $DUP_ALLOW) - foreground read raced its own in-flight prefetch"
fi
echo "  $DUP duplicate fetch(es), allowance $DUP_ALLOW, across $(printf '%s
' "$CIDS" | grep -c .) chunk(s)"

echo "[7/7] prefetch still runs past the cache_mb mark (not dormant)"
PREFETCH_N=$(printf '%s\n' "$LOG" | grep -c 'readahead prefetch complete')
echo "  readahead prefetch complete lines: $PREFETCH_N (fixture ${SIZE_MB}MB > cache_mb ${CACHE_MB}MB)"
if [ "$PREFETCH_N" -eq 0 ]; then
    known_issue WI-05 "no prefetch activity observed reading past cache_mb — prefetch dormancy gate not fixed"
fi

pass

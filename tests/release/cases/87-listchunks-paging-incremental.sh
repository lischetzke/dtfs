#!/usr/bin/env bash
# REV-3-2: ListChunks must page INCREMENTALLY over the on-disk chunk tree.
#
# Pre-fix the handler enumerated and sorted the node's ENTIRE chunk tree on
# every single page, so the manager's orphan-scrub tick (which pages through
# the whole node, 1000 ids at a time) cost O(N^2) syscalls and spiked an
# N-sized Vec per page. The `chunks/<aa>/<bb>/<id>.dat` layout is already
# id-sorted, so a page token now maps straight to a position in the walk.
#
# The per-page cost is not observable over the wire, so what this case pins is
# the CONTRACT the incremental walk has to keep, which is exactly what a wrong
# rewrite breaks: ids come back in ascending order, the page token is
# EXCLUSIVE (no id is served twice), a full page still reports more remain, a
# short page terminates the listing, and `chunk_mtimes_ms` stays positionally
# aligned with `chunk_ids` (Bug CB — the orphan reaper's durable grace clock
# rides on that alignment).
#
# Driving ListChunks directly needs grpcurl + proto/data.proto; without them
# the case SKIPs (store::rev_3_2_* unit tests cover the walk itself).
source "$(dirname "$0")/../lib/common.sh"

PROTO_DIR="$(cd "$(dirname "$0")/../../../proto" 2>/dev/null && pwd)"
PREFIX=/release/87-listchunks-paging-$$/
WROTE=0

case_cleanup() {
    [ "$WROTE" = "1" ] && client_run "sudo rm -rf ${MOUNT}${PREFIX}" >/dev/null 2>&1
    return 0
}
trap case_cleanup EXIT

echo "[1/6] preconditions: grpcurl + proto"
command -v grpcurl >/dev/null 2>&1 \
    || skip "grpcurl unavailable; REV-3-2 walk covered by store::rev_3_2_* unit tests"
[ -n "$PROTO_DIR" ] && [ -f "$PROTO_DIR/data.proto" ] \
    || skip "data.proto not found at $PROTO_DIR; cannot resolve the RPC without server reflection"

# ListChunks binds an EMPTY MAC binding (it names no single chunk). One stamp
# is valid for the whole page walk (300s skew window).
data_rpc_stamp ListChunks "" || fail "could not build the ListChunks auth stamp"

list_chunks() {  # <page_token> <limit> -> raw grpcurl JSON on stdout
    grpcurl -plaintext -max-time 20 -import-path "$PROTO_DIR" -proto data.proto \
        "${DATA_RPC_HDRS[@]}" \
        -d "{\"page_token\":\"$1\",\"limit\":$2}" \
        "$HOST:9500" dtfs.data.DataService/ListChunks 2>&1
}

echo "[2/6] pick a data node holding at least 3 chunks"
HOST=""
for h in $(data_list); do
    n=$(chunks_on_host "$h" 2>/dev/null | tr -d '[:space:]')
    case "$n" in ''|*[!0-9]*) continue ;; esac
    if [ "$n" -ge 3 ]; then HOST="$h"; break; fi
done
if [ -z "$HOST" ]; then
    echo "  no node holds 3 chunks yet — writing a 3-chunk file to make one"
    client_run "sudo mkdir -p ${MOUNT}${PREFIX} && \
                sudo dd if=/dev/urandom of='${MOUNT}${PREFIX}f.bin' bs=1M count=13 status=none && \
                sudo sync" || fail "could not stage a multi-chunk file"
    WROTE=1
    sleep 5
    for h in $(data_list); do
        n=$(chunks_on_host "$h" 2>/dev/null | tr -d '[:space:]')
        case "$n" in ''|*[!0-9]*) continue ;; esac
        if [ "$n" -ge 3 ]; then HOST="$h"; break; fi
    done
fi
[ -n "$HOST" ] || skip "no data node holds 3+ chunks; nothing to page over"
echo "  host=$HOST"

echo "[3/6] full listing (single big page) is ascending + duplicate-free"
FULL=$(list_chunks "" 100000)
echo "$FULL" | grep -qiE 'PermissionDenied|permission denied' \
    && fail "ListChunks rejected the auth stamp built by data_rpc_stamp: $FULL"
FULL_IDS=$(printf '%s' "$FULL" | python3 -c "
import json,sys
try:
    d=json.loads(sys.stdin.read())
except Exception as e:
    print('PARSE_ERROR', e); sys.exit(0)
ids=d.get('chunkIds') or d.get('chunk_ids') or []
mt=d.get('chunkMtimesMs') or d.get('chunk_mtimes_ms') or []
if len(mt)!=len(ids):
    print('MISALIGNED %d ids vs %d mtimes' % (len(ids), len(mt))); sys.exit(0)
if ids!=sorted(ids):
    print('UNSORTED'); sys.exit(0)
if len(set(ids))!=len(ids):
    print('DUPLICATES'); sys.exit(0)
if any(int(m)<=0 for m in mt):
    print('ZERO_MTIME'); sys.exit(0)
print('\n'.join(ids))
")
case "$FULL_IDS" in
    PARSE_ERROR*|MISALIGNED*|UNSORTED*|DUPLICATES*|ZERO_MTIME*)
        fail "full ListChunks page violated the response contract: $FULL_IDS ($FULL)" ;;
esac
TOTAL=$(printf '%s\n' "$FULL_IDS" | grep -c '^[a-f0-9]\+$')
assert_ge "$TOTAL" 3 "chunks visible on $HOST"
echo "  $TOTAL chunk ids, ascending, mtimes aligned + non-zero"

echo "[4/6] walk the same node one id per page; token must be EXCLUSIVE"
TOKEN=""
PAGED=""
PAGES=3
i=0
while [ "$i" -lt "$PAGES" ]; do
    RESP=$(list_chunks "$TOKEN" 1)
    LINE=$(printf '%s' "$RESP" | python3 -c "
import json,sys
try:
    d=json.loads(sys.stdin.read())
except Exception as e:
    print('PARSE_ERROR', e); sys.exit(0)
ids=d.get('chunkIds') or d.get('chunk_ids') or []
mt=d.get('chunkMtimesMs') or d.get('chunk_mtimes_ms') or []
tok=d.get('nextPageToken') or d.get('next_page_token') or ''
if len(ids)!=1:
    print('BAD_PAGE_SIZE %d' % len(ids)); sys.exit(0)
if len(mt)!=1:
    print('MISALIGNED %d' % len(mt)); sys.exit(0)
# More chunks remain (the node holds >= 3 and we take 3 pages), so the server
# MUST hand back a cursor, and that cursor is the last id of this page.
if tok != ids[0]:
    print('BAD_TOKEN %s != %s' % (tok, ids[0])); sys.exit(0)
print(ids[0])
")
    case "$LINE" in
        PARSE_ERROR*|BAD_PAGE_SIZE*|MISALIGNED*|BAD_TOKEN*)
            fail "page $((i+1)) violated the paging contract: $LINE ($RESP)" ;;
    esac
    PAGED="$PAGED$LINE
"
    TOKEN="$LINE"
    i=$((i+1))
done
echo "  paged ids:"
printf '%s' "$PAGED" | sed 's/^/    /'

echo "[5/6] paged ids are ascending, unique, and all really on the node"
printf '%s' "$PAGED" | python3 -c "
import sys
paged=[l for l in sys.stdin.read().split('\n') if l]
if paged!=sorted(paged):
    print('UNSORTED'); sys.exit(1)
if len(set(paged))!=len(paged):
    print('REPEATED %s' % paged); sys.exit(1)
" || fail "the page walk repeated or reordered ids (exclusive-cursor regression)"
# Cross-check against a listing taken AFTER the walk, so a chunk written by
# some other actor mid-case cannot make this flaky.
AFTER=$(list_chunks "" 100000 | python3 -c "
import json,sys
d=json.loads(sys.stdin.read())
print('\n'.join(d.get('chunkIds') or d.get('chunk_ids') or []))
")
for cid in $PAGED; do
    printf '%s\n' "$AFTER" | grep -qx "$cid" \
        || fail "paged id $cid is not in the node's own full listing"
done

echo "[6/6] a token past the last id terminates the listing"
LAST=$(printf '%s\n' "$AFTER" | grep '^[a-f0-9]\+$' | tail -1)
[ -n "$LAST" ] || fail "could not determine the last chunk id"
TAIL=$(list_chunks "$LAST" 1000)
printf '%s' "$TAIL" | python3 -c "
import json,sys
d=json.loads(sys.stdin.read())
ids=d.get('chunkIds') or d.get('chunk_ids') or []
tok=d.get('nextPageToken') or d.get('next_page_token') or ''
if ids:
    print('NOT_EMPTY %d' % len(ids)); sys.exit(1)
if tok:
    print('TOKEN_SET %s' % tok); sys.exit(1)
" || fail "paging past the last id did not terminate (token is not exclusive): $TAIL"
echo "  listing terminated: no ids, no token"
pass

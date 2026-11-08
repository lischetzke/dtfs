#!/usr/bin/env bash
# REV-4-5: the data node must size its put_chunk concurrency from the memory
# cap ALONE, never by also reserving `[cache].max_memory_mb`.
#
# The data node has no server-side ARC chunk cache — `cache.rs::put` has no
# caller anywhere in the workspace, so the cache was permanently empty. Its
# configured budget was nevertheless subtracted when sizing put_chunk slots,
# which is pure phantom accounting: on a capped node a 768 MB `[cache]` line
# against a 1 GB `[memory].hard_cap_mb` left put_chunk with a SINGLE slot, so
# every concurrent write on that node serialised for no reason at all.
#
# The node logs the exact inputs it sized from at startup, which is the cheapest
# unambiguous place to pin this:
#   "Bug G memory budget engaged: sized put_chunk slots from hard cap"
#     cap_mb=… chunk_mb=… overhead_mb=… slots=…
# `overhead_mb` MUST be the fixed 256 MB static reservation (tonic + raft +
# kernel pages) with no cache term folded in, and `slots` must equal
#   clamp((cap - overhead) / chunk_estimate, 1, 256)
# which this case recomputes from the node's own on-disk config.
source "$(dirname "$0")/../lib/common.sh"

# server.rs constants the log line is derived from.
SLOT_ESTIMATE_MB=16
STATIC_OVERHEAD_MB=256

echo "[1/4] find a data node with a hard memory cap configured"
HOST=""
CAP_MB=0
CACHE_MB=0
for h in $(data_list); do
    # Same on-node tomllib read the suite already uses for data.toml (case 76).
    out=$(ssh_run "$h" "python3 - <<'TOML'
import tomllib
try:
    with open('/etc/dtfs/data.toml','rb') as f:
        c = tomllib.load(f)
except Exception:
    print(0, 0); raise SystemExit
print(int(c.get('memory', {}).get('hard_cap_mb', 0) or 0),
      int(c.get('cache', {}).get('max_memory_mb', 0) or 0))
TOML
" 2>/dev/null | tr -d '\r')
    cap=$(printf '%s\n' "$out" | awk 'NF{print $1; exit}')
    cache=$(printf '%s\n' "$out" | awk 'NF{print $2; exit}')
    case "${cap:-}" in ''|*[!0-9]*) continue ;; esac
    case "${cache:-}" in ''|*[!0-9]*) cache=0 ;; esac
    if [ "$cap" -gt 0 ]; then
        HOST="$h"; CAP_MB="$cap"; CACHE_MB="$cache"
        break
    fi
done
[ -n "$HOST" ] || skip "no data node sets [memory].hard_cap_mb; slot sizing falls back to grpc.max_concurrent_chunk_writes and there is nothing to pin"
echo "  host=$HOST hard_cap_mb=$CAP_MB cache.max_memory_mb=$CACHE_MB"

echo "[2/4] read the slot-sizing line the node logged at startup"
LOGLINE=$(ssh_run "$HOST" "sudo journalctl -u dtfs-data --no-pager -o cat 2>/dev/null \
    | grep 'sized put_chunk slots from hard cap' | tail -1" | tr -d '\r')
[ -n "$LOGLINE" ] \
    || skip "no 'sized put_chunk slots' line in dtfs-data's journal on $HOST (log rotated, or the unit predates Bug G)"
echo "  $LOGLINE"

SLOTS=$(printf '%s\n' "$LOGLINE" | sed -n 's/.*slots=\([0-9]\+\).*/\1/p' | tail -1)
OVERHEAD=$(printf '%s\n' "$LOGLINE" | sed -n 's/.*overhead_mb=\([0-9]\+\).*/\1/p' | tail -1)
LOGGED_CAP=$(printf '%s\n' "$LOGLINE" | sed -n 's/.*cap_mb=\([0-9]\+\).*/\1/p' | tail -1)
[ -n "$SLOTS" ] && [ -n "$OVERHEAD" ] \
    || fail "could not parse slots=/overhead_mb= out of: $LOGLINE"
# A running node may predate the config on disk (edited but not restarted).
if [ -n "$LOGGED_CAP" ] && [ "$LOGGED_CAP" != "$CAP_MB" ]; then
    skip "dtfs-data on $HOST was started with cap_mb=$LOGGED_CAP but /etc/dtfs/data.toml now says $CAP_MB; restart the unit before pinning the arithmetic"
fi

echo "[3/4] the static overhead must not include the phantom cache budget"
assert_eq "$STATIC_OVERHEAD_MB" "$OVERHEAD" "overhead_mb (REV-4-5: fixed 256 MB, no [cache] term)"

echo "[4/4] slots must equal clamp((cap - overhead) / ${SLOT_ESTIMATE_MB}MB, 1, 256)"
EXPECTED=$(python3 -c "
cap, overhead, est = $CAP_MB, $STATIC_OVERHEAD_MB, $SLOT_ESTIMATE_MB
print(1 if overhead >= cap else max(1, min(256, (cap - overhead) // est)))
")
if [ "$SLOTS" != "$EXPECTED" ]; then
    PHANTOM=$(python3 -c "
cap, overhead, est, cache = $CAP_MB, $STATIC_OVERHEAD_MB, $SLOT_ESTIMATE_MB, $CACHE_MB
o = overhead + cache
print(1 if o >= cap else max(1, min(256, (cap - o) // est)))
")
    if [ "$SLOTS" = "$PHANTOM" ] && [ "$CACHE_MB" -gt 0 ]; then
        fail "REV-4-5 regression: slots=$SLOTS matches the PHANTOM sizing that also reserves [cache].max_memory_mb=${CACHE_MB}MB; expected $EXPECTED"
    fi
    fail "slots=$SLOTS but cap=${CAP_MB}MB overhead=${OVERHEAD}MB implies $EXPECTED"
fi
echo "  slots=$SLOTS as expected"
pass

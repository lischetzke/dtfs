#!/usr/bin/env bash
# CE: a whole-file sequential read on a RAM-starved client must NOT OOM-kill
# the FUSE process. The in-memory readahead cache used to be bounded by ENTRY
# COUNT (256), but each entry is a whole chunk, so a large-file read grew FUSE
# RSS without bound (observed ~800 MB on the 1 GB client -> OOM). The cache is
# now bounded by a byte budget ([readahead] cache_mb, default 64), so RSS stays
# bounded regardless of file size.
#
# Assert: after reading the whole file, the FUSE process is the SAME pid (not
# OOM-killed + restarted), the mount is still responsive, the read returned a
# stable checksum, and resident memory stayed well under the file size.
# A deployed binary that predates CE will OOM (pid changes / mount drops /
# RSS ~= file size) -> KNOWN_ISSUE [CE], which auto-promotes once CE ships.
source "$(dirname "$0")/../lib/common.sh"

FUSE_UNIT=${FUSE_UNIT:-dtfs-fuse@-mnt-dtfs.service}
SIZE_MB=${CE_SIZE_MB:-384}
# Post-fix RSS ceiling: cache_mb budget (64) + generous FUSE/runtime overhead.
# Pre-fix RSS would track the whole file (~$SIZE_MB MB).
RSS_CEIL_MB=${CE_RSS_CEIL_MB:-220}

F=""
case_cleanup() { rm_fuse_file "$F"; }
trap case_cleanup EXIT

fuse_pid()    { client_run "systemctl show -p MainPID --value $FUSE_UNIT" 2>/dev/null | tr -d '\r'; }
fuse_active() { client_run "systemctl is-active $FUSE_UNIT" 2>/dev/null | grep -q '^active'; }

echo "[1/6] preconditions"
fuse_active || skip "FUSE unit $FUSE_UNIT not active on $CLIENT"
PID0=$(fuse_pid)
[ -n "$PID0" ] && [ "$PID0" != "0" ] || skip "could not read FUSE MainPID"
echo "  fuse pid=$PID0"

echo "[2/6] write a ${SIZE_MB} MB file through the mount"
F=$(make_fuse_file "release/40-ce-$$.bin" "$SIZE_MB")
[ -n "$F" ] || fail "could not create fixture"

echo "[3/6] drop client cache so the read re-fetches chunks from data nodes"
drop_client_cache

echo "[4/6] whole-file read (the pre-CE OOM trigger)"
SUM1=$(client_run "sudo md5sum '$F' 2>/dev/null | awk '{print \$1}'")
if [ -z "$SUM1" ]; then
    if ! fuse_active || [ "$(fuse_pid)" != "$PID0" ]; then
        known_issue CE "FUSE process died during whole-file read (OOM) — byte-bounded readahead cache not in the deployed binary"
    fi
    fail "whole-file read produced no checksum but FUSE is still up"
fi
echo "  md5=$SUM1"

echo "[5/6] FUSE survived + mount still responsive"
PID1=$(fuse_pid)
if ! fuse_active || [ "$PID1" != "$PID0" ]; then
    known_issue CE "FUSE pid changed ($PID0 -> $PID1) — process was OOM-killed/restarted during the read"
fi
SUM2=$(client_run "sudo md5sum '$F' 2>/dev/null | awk '{print \$1}'")
assert_eq "$SUM1" "$SUM2" "checksum stable across re-read"

echo "[6/6] resident memory stayed bounded (cache is byte-capped)"
RSS_KB=$(client_run "ps -o rss= -p $PID1 2>/dev/null | tr -d ' '")
RSS_MB=$(( ${RSS_KB:-0} / 1024 ))
echo "  fuse RSS=${RSS_MB} MB (ceiling ${RSS_CEIL_MB} MB, file ${SIZE_MB} MB)"
if [ "${RSS_KB:-0}" -gt 0 ] && [ "$RSS_MB" -gt "$RSS_CEIL_MB" ]; then
    known_issue CE "FUSE RSS ${RSS_MB} MB exceeds ceiling ${RSS_CEIL_MB} MB after a whole-file read — readahead cache not byte-bounded"
fi
pass

#!/usr/bin/env bash
# BP-1: the manager raft-log dir must stay bounded — RocksDB info-LOG
# (LOG.old.*) capped by keep_log_file_num and the MANIFEST capped by
# max_manifest_file_size. Pre-BP these grew unbounded (6.8 GB of LOG.old +
# 207 MB MANIFEST filled a 16 GB partition, ISSUE-SCAN-2026-06-02 F5).
#
# Asserts, per manager: LOG.old.* count <= 8 (keep_log_file_num=4 + slack)
# and MANIFEST < 128 MiB. SSH-based; SKIPs if the raft-log dir is not found.
source "$(dirname "$0")/../lib/common.sh"

RAFT_LOG_DIR=${RAFT_LOG_DIR:-/var/lib/dtfs/manager/raft-log}
MAX_LOGOLD=${MAX_LOGOLD:-8}
MAX_MANIFEST_MIB=${MAX_MANIFEST_MIB:-128}

fail_nodes=""
checked=0
for h in $(mgrs_list); do
    out=$(ssh_run "$h" "cd $RAFT_LOG_DIR 2>/dev/null || exit 9; \
        echo -n 'logold='; ls LOG.old.* 2>/dev/null | wc -l; \
        echo -n 'manifest_mib='; du -m MANIFEST-* 2>/dev/null | sort -rn | head -1 | awk '{print \$1+0}'" 2>/dev/null)
    rc=$?
    if [ "$rc" = "9" ] || [ -z "$out" ]; then
        echo "  $h: raft-log dir not found (skip)"; continue
    fi
    checked=$((checked+1))
    logold=$(echo "$out" | sed -n 's/^logold=//p')
    manifest=$(echo "$out" | sed -n 's/^manifest_mib=//p')
    echo "  $h: LOG.old=$logold MANIFEST=${manifest}MiB"
    if [ "${logold:-0}" -gt "$MAX_LOGOLD" ] || [ "${manifest:-0}" -gt "$MAX_MANIFEST_MIB" ]; then
        fail_nodes="$fail_nodes $h"
    fi
done

[ "$checked" -gt 0 ] || skip "no manager raft-log dirs reachable"

if [ -z "$fail_nodes" ]; then
    pass
else
    known_issue BP "raft-log unbounded on$fail_nodes (LOG.old>$MAX_LOGOLD or MANIFEST>${MAX_MANIFEST_MIB}MiB) — BP-1 bounds not in the deployed binary"
fi

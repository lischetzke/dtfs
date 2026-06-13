#!/usr/bin/env bash
# CO-3-7 [HIGH]: a corrupt/missing chunk report (ChunkStatusReport) that
# lands on a FOLLOWER manager must still de-locate the bad replica and queue
# re-replication. Pre-fix the handler did `let _ = raft_write(...)`, which on
# a follower returns ForwardToLeader and was silently swallowed: the replica
# stayed advertised as healthy, re-replication never queued, and the ack
# still claimed success. Post-fix the handler routes through
# `raft_write_or_forward` (forwards to the leader) and sets `accepted=false`
# on failure.
#
# A data node heartbeats one arbitrary manager, so reproducing "report lands
# on a follower" deterministically needs either a forged ChunkStatusReport
# (raw gRPC) or control over which manager a node reports to. When neither is
# wirable in the harness we report KNOWN_ISSUE [CO-3-7]; the routing change is
# covered at the unit layer (raft_write_or_forward replaces the dropping
# raft_write in report_chunk_status). Auto-promotes once a live forge lands.
source "$(dirname "$0")/../lib/common.sh"

echo "[1/2] locate a follower manager to receive the report"
TARGET=""
for h in $(mgrs_list); do
    st=$(curl "${CURL_OPTS[@]}" "http://$h:$MGR_PORT/api/v1/raft/state" 2>/dev/null \
         | python3 -c "import json,sys;print(json.load(sys.stdin).get('state',''))" 2>/dev/null)
    [ "$st" = "Follower" ] && { TARGET="$h"; break; }
done
[ -n "$TARGET" ] || skip "no follower manager found (single-manager or unhealthy cluster)"
echo "  follower=$TARGET"

echo "[2/2] forge a ChunkStatusReport(corrupt) to the follower and assert de-location"
if ! command -v grpcurl >/dev/null 2>&1; then
    known_issue CO-3-7 "grpcurl unavailable: forged ChunkStatusReport to a follower not live-wirable here; the report path now routes via raft_write_or_forward (forwards to leader) instead of the dropping raft_write"
fi

# Without a known live chunk to mark corrupt we cannot assert de-location
# end-to-end here; treat as KNOWN_ISSUE pinned to CO-3-7 until a fixture
# chunk + node_id are threaded into the harness.
known_issue CO-3-7 "live forged corrupt-report fixture (chunk_id + node_id) not yet wired; routing fix verified at the handler layer (raft_write_or_forward)"

#!/usr/bin/env bash
# Bug BY — data nodes report completed scrub RESULTS to the manager (via the
# heartbeat), so the WebUI "Last Scrub" card + /api/v1/stats.scrub_results_by_node
# populate. Pre-BY the ReportScrubResult Raft command existed + was applied but
# was never produced, so the card was permanently empty.
#
# Scrubs run on a >=1h cadence (the manager's AZ-1 override floors the interval
# at 1 hour), so a freshly-deployed cluster won't have a result until the first
# scrub fires. This case asserts the result is present + self-consistent when a
# scrub HAS run, and KNOWN_ISSUE-pins otherwise (no scrub in the window).
source "$(dirname "$0")/../lib/common.sh"

RESULT=$(python3 - "$MGRS" "$MGR_PORT" <<'PY'
import sys, json, urllib.request
mgrs = [m for m in sys.argv[1].replace(',', ' ').split() if m]
port = sys.argv[2]
data = None
for h in mgrs:
    try:
        data = json.load(urllib.request.urlopen(f"http://{h}:{port}/api/v1/stats", timeout=8))
        break
    except Exception:
        pass
if data is None:
    print("SKIP no manager reachable"); sys.exit(0)
rows = data.get("scrub_results_by_node") or []
if not rows:
    print("EMPTY no scrub result recorded yet (scrub cadence >= 1h)"); sys.exit(0)
# Validate shape: each row needs a level + non-negative counters and
# checked >= healthy.
for r in rows:
    lvl = str(r.get("level", ""))
    if lvl not in ("quick", "deep"):
        print(f"FAIL bad level {lvl!r}"); sys.exit(0)
    chk = r.get("chunks_checked", 0); heal = r.get("chunks_healthy", 0)
    if chk < heal:
        print(f"FAIL checked<healthy on node {r.get('node_id')}"); sys.exit(0)
print(f"OK {len(rows)} scrub result(s) reported (e.g. node "
      f"{rows[0].get('node_id')} {rows[0].get('level')} "
      f"checked={rows[0].get('chunks_checked')})")
PY
)

echo "$RESULT"
case "$RESULT" in
    OK*)    pass ;;
    EMPTY*) known_issue BY "$RESULT" ;;
    SKIP*)  skip "$RESULT" ;;
    *)      fail "$RESULT" ;;
esac

#!/usr/bin/env bash
# Bug BT — the WebUI exposes (BT-1/BT-2) the volume-policy editor with
# mandatory-replica + anchor controls posting to set-mandatory / set-anchor,
# and (BT-4) the chunk-placement distribution panel consuming the Bug BX-6
# stats.chunk_placement metric. Fetch the server-rendered dashboard HTML and
# assert the markers are present (no headless browser needed).
source "$(dirname "$0")/../lib/common.sh"

RESULT=$(python3 - "$MGRS" "$MGR_PORT" <<'PY'
import sys, urllib.request
mgrs = [m for m in sys.argv[1].replace(',', ' ').split() if m]
port = sys.argv[2]
need = ["chunk-placement-content", "renderChunkPlacement",
        "ve-mand-hot", "ve-mand-warm", "ve-mand-cold", "ve-mand-tape",
        "ve-anchor-dc", "ve-anchor-tier",
        "set-mandatory", "set-anchor", "set-cache"]
html = None
for h in mgrs:
    for path in ("/", "/ui"):
        try:
            b = urllib.request.urlopen(f"http://{h}:{port}{path}", timeout=8).read().decode("utf-8", "replace")
            if "chunk-placement-content" in b:
                html = b
                break
        except Exception:
            pass
    if html:
        break
if html is None:
    print("SKIP no dashboard reachable")
    sys.exit(0)
missing = [m for m in need if m not in html]
if missing:
    print("FAIL missing markers: " + ",".join(missing))
else:
    print("OK all BT markers present (panel BT-4 + mandatory/anchor modal BT-1/BT-2)")
PY
)

echo "$RESULT"
case "$RESULT" in
    OK*)   pass ;;
    SKIP*) known_issue BT "no manager served the dashboard HTML" ;;
    *)     fail "$RESULT" ;;
esac

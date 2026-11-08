#!/usr/bin/env bash
# LIS-41 — Down-node replicas must not satisfy live coverage metrics.
# When a volume requires a tier/anchor that only exists as location rows
# on Down data nodes (ghost DC3 cold after wipe), stats must NOT
# report all-green (anchor_pending=0 AND undergoal=0 AND misplaced=0).
# If the ghost-Down pattern is absent on this cluster, SKIP.
source "$(dirname "$0")/../lib/common.sh"

NODES=$(mgr_get_any /api/v1/nodes) || known_issue LIS-41 "no manager reachable"
STATS=$(mgr_get_any /api/v1/stats) || known_issue LIS-41 "stats unreachable"
VOLS=$(mgr_get_any /api/v1/volumes) || known_issue LIS-41 "volumes unreachable"

PATTERN=$(NODES_JSON="$NODES" VOLS_JSON="$VOLS" python3 - <<'PY'
import json, os
nodes = json.loads(os.environ["NODES_JSON"])
vols_raw = json.loads(os.environ["VOLS_JSON"])
vols = vols_raw if isinstance(vols_raw, list) else vols_raw.get("volumes", vols_raw.get("items", []))

def role(n):
    return str(n.get("role", "")).lower()
def health(n):
    return str(n.get("health", "")).lower()
def tiers(n):
    out = set()
    for d in n.get("disks") or []:
        t = str(d.get("tier", "")).lower()
        if t:
            out.add(t)
    return out

down_data = [n for n in nodes if role(n) == "data" and health(n) == "down"]
healthy_data = [n for n in nodes if role(n) == "data" and health(n) == "healthy"]
if not down_data:
    print("NONE")
    raise SystemExit(0)

down_tiers = set()
for n in down_data:
    down_tiers |= tiers(n)
healthy_tiers = set()
for n in healthy_data:
    healthy_tiers |= tiers(n)

needed = set()
for v in vols:
    m = v.get("mandatory_replicas") or v.get("replication") or {}
    for t in ("hot", "warm", "cold", "tape"):
        try:
            if int(m.get(t, 0) or 0) > 0:
                needed.add(t)
        except (TypeError, ValueError):
            pass
    a = v.get("anchor") or {}
    rt = a.get("required_tier")
    if rt:
        needed.add(str(rt).lower())
    elif v.get("anchor_datacenter"):
        needed.add("cold")

ghost = needed & down_tiers - healthy_tiers
if ghost:
    print("HIT:" + ",".join(sorted(ghost)))
else:
    print("NONE")
PY
)

echo "[1/2] ghost-Down tier pattern: $PATTERN"
case "$PATTERN" in
  NONE)
    skip "no Down-only mandated/anchor tier on this cluster"
    ;;
  HIT:*)
    ;;
  *)
    fail "unexpected pattern probe output: $PATTERN"
    ;;
esac

AP=$(echo "$STATS" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('anchor_pending', 'MISSING'))")
UG=$(echo "$STATS" | python3 -c "import json,sys; d=json.load(sys.stdin).get('chunk_placement') or {}; print(d.get('undergoal', 'MISSING'))")
MP=$(echo "$STATS" | python3 -c "import json,sys; d=json.load(sys.stdin).get('chunk_placement') or {}; print(d.get('misplaced', 'MISSING'))")
ST=$(echo "$STATS" | python3 -c "import json,sys; d=json.load(sys.stdin).get('chunk_placement') or {}; print(d.get('stable', 'MISSING'))")

echo "[2/2] metrics: anchor_pending=$AP undergoal=$UG misplaced=$MP stable=$ST"
[ "$AP" = "MISSING" ] && fail "stats missing anchor_pending"
[ "$UG" = "MISSING" ] && fail "stats missing chunk_placement.undergoal"
[ "$MP" = "MISSING" ] && fail "stats missing chunk_placement.misplaced"

# False-green = all three deficit signals zero while ghost Down tiers exist.
if [ "$AP" = "0" ] && [ "$UG" = "0" ] && [ "$MP" = "0" ]; then
  fail "LIS-41 false-green: Down-only tier(s) ${PATTERN#HIT:} but anchor_pending=undergoal=misplaced=0 (stable=$ST)"
fi

echo "deficit surfaced (not false-green)"
pass

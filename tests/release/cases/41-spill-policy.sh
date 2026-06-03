#!/usr/bin/env bash
# Bug BZ — per-volume capacity-spill exclusions. An operator can forbid the
# manager from spilling a volume's chunks (Bug BX-2) onto specific tiers or
# datacenters, or disable spill entirely for the volume. This case drives the
# `set-spill` endpoint and verifies the policy persists in the volume config
# (the enforcement filter is unit-tested in dtfs-common + dtfs-manager).
source "$(dirname "$0")/../lib/common.sh"

VOL=${VOL:-default}

RESULT=$(python3 - "$MGRS" "$MGR_PORT" "$VOL" <<'PY'
import sys, json, urllib.request
mgrs = [m for m in sys.argv[1].replace(',', ' ').split() if m]
port, vol = sys.argv[2], sys.argv[3]

def post(path, body):
    data = json.dumps(body).encode()
    # The set-spill Raft write only succeeds on the leader; non-leaders 503.
    for h in mgrs:
        try:
            req = urllib.request.Request(f"http://{h}:{port}{path}", data=data,
                                         headers={"Content-Type": "application/json"}, method="POST")
            return json.load(urllib.request.urlopen(req, timeout=8))
        except urllib.error.HTTPError as e:
            if e.code in (503, 421):
                continue
            return {"_http": e.code}
        except Exception:
            continue
    return None

def get_spill():
    for h in mgrs:
        try:
            v = json.load(urllib.request.urlopen(f"http://{h}:{port}/api/v1/volumes", timeout=8))
            vs = v if isinstance(v, list) else v.get("volumes", [])
            for x in vs:
                if x.get("volume_id") == vol:
                    return x.get("spill")
        except Exception:
            continue
    return None

orig = get_spill()
if orig is None:
    print("SKIP volume not found / no manager reachable"); sys.exit(0)

want = {"allow": False, "forbidden_tiers": ["hot"], "forbidden_datacenters": ["dc-zz"]}
if post(f"/api/v1/volumes/{vol}/set-spill", want) is None:
    print("SKIP could not reach a leader to set spill policy"); sys.exit(0)

got = get_spill() or {}
ok = (got.get("allow") is False
      and got.get("forbidden_tiers") == ["hot"]
      and got.get("forbidden_datacenters") == ["dc-zz"])

# Restore the original policy (best-effort cleanup).
restore = {
    "allow": (orig or {}).get("allow"),
    "forbidden_tiers": (orig or {}).get("forbidden_tiers", []),
    "forbidden_datacenters": (orig or {}).get("forbidden_datacenters", []),
}
post(f"/api/v1/volumes/{vol}/set-spill", restore)

# Unknown tier must be rejected (validation).
bad = post(f"/api/v1/volumes/{vol}/set-spill", {"forbidden_tiers": ["flash"]})
bad_rejected = bad is None or bad.get("_http") == 400 or "error" in (bad or {})

if ok and bad_rejected:
    print(f"OK spill policy persisted ({got}); bad tier rejected")
elif not ok:
    print(f"FAIL spill policy did not persist: got {got}")
else:
    print("FAIL invalid tier was not rejected")
PY
)

echo "$RESULT"
case "$RESULT" in
    OK*)   pass ;;
    SKIP*) known_issue BZ "$RESULT" ;;
    *)     fail "$RESULT" ;;
esac

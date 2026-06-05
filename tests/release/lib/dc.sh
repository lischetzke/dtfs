# tests/release/lib/dc.sh — datacenter / topology helpers for the chaos +
# cross-DC + balance cases. Pure SSH/API + inventory JSON; NO hypervisor
# access (see lib/pve.sh for that), so cases 46/47 use this without
# ALLOW_PVE_CHAOS. Depends on common.sh (mgr_get_any, mgrs_list, MGR_PORT,
# CURL_OPTS) and health.sh (mgr_leader).
#
# Topology source of truth: tests/proxmox/inventory-3dc.json
# (vms[].{name,vmid,ip,dc,role,extra_disks}). Override with INVENTORY_JSON.

_DC_LIBDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INVENTORY_JSON=${INVENTORY_JSON:-$_DC_LIBDIR/../../proxmox/inventory-3dc.json}

# ─── Inventory lookups (offline; read the JSON) ──────────────────
_inv_vm_field() {  # <host> <field> -> field value (empty if not found)
    INV="$INVENTORY_JSON" python3 - "$1" "$2" <<'PY'
import json, os, sys
inv = json.load(open(os.environ["INV"]))
host, field = sys.argv[1], sys.argv[2]
for v in inv.get("vms", []):
    if v.get("name") == host:
        print(v.get(field, ""))
        break
PY
}
dc_of_host()   { _inv_vm_field "$1" dc; }
vmid_of_host() { _inv_vm_field "$1" vmid; }
ip_of_host()   { _inv_vm_field "$1" ip; }
role_of_host() { _inv_vm_field "$1" role; }

_inv_names_in_dc() {  # <dc> [role] -> names, one per line
    INV="$INVENTORY_JSON" python3 - "$1" "${2:-}" <<'PY'
import json, os, sys
inv = json.load(open(os.environ["INV"]))
dc = sys.argv[1]
role = sys.argv[2] if len(sys.argv) > 2 else ""
for v in inv.get("vms", []):
    if v.get("dc") == dc and (not role or v.get("role") == role):
        print(v.get("name"))
PY
}
hosts_in_dc()      { _inv_names_in_dc "$1"; }
data_hosts_in_dc() { _inv_names_in_dc "$1" data; }
client_for_dc()    { _inv_names_in_dc "$1" client | head -1; }
mgr_for_dc()       { _inv_names_in_dc "$1" manager | head -1; }

# ─── Live cluster lookups (hit the manager API) ──────────────────

# Map a hostname to the numeric node_id the manager assigned (matches the
# ids used in /api/v1/files/inspect). Empty if the host isn't registered.
hostname_to_node_id() {
    mgr_get_any /api/v1/nodes | INV_HOST="$1" python3 -c "
import json, os, sys
d = json.load(sys.stdin); h = os.environ['INV_HOST']
for n in d:
    if n.get('hostname') == h:
        print(n.get('node_id', '')); break
"
}

# Cumulative bytes a data node has served (Bug REL-1-1). -1 if the host is
# unknown; 0 if it has served nothing.
served_bytes_for_node() {
    mgr_get_any /api/v1/nodes | INV_HOST="$1" python3 -c "
import json, os, sys
d = json.load(sys.stdin); h = os.environ['INV_HOST']
for n in d:
    if n.get('hostname') == h:
        print(int(n.get('total_bytes_read', 0) or 0)); break
else:
    print(-1)
"
}

# Predicate: /api/v1/nodes rows carry total_bytes_read (REL-1-1 deployed).
# Lets case 42 KNOWN_ISSUE gracefully against an older manager.
nodes_expose_io() {
    mgr_get_any /api/v1/nodes | python3 -c "
import json, sys
d = json.load(sys.stdin)
sys.exit(0 if any('total_bytes_read' in n for n in d) else 1)
"
}

# Emit one row per replica of a path: "chunk_index node_id disk_id tier dc host".
# Prefers inspect's per-replica array (REL-1-2: node_id+disk_id+tier); falls
# back to bare node_ids (tier resolved from the node's single disk, else '?').
_replica_rows() {
    local path="$1" insp nodes
    insp=$(mgr_get_any "/api/v1/files/inspect?path=$path") || return 1
    nodes=$(mgr_get_any /api/v1/nodes) || return 1
    INV="$INVENTORY_JSON" python3 - "$insp" "$nodes" <<'PY'
import json, os, sys
insp = json.loads(sys.argv[1])
nodes = json.loads(sys.argv[2])
inv = json.load(open(os.environ["INV"]))
id2host = {str(n.get("node_id")): n.get("hostname", "") for n in nodes}
id2disktier = {}
for n in nodes:
    id2disktier[str(n.get("node_id"))] = {
        str(d.get("disk_id")): str(d.get("tier", "")).lower() for d in n.get("disks", [])
    }
host2dc = {v["name"]: v.get("dc", "?") for v in inv.get("vms", [])}
if isinstance(insp, dict) and "error" in insp:
    sys.exit(0)
for c in insp.get("chunks", []):
    idx = c.get("chunk_index")
    reps = c.get("replicas")
    if reps is None:
        reps = [{"node_id": nid} for nid in c.get("node_ids", [])]
    for r in reps:
        nid = str(r.get("node_id"))
        disk = "" if r.get("disk_id") is None else str(r.get("disk_id"))
        tier = str(r.get("tier", "")).lower()
        if not tier:
            dm = id2disktier.get(nid, {})
            if disk and disk in dm:
                tier = dm[disk]
            elif len(dm) == 1:
                tier = next(iter(dm.values()))
        host = id2host.get(nid, "")
        dc = host2dc.get(host, "?")
        print(f"{idx} {nid} {disk} {tier or '?'} {dc} {host}")
PY
}

# Predicate: inspect emits the per-replica array (REL-1-2 deployed) so tier is
# trustworthy even on multi-disk nodes. Lets cases KNOWN_ISSUE on old managers.
inspect_has_replicas() {
    mgr_get_any "/api/v1/files/inspect?path=$1" | python3 -c "
import json, sys
d = json.load(sys.stdin)
cs = d.get('chunks', []) if isinstance(d, dict) else []
sys.exit(0 if cs and all('replicas' in c for c in cs) else 1)
" 2>/dev/null
}

# Count replicas of <path> in a given <dc> (any tier).
count_replicas_in_dc() {
    _replica_rows "$1" | awk -v d="$2" '$5==d {c++} END{print c+0}'
}
# Count replicas of <path> in a given <dc> AND <tier>.
count_replicas_in_dc_tier() {
    _replica_rows "$1" | awk -v d="$2" -v t="$3" '$5==d && $4==t {c++} END{print c+0}'
}
# Count replicas of <path> on a given <tier> across all DCs.
count_replicas_in_tier() {
    _replica_rows "$1" | awk -v t="$2" '$4==t {c++} END{print c+0}'
}

# Predicate: a leader is elected AND at least 2 managers answer the API
# (quorum survives a single-DC outage on the 3dc topology).
quorum_ok() {
    local leader h cnt=0
    leader=$(mgr_leader) || return 1
    [ -n "$leader" ] || return 1
    for h in $(mgrs_list); do
        curl "${CURL_OPTS[@]}" "http://$h:$MGR_PORT/api/v1/nodes" >/dev/null 2>&1 && cnt=$((cnt + 1))
    done
    [ "$cnt" -ge 2 ]
}

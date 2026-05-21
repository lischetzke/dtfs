#!/usr/bin/env bash
# Provision the 6-VM minicluster from inventory-mini.json.
#
# Destructive: stops + destroys any VM in range 301..306 first. Preserves
# template (9000) + builder (9001). Idempotent — safe to re-run.
#
# Run from a workstation with curl + jq; requires the PVE API token in
# $PVE_TOKEN (default falls back to the lab token hardcoded in pve.ps1
# for convenience).

set -euo pipefail

PVE_TOKEN="${PVE_TOKEN:-root@pam!dtfs-test=631edfe4-7a29-44e9-b10b-6d0141cdbf88}"
PVE_HOST="${PVE_HOST:-10.150.0.121}"
PVE_NODE="${PVE_NODE:-i-pve01}"
BASE="https://${PVE_HOST}:8006/api2/json"
TEMPLATE_VMID=9000

INV="$(dirname "$0")/inventory-mini.json"
[ -f "$INV" ] || { echo "inventory not found: $INV"; exit 1; }

api() {
    local method=$1 path=$2 body=${3:-}
    if [ -n "$body" ]; then
        curl -sk -X "$method" -H "Authorization: PVEAPIToken=$PVE_TOKEN" \
            --data-urlencode-format json --data-urlencode "_dummy=" \
            -d "$body" "${BASE}${path}" -G 2>/dev/null
    else
        curl -sk -X "$method" -H "Authorization: PVEAPIToken=$PVE_TOKEN" "${BASE}${path}"
    fi
}

# More portable: use form-encoded POSTs instead of JSON since proxmox
# accepts both; form is simpler with curl --data.
api_form() {
    local method=$1 path=$2; shift 2
    curl -sk -X "$method" -H "Authorization: PVEAPIToken=$PVE_TOKEN" "$@" "${BASE}${path}"
}

wait_task() {
    local upid=$1 timeout=${2:-240}
    local enc deadline status
    enc=$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1], safe=''))" "$upid")
    deadline=$(( $(date +%s) + timeout ))
    while [ "$(date +%s)" -lt "$deadline" ]; do
        status=$(api_form GET "/nodes/${PVE_NODE}/tasks/${enc}/status" | python3 -c "import json,sys; d=json.load(sys.stdin).get('data',{}); print((d.get('status','running'))+'|'+(d.get('exitstatus','') or ''))")
        case "$status" in
            stopped\|OK) return 0 ;;
            stopped\|*) echo "task $upid failed: $status"; return 1 ;;
        esac
        sleep 3
    done
    echo "task $upid timed out after ${timeout}s"
    return 1
}

# Step 1: destroy any VM in [301,306].
echo "=== Step 1: destroy existing VMs in range 301..306 ==="
existing=$(api_form GET "/nodes/${PVE_NODE}/qemu" | python3 -c "
import json,sys
for vm in json.load(sys.stdin).get('data',[]):
    if 301 <= vm.get('vmid',0) <= 306:
        print(f\"{vm['vmid']}|{vm.get('status','unknown')}\")
")
while IFS='|' read -r vmid st; do
    [ -z "$vmid" ] && continue
    echo "  destroying vmid=$vmid (status=$st)"
    if [ "$st" = "running" ]; then
        upid=$(api_form POST "/nodes/${PVE_NODE}/qemu/$vmid/status/stop" | python3 -c "import json,sys; print(json.load(sys.stdin).get('data',''))")
        [ -n "$upid" ] && wait_task "$upid" 60 || true
    fi
    upid=$(api_form POST "/nodes/${PVE_NODE}/qemu/$vmid" \
        --data "purge=1" --data "destroy-unreferenced-disks=1" \
        -X DELETE | python3 -c "import json,sys; print(json.load(sys.stdin).get('data',''))")
    [ -n "$upid" ] && wait_task "$upid" 120 || true
done <<< "$existing"

# Step 2: clone + configure + start each VM.
echo
echo "=== Step 2: clone 6 VMs from template $TEMPLATE_VMID ==="
python3 -c "
import json
inv = json.load(open('$INV'))
for v in inv['vms']:
    extras = ' '.join(f\"{d['storage']}:{d['size_gb']}:{d['tier']}:{d['mount']}\" for d in v.get('extra_disks',[]))
    print(f\"{v['vmid']}|{v['name']}|{v['ip']}|{v['cores']}|{v['memory_mb']}|{v['os_disk_gb']}|{v['role']}|{v['dc']}|{v['rack']}|{extras}\")
" | while IFS='|' read -r vmid name ip cores mem osdisk role dc rack extras; do
    echo
    echo ">>> $name vmid=$vmid ip=$ip role=$role dc=$dc rack=$rack"

    # Clone
    upid=$(api_form POST "/nodes/${PVE_NODE}/qemu/$TEMPLATE_VMID/clone" \
        --data "newid=$vmid" --data "name=$name" \
        --data "target=$PVE_NODE" --data "storage=pve01_ssd01" --data "full=1" \
        | python3 -c "import json,sys; print(json.load(sys.stdin).get('data',''))")
    wait_task "$upid" 240

    # Configure cores/memory/network
    api_form PUT "/nodes/${PVE_NODE}/qemu/$vmid/config" \
        --data "cores=$cores" --data "memory=$mem" \
        --data "ipconfig0=ip=$ip/24,gw=10.150.0.1" \
        --data "nameserver=10.150.0.2 10.150.0.3" \
        > /dev/null

    # Resize OS disk
    if [ "$osdisk" -gt 3 ]; then
        api_form PUT "/nodes/${PVE_NODE}/qemu/$vmid/resize" \
            --data "disk=scsi0" --data "size=${osdisk}G" > /dev/null
    fi

    # Add extra disks (data nodes)
    if [ -n "$extras" ]; then
        slot=1
        for disk in $extras; do
            IFS=: read -r dstorage dsize dtier dmount <<< "$disk"
            echo "  + scsi$slot ${dsize}G on $dstorage tier=$dtier mount=$dmount"
            api_form PUT "/nodes/${PVE_NODE}/qemu/$vmid/config" \
                --data "scsi$slot=$dstorage:$dsize,discard=on,ssd=1" \
                > /dev/null
            slot=$((slot+1))
        done
    fi

    # Start
    upid=$(api_form POST "/nodes/${PVE_NODE}/qemu/$vmid/status/start" | python3 -c "import json,sys; print(json.load(sys.stdin).get('data',''))")
    wait_task "$upid" 60
done

echo
echo "=== Step 3: wait for SSH on all VMs ==="
python3 -c "
import json
for v in json.load(open('$INV'))['vms']:
    print(v['ip'])
" | while read -r ip; do
    echo -n "  $ip "
    for _ in $(seq 1 60); do
        if ssh -i ~/.ssh/dtfs-lab/id_ed25519 -o StrictHostKeyChecking=no -o ConnectTimeout=2 -o BatchMode=yes "debian@$ip" 'echo up' >/dev/null 2>&1; then
            echo "up"
            break
        fi
        sleep 2
        echo -n "."
    done
done

echo
echo "Done. Run deploy-mini.sh next."

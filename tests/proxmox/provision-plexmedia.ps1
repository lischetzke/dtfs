# Provision the 12-VM Plexmedia topology from inventory-plexmedia.json.
# Destructive: stops + destroys existing VMs in VMID range 200..299, then
# clones from the cloud-init template. Preserves builder (9001) + template (9000)
# and the previous 3-DC topology (100..199) so a rollback is possible.
#
# Plexmedia represents the real production target:
#   * DC LELI (40 Mbps DSL) — anchor, all 3 voting managers, 1 viewer
#   * DC TORO (400 Mbps)    — SSD warm, 1 manager, 1 cached viewer
#   * DC NRB  (1 Gbps)      — ingest, 1 manager, 1 download client
#
# Run from a Windows host with the PVE API token. Requires PowerShell 7+.

. "$PSScriptRoot\pve.ps1"

$inventoryPath = Join-Path $PSScriptRoot 'inventory-plexmedia.json'
if (-not (Test-Path $inventoryPath)) {
    throw "Inventory not found: $inventoryPath"
}
$inventory = Get-Content $inventoryPath -Raw | ConvertFrom-Json

Write-Output ""
Write-Output "=== Plexmedia provisioning ==="
Write-Output "  cluster: $($inventory.topology.cluster_name)"
Write-Output "  VMs:     $($inventory.vms.Count) (builder kept alive)"

# Step 1 — Tear down anything in the Plexmedia VMID range (200..299).
# This is destructive and intentional: re-provisioning must be idempotent.
Write-Output ""
Write-Output "=== Step 1: stop + destroy existing Plexmedia VMs (range 200..299) ==="
$existing = Invoke-Pve -Method GET -Path "/nodes/$($script:PveNode)/qemu"
$toDestroy = $existing | Where-Object { $_.vmid -ge 200 -and $_.vmid -le 299 }
foreach ($vm in $toDestroy) {
    Write-Output "  destroying vmid=$($vm.vmid) name=$($vm.name) status=$($vm.status)"
    try {
        Remove-LabVm -Vmid $vm.vmid
    } catch {
        Write-Output "  destroy failed (continuing): $_"
    }
}

# Step 2 — Clone each VM from the cloud-init template, attach the storage
# disks specified in inventory. Each "extra_disks" entry is a separate
# (storage, size, tier) tuple — the data-primary01 VM uses all 4 USB
# disks at once (the closest analog to real LELI's 4×~12 TB drive bay).
Write-Output ""
Write-Output "=== Step 2: clone $(($inventory.vms | Where-Object { $_.role -ne 'builder' }).Count) fresh VMs from template ==="
foreach ($vm in $inventory.vms) {
    if ($vm.role -eq 'builder') {
        Write-Output "skip builder vmid=$($vm.vmid) (kept alive)"
        continue
    }

    $extraStorages = @()
    $extraSizes = @()
    if ($vm.extra_disks) {
        foreach ($d in $vm.extra_disks) {
            $extraStorages += $d.storage
            $extraSizes += [int]$d.size_gb
        }
    }

    Write-Output ""
    Write-Output ">>> $($vm.name) vmid=$($vm.vmid) ip=$($vm.ip) dc=$($vm.dc) cores=$($vm.cores) mem=$($vm.memory_mb)MB extra_disks=$($extraSizes -join '+')GB"

    # New-LabClone only attaches the OS disk + a single extra-disks storage;
    # for VMs with multiple extras (data-primary01 has 4), we add the rest
    # below in a second pass.
    $firstStorage = if ($extraStorages.Count -gt 0) { $extraStorages[0] } else { 'pve01_ssd02' }

    New-LabClone `
        -Vmid $vm.vmid `
        -Name $vm.name `
        -Ip $vm.ip `
        -Cores $vm.cores `
        -MemoryMb $vm.memory_mb `
        -OsDiskGb $vm.os_disk_gb `
        -ExtraDisksGb @() `
        -ExtraStorage $firstStorage

    # Add each extra disk to a unique scsi slot. The cloud-init disk lives
    # on scsi0; OS lives on scsi1; extras start at scsi2.
    for ($i = 0; $i -lt $extraStorages.Count; $i++) {
        $slot = $i + 2
        $storage = $extraStorages[$i]
        $size = $extraSizes[$i]
        $label = if ($vm.extra_disks[$i].label) { $vm.extra_disks[$i].label } else { "$($vm.name)-disk$slot" }
        Write-Output "[$($vm.name)] adding scsi$slot ${size}G on $storage (tier=$($vm.extra_disks[$i].tier), label=$label)"
        Invoke-Pve -Method PUT -Path "/nodes/$($script:PveNode)/qemu/$($vm.vmid)/config" -Body @{
            "scsi$slot" = "$($storage):$size,discard=on,ssd=1"
        } | Out-Null
    }

    # Disable balloon (lab budget RAM is preallocated per inventory.ram_budget).
    Invoke-Pve -Method PUT -Path "/nodes/$($script:PveNode)/qemu/$($vm.vmid)/config" -Body @{
        balloon = 0
    } | Out-Null

    Write-Output "[$($vm.name)] starting"
    $upid = Invoke-Pve -Method POST -Path "/nodes/$($script:PveNode)/qemu/$($vm.vmid)/status/start"
    Wait-PveTask -Upid $upid -TimeoutSec 120 | Out-Null
}

# Step 3 — Wait for SSH on every freshly-cloned VM. Without this the
# next phase (Ansible deploy) would crash on the first unreachable host.
Write-Output ""
Write-Output "=== Step 3: wait for SSH on all 11 Plexmedia VMs ==="
foreach ($vm in $inventory.vms) {
    if ($vm.role -eq 'builder') { continue }
    Write-Output "  waiting for SSH on $($vm.name) ($($vm.ip))..."
    try {
        Wait-LabSsh -Ip $vm.ip -TimeoutSec 300 | Out-Null
        Write-Output "  $($vm.name): ready"
    } catch {
        Write-Output "  $($vm.name): SSH timeout — $_"
    }
}

Write-Output ""
Write-Output "=== Plexmedia provisioning complete ==="
Write-Output ""
Write-Output "Topology summary:"
Write-Output "  LELI (40 Mbps anchor):  mgr01..03, data-primary01 (600 GB), client01"
Write-Output "  TORO (400 Mbps warm):   mgr04, data-warm01 (180 GB SSD), client02 (40 GB cache, 90d TTL)"
Write-Output "  NRB  (1 Gbps ingest):   mgr05, data-warm02 (180 GB), client03"
Write-Output ""
Write-Output "Next steps:"
Write-Output "  1. ssh -i C:/Users/leduc/.ssh/dtfs-lab/id_ed25519 debian@10.150.0.150"
Write-Output "  2. cd /home/debian/dtfs2 && git fetch origin && git reset --hard origin/master"
Write-Output "  3. cargo build --release --workspace"
Write-Output "  4. rsync target/release/{dtfs-manager,dtfs-data,dtfs-fuse,dtfs-cli} → all Plexmedia VMs"
Write-Output "  5. ansible-playbook -i inventories/plexmedia/hosts.yml playbooks/full-deploy.yml"
Write-Output "  6. ansible-playbook -i inventories/plexmedia/hosts.yml playbooks/apply-shaping-plexmedia.yml"
Write-Output "  7. ./tests/proxmox/soak/run-plexmedia-1h.sh"

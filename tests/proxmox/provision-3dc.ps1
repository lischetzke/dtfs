# Provision the 13-VM 3-DC topology from inventory-3dc.json.
# Destructive: stops + destroys existing VMs in the IP pool 10.150.0.151-162
# (preserves the builder at .150), then clones from the cloud-init template.
#
# Run from a Windows host with the PVE API token. Requires PowerShell 7+.

. "$PSScriptRoot\pve.ps1"

$inventoryPath = Join-Path $PSScriptRoot 'inventory-3dc.json'
$inventory = Get-Content $inventoryPath -Raw | ConvertFrom-Json

# VMIDs we're going to use (from inventory).
$targetVmids = @($inventory.vms | Where-Object { $_.role -ne 'builder' } | ForEach-Object { $_.vmid })

# Existing VMIDs in the same pool that may collide; destroy them first.
# We only ever touch IDs 100-499 (lab range) — never the builder (9001) and
# never the template (9000).
Write-Output ""
Write-Output "=== Step 1: stop + destroy existing lab VMs (preserving builder + template) ==="
$existing = Invoke-Pve -Method GET -Path "/nodes/$($script:PveNode)/qemu"
$toDestroy = $existing | Where-Object { $_.vmid -ge 100 -and $_.vmid -le 499 }
foreach ($vm in $toDestroy) {
    Write-Output "  destroying vmid=$($vm.vmid) name=$($vm.name) status=$($vm.status)"
    try {
        Remove-LabVm -Vmid $vm.vmid
    } catch {
        Write-Output "  destroy failed (continuing): $_"
    }
}

Write-Output ""
Write-Output "=== Step 2: clone 13 fresh VMs from template ==="
foreach ($vm in $inventory.vms) {
    if ($vm.role -eq 'builder') {
        Write-Output "skip builder vmid=$($vm.vmid) (kept alive)"
        continue
    }

    $extraSizes = @()
    $extraStorages = @()
    if ($vm.extra_disks) {
        foreach ($d in $vm.extra_disks) {
            $extraSizes += [int]$d.size_gb
            $extraStorages += $d.storage
        }
    }

    Write-Output ""
    Write-Output ">>> $($vm.name) vmid=$($vm.vmid) ip=$($vm.ip) dc=$($vm.dc) extra_disks=$($extraSizes -join ',')"

    # Use the first extra storage as the default; the helper applies the
    # same storage to every extra disk. Where disks come from different
    # storages, we add them one-by-one after the clone.
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

    # Add each extra disk on its specific storage.
    for ($i = 0; $i -lt $extraStorages.Count; $i++) {
        $slot = $i + 1
        $storage = $extraStorages[$i]
        $size = $extraSizes[$i]
        Write-Output "[$($vm.name)] adding scsi$slot ${size}G on $storage (tier=$($vm.extra_disks[$i].tier))"
        Invoke-Pve -Method PUT -Path "/nodes/$($script:PveNode)/qemu/$($vm.vmid)/config" -Body @{
            "scsi$slot" = "$($storage):$size,discard=on,ssd=1"
        } | Out-Null
    }

    Write-Output "[$($vm.name)] starting"
    $upid = Invoke-Pve -Method POST -Path "/nodes/$($script:PveNode)/qemu/$($vm.vmid)/status/start"
    Wait-PveTask -Upid $upid -TimeoutSec 120 | Out-Null
}

Write-Output ""
Write-Output "=== Step 3: wait for SSH on all VMs ==="
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
Write-Output "=== provisioning complete ==="
Write-Output "Next steps:"
Write-Output "  1. cd tests/proxmox/ansible"
Write-Output "  2. ansible-playbook -i inventories/3dc/hosts.yml playbooks/full-deploy.yml"
Write-Output "  3. ansible-playbook -i inventories/3dc/hosts.yml playbooks/apply-shaping.yml"

# Continuation script — only provisions VMs whose vmid is not already
# present in PVE. Idempotent; safe to re-run.

. "$PSScriptRoot\pve.ps1"

$inventoryPath = Join-Path $PSScriptRoot 'inventory-3dc.json'
$inventory = Get-Content $inventoryPath -Raw | ConvertFrom-Json

$existing = Invoke-Pve -Method GET -Path "/nodes/$($script:PveNode)/qemu"
$existingIds = @($existing | ForEach-Object { [int]$_.vmid })

foreach ($vm in $inventory.vms) {
    if ($vm.role -eq 'builder') { continue }
    if ($existingIds -contains [int]$vm.vmid) {
        Write-Output "skip $($vm.name) vmid=$($vm.vmid) (already exists)"
        # Make sure it's running
        $info = $existing | Where-Object { $_.vmid -eq $vm.vmid } | Select-Object -First 1
        if ($info.status -ne 'running') {
            Write-Output "  starting $($vm.name)"
            try { Start-LabVm -Vmid $vm.vmid } catch { Write-Output "  start failed: $_" }
        }
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
    $firstStorage = if ($extraStorages.Count -gt 0) { $extraStorages[0] } else { 'pve01_ssd02' }

    Write-Output ""
    Write-Output ">>> $($vm.name) vmid=$($vm.vmid) ip=$($vm.ip) dc=$($vm.dc) extra_disks=$($extraSizes -join ',')"
    New-LabClone `
        -Vmid $vm.vmid `
        -Name $vm.name `
        -Ip $vm.ip `
        -Cores $vm.cores `
        -MemoryMb $vm.memory_mb `
        -OsDiskGb $vm.os_disk_gb `
        -ExtraDisksGb @() `
        -ExtraStorage $firstStorage
    for ($i = 0; $i -lt $extraStorages.Count; $i++) {
        $slot = $i + 1
        $storage = $extraStorages[$i]
        $size = $extraSizes[$i]
        Write-Output "[$($vm.name)] adding scsi$slot ${size}G on $storage"
        Invoke-Pve -Method PUT -Path "/nodes/$($script:PveNode)/qemu/$($vm.vmid)/config" -Body @{
            "scsi$slot" = "$($storage):$size,discard=on,ssd=1"
        } | Out-Null
    }
    $upid = Invoke-Pve -Method POST -Path "/nodes/$($script:PveNode)/qemu/$($vm.vmid)/status/start"
    Wait-PveTask -Upid $upid -TimeoutSec 120 | Out-Null
}

Write-Output ""
Write-Output "=== Waiting for SSH on all 12 lab VMs ==="
foreach ($vm in $inventory.vms) {
    if ($vm.role -eq 'builder') { continue }
    try {
        Wait-LabSsh -Ip $vm.ip -TimeoutSec 240 | Out-Null
        Write-Output "  $($vm.name) ($($vm.ip)): ready"
    } catch {
        Write-Output "  $($vm.name) ($($vm.ip)): NOT READY ($_)"
    }
}

Write-Output ""
Write-Output "=== Provision complete ==="

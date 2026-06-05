#!/usr/bin/env pwsh
# tests/proxmox/pve-chaos.ps1 — DC-aware chaos verbs for the release suite.
# Dot-sources pve.ps1 (PVE REST helpers) and resolves VM names + DC groups
# from inventory-3dc.json. REST-only (no ssh), so it runs under Linux pwsh in
# the bash release harness as well as on Windows.
#
#   pwsh -NoProfile -File pve-chaos.ps1 -Action power-off  -Dc dc-c -Force
#   pwsh -NoProfile -File pve-chaos.ps1 -Action power-on   -Dc dc-c
#   pwsh -NoProfile -File pve-chaos.ps1 -Action detach-disk -Vm data-a1 -Slot 2
#   pwsh -NoProfile -File pve-chaos.ps1 -Action attach-disk -Vm data-a1 -Slot 2 -SizeGb 16
#   pwsh -NoProfile -File pve-chaos.ps1 -Action destroy-all
[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [ValidateSet('power-off', 'power-on', 'detach-disk', 'attach-disk', 'destroy-all', 'help')]
    [string]$Action,
    [string]$Dc,
    [string]$Vm,
    [int]$Slot,
    [int]$SizeGb = 16,
    [switch]$Force
)

. "$PSScriptRoot/pve.ps1"

$invPath = Join-Path $PSScriptRoot 'inventory-3dc.json'
$inv = Get-Content $invPath -Raw | ConvertFrom-Json
$extraStorage = if ($inv.pve.storage_hot) { $inv.pve.storage_hot } else { 'pve01_ssd02' }

function Resolve-VmidForName([string]$name) {
    $v = $inv.vms | Where-Object { $_.name -eq $name } | Select-Object -First 1
    if (-not $v) { throw "unknown VM name: $name" }
    return [int]$v.vmid
}

function Get-Targets {
    if ($Dc) { return @($inv.vms | Where-Object { $_.dc -eq $Dc -and $_.role -ne 'builder' }) }
    if ($Vm) { return @($inv.vms | Where-Object { $_.name -eq $Vm } | Select-Object -First 1) }
    throw "specify -Dc or -Vm"
}

switch ($Action) {
    'help' {
        Write-Output "pve-chaos verbs:"
        Write-Output "  power-off  [-Dc <dc>|-Vm <name>] [-Force]"
        Write-Output "  power-on   [-Dc <dc>|-Vm <name>]"
        Write-Output "  detach-disk -Vm <name> -Slot <n>"
        Write-Output "  attach-disk -Vm <name> -Slot <n> [-SizeGb <gb>]"
        Write-Output "  destroy-all"
    }
    'power-off' {
        foreach ($v in Get-Targets) {
            Write-Output "power-off $($v.name) vmid=$($v.vmid) (force=$Force)"
            Stop-LabVm -Vmid ([int]$v.vmid) -Force:$Force
        }
    }
    'power-on' {
        foreach ($v in Get-Targets) {
            Write-Output "power-on $($v.name) vmid=$($v.vmid)"
            Start-LabVm -Vmid ([int]$v.vmid)
        }
    }
    'detach-disk' {
        if (-not $Vm -or -not $Slot) { throw "detach-disk needs -Vm and -Slot" }
        $id = Resolve-VmidForName $Vm
        Write-Output "detach scsi$Slot from $Vm vmid=$id"
        $upid = Invoke-Pve -Method PUT -Path "/nodes/$($script:PveNode)/qemu/$id/config" -Body @{ delete = "scsi$Slot" }
        if ($upid) { try { Wait-PveTask -Upid $upid -TimeoutSec 60 | Out-Null } catch { Write-Output "  (config edit returned: $_)" } }
    }
    'attach-disk' {
        if (-not $Vm -or -not $Slot) { throw "attach-disk needs -Vm and -Slot" }
        $id = Resolve-VmidForName $Vm
        Write-Output "attach blank scsi$Slot (${SizeGb}G on $extraStorage) to $Vm vmid=$id"
        Invoke-Pve -Method PUT -Path "/nodes/$($script:PveNode)/qemu/$id/config" -Body @{ "scsi$Slot" = "$($extraStorage):$SizeGb,discard=on,ssd=1" } | Out-Null
    }
    'destroy-all' {
        foreach ($v in ($inv.vms | Where-Object { $_.role -ne 'builder' })) {
            Write-Output "destroy $($v.name) vmid=$($v.vmid)"
            try { Remove-LabVm -Vmid ([int]$v.vmid) } catch { Write-Output "  destroy failed (continuing): $_" }
        }
    }
}

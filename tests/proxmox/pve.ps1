# DTFS Proxmox lab helper - dot-source at top of scripts:
#   . "$PSScriptRoot\pve.ps1"
# Reads API token from $env:PVE_TOKEN if set, else uses the hardcoded lab token.

$script:PveHost = '10.150.0.121'
$script:PveNode = 'i-pve01'
$script:PveBase = "https://$($script:PveHost):8006/api2/json"
$script:PveToken = if ($env:PVE_TOKEN) { $env:PVE_TOKEN } else { 'root@pam!dtfs-test=631edfe4-7a29-44e9-b10b-6d0141cdbf88' }
$script:PveHeaders = @{ Authorization = "PVEAPIToken=$($script:PveToken)" }
$script:PveCommon = @{
    Headers              = $script:PveHeaders
    SkipCertificateCheck = $true
    SkipHeaderValidation = $true
    TimeoutSec           = 30
}

function Invoke-Pve {
    param(
        [Parameter(Mandatory)] [ValidateSet('GET', 'POST', 'PUT', 'DELETE')] [string]$Method,
        [Parameter(Mandatory)] [string]$Path,
        [hashtable]$Body
    )
    $uri = "$($script:PveBase)$Path"
    $args = $script:PveCommon.Clone()
    $args.Method = $Method
    $args.Uri = $uri
    if ($Body) { $args.Body = $Body }
    try {
        $r = Invoke-RestMethod @args
        return $r.data
    } catch {
        $msg = $_.Exception.Message
        if ($_.ErrorDetails.Message) { $msg += " | " + $_.ErrorDetails.Message }
        throw $msg
    }
}

function Wait-PveTask {
    param(
        [Parameter(Mandatory)] [string]$Upid,
        [int]$TimeoutSec = 300
    )
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    $encUpid = [uri]::EscapeDataString($Upid)
    while ((Get-Date) -lt $deadline) {
        $r = Invoke-Pve -Method GET -Path "/nodes/$($script:PveNode)/tasks/$encUpid/status"
        if ($r.status -eq 'stopped') {
            if ($r.exitstatus -eq 'OK') { return $true }
            throw "Task $Upid failed: $($r.exitstatus)"
        }
        Start-Sleep -Seconds 3
    }
    throw "Task $Upid timed out after $TimeoutSec s"
}

function New-LabClone {
    param(
        [Parameter(Mandatory)] [int]$Vmid,
        [Parameter(Mandatory)] [string]$Name,
        [Parameter(Mandatory)] [string]$Ip,
        [Parameter(Mandatory)] [int]$Cores,
        [Parameter(Mandatory)] [int]$MemoryMb,
        [Parameter(Mandatory)] [int]$OsDiskGb,
        [int[]]$ExtraDisksGb = @(),
        [string]$ExtraStorage = 'pve01_ssd02',
        [int]$TemplateVmid = 9000,
        [string]$Storage = 'pve01_ssd01'
    )
    Write-Output "[$Name vmid=$Vmid] cloning from $TemplateVmid"
    $upid = Invoke-Pve -Method POST -Path "/nodes/$($script:PveNode)/qemu/$TemplateVmid/clone" -Body @{ newid = $Vmid; name = $Name; target = $script:PveNode; storage = $Storage; full = 1 }
    Wait-PveTask -Upid $upid -TimeoutSec 240 | Out-Null
    Write-Output "[$Name] configuring (cores=$Cores mem=$MemoryMb ip=$Ip)"
    Invoke-Pve -Method PUT -Path "/nodes/$($script:PveNode)/qemu/$Vmid/config" -Body @{
        cores      = $Cores
        memory     = $MemoryMb
        ipconfig0  = "ip=$Ip/24,gw=10.150.0.1"
        nameserver = '10.150.0.2 10.150.0.3'
    } | Out-Null
    if ($OsDiskGb -gt 3) {
        Write-Output "[$Name] resizing scsi0 to ${OsDiskGb}G"
        Invoke-Pve -Method PUT -Path "/nodes/$($script:PveNode)/qemu/$Vmid/resize" -Body @{ disk = 'scsi0'; size = "${OsDiskGb}G" } | Out-Null
    }
    for ($i = 0; $i -lt $ExtraDisksGb.Count; $i++) {
        $slot = $i + 1
        $size = $ExtraDisksGb[$i]
        Write-Output "[$Name] adding scsi$slot ${size}G on $ExtraStorage"
        Invoke-Pve -Method PUT -Path "/nodes/$($script:PveNode)/qemu/$Vmid/config" -Body @{
            "scsi$slot" = "$($ExtraStorage):$size,discard=on,ssd=1"
        } | Out-Null
    }
    return $Vmid
}

function Start-LabVm {
    param([Parameter(Mandatory)] [int]$Vmid)
    $upid = Invoke-Pve -Method POST -Path "/nodes/$($script:PveNode)/qemu/$Vmid/status/start"
    Wait-PveTask -Upid $upid -TimeoutSec 60 | Out-Null
}

function Stop-LabVm {
    param([Parameter(Mandatory)] [int]$Vmid, [switch]$Force)
    $endpoint = if ($Force) { 'stop' } else { 'shutdown' }
    try {
        $upid = Invoke-Pve -Method POST -Path "/nodes/$($script:PveNode)/qemu/$Vmid/status/$endpoint" -Body @{ timeout = 30 }
        Wait-PveTask -Upid $upid -TimeoutSec 60 | Out-Null
    } catch {
        Write-Output "Stop-LabVm: $_"
    }
}

function Remove-LabVm {
    param([Parameter(Mandatory)] [int]$Vmid)
    Stop-LabVm -Vmid $Vmid -Force
    $upid = Invoke-Pve -Method DELETE -Path "/nodes/$($script:PveNode)/qemu/$Vmid"
    Wait-PveTask -Upid $upid -TimeoutSec 120 | Out-Null
}

function Wait-LabSsh {
    param(
        [Parameter(Mandatory)] [string]$Ip,
        [string]$User = 'debian',
        [int]$TimeoutSec = 300,
        [string]$KeyPath = "$env:USERPROFILE\.ssh\dtfs-lab\id_ed25519"
    )
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        $r = & ssh.exe -i $KeyPath -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile="$env:USERPROFILE\.ssh\dtfs-lab\known_hosts" -o BatchMode=yes "$User@$Ip" 'echo ready' 2>$null
        if ($LASTEXITCODE -eq 0 -and $r -eq 'ready') { return $true }
        Start-Sleep -Seconds 5
    }
    throw "SSH to $User@$Ip did not become available within ${TimeoutSec}s"
}

function Invoke-LabSsh {
    param(
        [Parameter(Mandatory)] [string]$Ip,
        [Parameter(Mandatory)] [string]$Command,
        [string]$User = 'debian',
        [string]$KeyPath = "$env:USERPROFILE\.ssh\dtfs-lab\id_ed25519"
    )
    & ssh.exe -i $KeyPath -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile="$env:USERPROFILE\.ssh\dtfs-lab\known_hosts" "$User@$Ip" $Command
}

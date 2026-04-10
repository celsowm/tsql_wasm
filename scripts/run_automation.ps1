$ErrorActionPreference = "Stop"

# Runner for TDS Proxy and SSMS Automation
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$credentialsPath = Join-Path $repoRoot "scripts\credentials.json"
$credentials = Get-Content -LiteralPath $credentialsPath -Raw | ConvertFrom-Json
$sqlUser = $credentials.sql_server_user
$sqlPassword = $credentials.sql_server_password
$serverName = "tcp:127.0.0.1,1433"
$connectTimeoutSeconds = 120

function Wait-TcpPort {
    param(
        [Parameter(Mandatory = $true)]
        [string] $HostName,
        [Parameter(Mandatory = $true)]
        [int] $Port,
        [Parameter(Mandatory = $true)]
        [int] $TimeoutSeconds
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        try {
            $tcp = New-Object System.Net.Sockets.TcpClient
            $iar = $tcp.BeginConnect($HostName, $Port, $null, $null)
            if ($iar.AsyncWaitHandle.WaitOne(1000, $false) -and $tcp.Connected) {
                $tcp.EndConnect($iar) | Out-Null
                $tcp.Close()
                return $true
            }
            $tcp.Close()
        } catch {
        }
        Start-Sleep -Milliseconds 500
    }
    return $false
}

function Wait-LogMarker {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path,
        [Parameter(Mandatory = $true)]
        [string] $Pattern,
        [Parameter(Mandatory = $true)]
        [int] $TimeoutSeconds
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        if (Test-Path -LiteralPath $Path) {
            if (Select-String -LiteralPath $Path -Pattern $Pattern -Quiet) {
                return $true
            }
        }
        Start-Sleep -Milliseconds 500
    }
    return $false
}

# 0. Close existing SSMS
Write-Host "Closing existing SSMS..." -ForegroundColor Gray
Stop-Process -Name SSMS -ErrorAction SilentlyContinue

$logDir = Join-Path $repoRoot "logs\automation"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$proxyLogPath = Join-Path $logDir "proxy_$timestamp.log"
$proxyErrPath = Join-Path $logDir "proxy_$timestamp.err.log"

# 1. Start TDS Proxy, skipping Azure
Write-Host "Starting TDS Proxy (Phase 2: Playground)..." -ForegroundColor Cyan
$proxyArgs = @("scripts/tds_proxy.py", "--no-interactive", "--skip-azure")
$proxyProcess = Start-Process `
    -FilePath python `
    -ArgumentList $proxyArgs `
    -PassThru `
    -NoNewWindow:$false `
    -RedirectStandardOutput $proxyLogPath `
    -RedirectStandardError $proxyErrPath

Write-Host "Proxy PID: $($proxyProcess.Id)" -ForegroundColor Gray
Write-Host "Proxy log: $proxyLogPath" -ForegroundColor Gray
Write-Host "Proxy err: $proxyErrPath" -ForegroundColor Gray

Write-Host "Waiting for proxy port 127.0.0.1:1433..." -ForegroundColor Yellow
if (-not (Wait-TcpPort -HostName "127.0.0.1" -Port 1433 -TimeoutSeconds 90)) {
    throw "Timeout waiting for proxy port 127.0.0.1:1433."
}

Write-Host "Waiting for phase 2 ready marker in proxy log..." -ForegroundColor Yellow
if (-not (Wait-LogMarker -Path $proxyLogPath -Pattern "PHASE2_READY" -TimeoutSeconds 180)) {
    throw "Timeout waiting for phase 2 readiness marker (PHASE2_READY) in $proxyLogPath."
}

# 2. Run SSMS Automation for Playground
Write-Host "Running SSMS Automation for Playground..." -ForegroundColor Green
$automationArgs = @(
    "scripts/ssms_automation.py"
    "--server", $serverName
    "--user", $sqlUser
    "--password", $sqlPassword
    "--connect-timeout", "$connectTimeoutSeconds"
)
& python @automationArgs
$automationExitCode = $LASTEXITCODE

if ($automationExitCode -ne 0) {
    throw "SSMS automation failed with exit code $automationExitCode. Check $proxyLogPath and $proxyErrPath for proxy details."
}

Write-Host "Automation sequence finished successfully." -ForegroundColor Cyan
Write-Host "The proxy is still running in the background (PID $($proxyProcess.Id)). Close it when done." -ForegroundColor Gray

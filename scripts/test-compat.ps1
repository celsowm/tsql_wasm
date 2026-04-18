$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot
$credentialsPath = Join-Path $repoRoot "scripts\credentials.json"
$credentials = Get-Content -LiteralPath $credentialsPath -Raw | ConvertFrom-Json
$sqlPassword = $credentials.sql_server_password

function Stop-PodmanIfRunning {
    $machineRunning = podman machine list 2>&1 | Select-String "Currently running"
    if (-not $machineRunning) {
        return
    }

    Write-Host "Podman machine is running; stopping iridium_test_sqlserver before compatibility run..." -ForegroundColor Yellow
    $existing = podman ps -a --filter "name=iridium_test_sqlserver" --format "{{.Names}}" 2>$null
    if ($existing -and ($existing | Select-String "iridium_test_sqlserver")) {
        $running = podman ps --filter "name=iridium_test_sqlserver" --format "{{.Names}}" 2>$null
        if ($running -and ($running | Select-String "iridium_test_sqlserver")) {
            podman stop iridium_test_sqlserver 2>&1 | ForEach-Object { Write-Host $_ }
        }
    }

    podman machine stop 2>&1 | ForEach-Object { Write-Host $_ }
}

# ══════════════════════════════════════════════════════════════════════
# STEP 1 — Ensure Podman + Azure SQL Edge are running
# ══════════════════════════════════════════════════════════════════════
Write-Host "=============================================" -ForegroundColor Cyan
Write-Host " STEP 1: Azure SQL Edge (Podman)" -ForegroundColor Cyan
Write-Host "=============================================" -ForegroundColor Cyan

Stop-PodmanIfRunning

$machineRunning = podman machine list 2>&1 | Select-String "Currently running"
if (-not $machineRunning) {
    Write-Host "Starting Podman machine..." -ForegroundColor Yellow
    podman machine start 2>&1 | Out-Null
    Start-Sleep -Seconds 3
} else {
    Write-Host "Podman machine already running." -ForegroundColor Green
}

$existing = podman ps -a --filter "name=iridium_test_sqlserver" --format "{{.Names}}" 2>$null
if (-not $existing) {
    Write-Host "Creating Azure SQL Edge container..." -ForegroundColor Yellow
    podman run -d --name iridium_test_sqlserver `
        -e ACCEPT_EULA=Y `
        -e MSSQL_SA_PASSWORD=$sqlPassword `
        -p 11433:1433 `
        --memory=512m `
        mcr.microsoft.com/azure-sql-edge:latest | Out-Null
} else {
    $running = podman ps --filter "name=iridium_test_sqlserver" --format "{{.Names}}" 2>$null
    if (-not $running) {
        Write-Host "Starting existing container..." -ForegroundColor Yellow
        podman start iridium_test_sqlserver | Out-Null
    } else {
        Write-Host "Container already running." -ForegroundColor Green
    }
}

Write-Host "Waiting for Azure SQL Edge..." -ForegroundColor Yellow
$azureReady = $false
for ($attempt = 1; $attempt -le 30; $attempt++) {
    try {
        $tc = [System.Net.Sockets.TcpClient]::new([System.Net.Sockets.AddressFamily]::InterNetworkV6)
        $tc.Connect([System.Net.IPAddress]::IPv6Loopback, 11433)
        $tc.Close()
        $azureReady = $true
        break
    } catch {
        Write-Host "  Attempt $attempt/30..." -ForegroundColor DarkGray
        Start-Sleep -Seconds 2
    }
}
if (-not $azureReady) {
    Write-Host "Azure SQL Edge not reachable!" -ForegroundColor Red
    exit 1
}
Write-Host "Azure SQL Edge ready." -ForegroundColor Green

# ══════════════════════════════════════════════════════════════════════
# STEP 2 — Build compat-query (Rust) + compat-runner (C#)
# ══════════════════════════════════════════════════════════════════════
Write-Host ""
Write-Host "=============================================" -ForegroundColor Cyan
Write-Host " STEP 2: Build" -ForegroundColor Cyan
Write-Host "=============================================" -ForegroundColor Cyan

$cargo = Join-Path $env:USERPROFILE ".cargo\bin\cargo.exe"
if (-not (Test-Path $cargo)) { $cargo = "cargo" }

$prevPref = $ErrorActionPreference
$ErrorActionPreference = "Continue"
& $cargo build --package iridium_server --bin compat-query 2>&1 | ForEach-Object { Write-Host $_ }
$ErrorActionPreference = $prevPref
if ($LASTEXITCODE -ne 0) {
    Write-Host "Cargo build failed!" -ForegroundColor Red
    exit 1
}
Write-Host "compat-query built." -ForegroundColor Green

# ══════════════════════════════════════════════════════════════════════
# STEP 3 — Run the C# test runner (seeds Azure + compares queries)
# ══════════════════════════════════════════════════════════════════════
Write-Host ""
Write-Host "=============================================" -ForegroundColor Cyan
Write-Host " STEP 3: Compatibility Tests" -ForegroundColor Cyan
Write-Host "=============================================" -ForegroundColor Cyan

$prevPref = $ErrorActionPreference
$ErrorActionPreference = "Continue"
$reportDir = Join-Path $repoRoot "target\compatibility"
if (-not (Test-Path $reportDir)) {
    New-Item -ItemType Directory -Path $reportDir | Out-Null
}
$reportStem = "compat-run-{0}" -f (Get-Date -Format "yyyyMMdd-HHmmss")
$reportPath = Join-Path $reportDir ($reportStem + ".log")
$env:IRIDIUM_COMPAT_REPORT_DIR = $reportDir
$env:IRIDIUM_COMPAT_REPORT_STEM = $reportStem
& dotnet run --project scripts/compat-runner 2>&1 | Tee-Object -FilePath $reportPath | ForEach-Object { Write-Host $_ }
$testExit = $LASTEXITCODE
$ErrorActionPreference = $prevPref

Write-Host "Compatibility report saved to $reportPath" -ForegroundColor Cyan
Write-Host "Structured JSON report saved to $(Join-Path $reportDir ($reportStem + '.json'))" -ForegroundColor Cyan

exit $testExit



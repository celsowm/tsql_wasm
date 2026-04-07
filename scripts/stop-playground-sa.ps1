$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$logDir = Join-Path $repoRoot "logs"
$pidFile = Join-Path $logDir "playground-sa.pid"

if (-not (Test-Path $pidFile)) {
    $proc = Get-Process tsql-server -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($null -eq $proc) {
        Write-Host "No playground PID file found and no tsql-server process is running." -ForegroundColor Yellow
        exit 0
    }

    Stop-Process -Id $proc.Id -Force
    Write-Host "Stopped tsql-server process $($proc.Id)." -ForegroundColor Green
    exit 0
}

$playgroundPid = [int](Get-Content -LiteralPath $pidFile -Raw).Trim()
if ($playgroundPid -le 0) {
    throw "Invalid PID in $pidFile"
}

$proc = Get-Process -Id $playgroundPid -ErrorAction SilentlyContinue
if ($null -eq $proc) {
    Write-Host "Playground process $playgroundPid is not running." -ForegroundColor Yellow
    Remove-Item -LiteralPath $pidFile -Force
    exit 0
}

Stop-Process -Id $playgroundPid -Force
Remove-Item -LiteralPath $pidFile -Force
Write-Host "Stopped playground process $playgroundPid." -ForegroundColor Green

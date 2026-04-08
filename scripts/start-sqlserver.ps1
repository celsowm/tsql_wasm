$ErrorActionPreference = "Stop"

function Write-LogLine {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Message,
        [ValidateSet("Gray", "Green", "Yellow", "Red", "Cyan", "White")]
        [string] $Color = "White"
    )

    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss.fff"
    $line = "[$timestamp] $Message"
    Add-Content -LiteralPath $script:LogFile -Value $line -Encoding utf8
    Write-Host $Message -ForegroundColor $Color
}

function Write-LogBlankLine {
    Add-Content -LiteralPath $script:LogFile -Value "" -Encoding utf8
    Write-Host ""
}

function Rotate-LogFile {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path,
        [int] $MaxBytes = 5MB,
        [int] $MaxBackups = 5
    )

    if (-not (Test-Path $Path)) {
        return
    }

    $length = (Get-Item -LiteralPath $Path).Length
    if ($length -lt $MaxBytes) {
        return
    }

    for ($i = $MaxBackups; $i -ge 1; $i--) {
        $source = if ($i -eq 1) { $Path } else { "$Path.$($i - 1)" }
        $target = "$Path.$i"
        if (Test-Path $source) {
            if (Test-Path $target) {
                Remove-Item -LiteralPath $target -Force
            }
            Move-Item -LiteralPath $source -Destination $target -Force
        }
    }
}

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot
$credentialsPath = Join-Path $repoRoot "scripts\credentials.json"
$credentials = Get-Content -LiteralPath $credentialsPath -Raw | ConvertFrom-Json
$sqlUser = $credentials.sql_server_user
$sqlPassword = $credentials.sql_server_password

$logDir = Join-Path $repoRoot "logs"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$script:LogFile = Join-Path $logDir "sqlserver-container.log"
Rotate-LogFile -Path $script:LogFile

Write-LogLine "Ensuring Podman machine is running..." Yellow
$machineStatus = podman machine list 2>&1 | Select-String "Currently running"
if (-not $machineStatus) {
    Write-LogLine "Starting Podman machine..." Yellow
    podman machine start 2>&1 | ForEach-Object {
        Add-Content -LiteralPath $script:LogFile -Value $_.ToString() -Encoding utf8
        Write-Host $_
    }
    Start-Sleep -Seconds 3
}

Write-LogLine "Checking tsql_test_sqlserver container..." Yellow
$existing = podman ps -a --filter "name=tsql_test_sqlserver" --format "{{.Names}}" 2>$null
if (-not $existing) {
    Write-LogLine "Creating container SQL Server..." Yellow
    podman run -d --name tsql_test_sqlserver -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD=$sqlPassword -p 11433:1433 --memory=512m mcr.microsoft.com/azure-sql-edge:latest 2>&1 | ForEach-Object {
        Add-Content -LiteralPath $script:LogFile -Value $_.ToString() -Encoding utf8
        Write-Host $_
    }
} else {
    $running = podman ps --filter "name=tsql_test_sqlserver" --format "{{.Names}}" 2>$null
    if (-not $running) {
        Write-LogLine "Starting existing container..." Yellow
        podman start tsql_test_sqlserver 2>&1 | ForEach-Object {
            Add-Content -LiteralPath $script:LogFile -Value $_.ToString() -Encoding utf8
            Write-Host $_
        }
    } else {
        Write-LogLine "Container already running." Green
    }
}

Write-LogLine "Waiting for SQL Server to become ready..." Yellow
Write-LogBlankLine

$maxRetries = 30
$retry = 0

do {
    Start-Sleep -Seconds 2
    $retry++
    $result = podman exec tsql_test_sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U $sqlUser -P $sqlPassword -Q "SELECT 1" 2>$null
    if ($LASTEXITCODE -eq 0) {
        Write-LogLine "SQL Server ready on localhost:11433" Green
        exit 0
    }
    Write-LogLine "Retry $retry/$maxRetries..." Gray
} while ($retry -lt $maxRetries)

Write-LogLine "Timeout waiting for SQL Server" Red
exit 1

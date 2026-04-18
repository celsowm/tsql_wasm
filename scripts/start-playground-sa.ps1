$ErrorActionPreference = "Stop"

# Run the iridium_server playground with explicit SQL auth so SSMS can log in.
# SSMS Preview often defaults to encrypted connections, so we start with TLS enabled.
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot
$credentialsPath = Join-Path $repoRoot "scripts\credentials.json"
$credentials = Get-Content -LiteralPath $credentialsPath -Raw | ConvertFrom-Json
$sqlUser = $credentials.sql_server_user
$sqlPassword = $credentials.sql_server_password

$cargo = Join-Path $env:USERPROFILE ".cargo\bin\cargo.exe"
if (-not (Test-Path $cargo)) {
    throw "cargo.exe not found at $cargo. Install Rust with rustup or add cargo to PATH."
}

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

$logDir = Join-Path $repoRoot "logs"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$script:LogFile = Join-Path $logDir "playground-sa.log"
Rotate-LogFile -Path $script:LogFile

$env:RUST_LOG = "iridium_server=debug,iridium_core=info"

function Stop-PodmanIfRunning {
    try {
        $machineRunning = podman machine list 2>$null | Select-String "Currently running"
        if (-not $machineRunning) {
            return
        }

        Write-LogLine "Podman machine is running; stopping iridium_test_sqlserver before starting playground..." Yellow

        $existing = podman ps -a --filter "name=iridium_test_sqlserver" --format "{{.Names}}" 2>$null
        if ($existing -and ($existing | Select-String "iridium_test_sqlserver")) {
            $running = podman ps --filter "name=iridium_test_sqlserver" --format "{{.Names}}" 2>$null
            if ($running -and ($running | Select-String "iridium_test_sqlserver")) {
                podman stop iridium_test_sqlserver 2>&1 | ForEach-Object {
                    Write-LogLine $_.ToString() Gray
                }
            }
        }

        podman machine stop 2>&1 | ForEach-Object {
            Write-LogLine $_.ToString() Gray
        }
    }
    catch {
        Write-LogLine "Podman cleanup skipped: $($_.Exception.Message)" Yellow
    }
}

function Get-ProcessOutputText {
    param(
        [Parameter(Mandatory = $true)]
        [object] $Item
    )

    if ($Item -is [System.Management.Automation.ErrorRecord]) {
        if ($Item.Exception -and $Item.Exception.Message) {
            return $Item.Exception.Message
        }
        if ($Item.TargetObject) {
            return $Item.TargetObject.ToString()
        }
    }

    return $Item.ToString()
}

function Stop-StaleIridiumServer {
    try {
        $running = Get-Process -Name "iridium-server" -ErrorAction SilentlyContinue
        if (-not $running) {
            return
        }
        foreach ($proc in $running) {
            Write-LogLine "Stopping stale iridium-server process $($proc.Id) to avoid binary lock..." Yellow
            Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
        }
    }
    catch {
        Write-LogLine "Could not stop stale iridium-server process: $($_.Exception.Message)" Yellow
    }
}

Stop-PodmanIfRunning
Stop-StaleIridiumServer

$args = @(
    "run"
    "--package", "iridium_server"
    "--bin", "iridium-server"
    "--"
    "--playground"
    "--memory"
    "--tls-gen"
    "--host", "127.0.0.1"
    "--port", "1433"
    "--user", $sqlUser
    "--password", $sqlPassword
)

Write-LogLine "Starting iridium-server playground on localhost:1433 with TLS and $sqlUser / $sqlPassword..." Green
Write-LogLine "Use Server Name = localhost in SSMS." Green
Write-LogLine "Writing server log to $script:LogFile" Cyan
Write-LogBlankLine

$script:PlaygroundReady = $false

& $cargo @args 2>&1 | ForEach-Object {
    $text = Get-ProcessOutputText $_
    if ($text -eq "System.Management.Automation.RemoteException") {
        return
    }
    Add-Content -LiteralPath $script:LogFile -Value $text -Encoding utf8
    Write-Host $text
    if (-not $script:PlaygroundReady -and $text -match "TDS Server listening on") {
        $script:PlaygroundReady = $true
        Write-LogLine "Playground is ready. Connect SSMS to localhost now." Green
    }
}

if ($LASTEXITCODE -ne 0) {
    Write-LogLine "iridium-server exited with code $LASTEXITCODE" Red
    exit $LASTEXITCODE
}

Write-LogLine "iridium-server stopped normally." Green


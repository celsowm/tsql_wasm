$ErrorActionPreference = "Stop"

# Run the tsql_server playground with explicit SQL auth so SSMS can log in.
# SSMS Preview often defaults to encrypted connections, so we start with TLS enabled.
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

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

$env:RUST_LOG = "tsql_server=debug,tsql_core=info"

$args = @(
    "run"
    "--package", "tsql_server"
    "--bin", "tsql-server"
    "--"
    "--playground"
    "--tls-gen"
    "--host", "127.0.0.1"
    "--port", "1433"
    "--user", "sa"
    "--password", "12345"
)

Write-LogLine "Starting tsql-server playground on localhost:1433 with TLS and sa / 12345..." Green
Write-LogLine "Use Server Name = localhost in SSMS." Green
Write-LogLine "Writing server log to $script:LogFile" Cyan
Write-LogBlankLine

& $cargo @args 2>&1 | ForEach-Object {
    $text = $_.ToString()
    Add-Content -LiteralPath $script:LogFile -Value $text -Encoding utf8
    Write-Host $text
}

if ($LASTEXITCODE -ne 0) {
    Write-LogLine "tsql-server exited with code $LASTEXITCODE" Red
    exit $LASTEXITCODE
}

Write-LogLine "tsql-server stopped normally." Green

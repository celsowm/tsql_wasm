$ErrorActionPreference = "Stop"

# Run the tsql_server playground with explicit SQL auth so SSMS can log in.
# SSMS Preview often defaults to encrypted connections, so we start with TLS enabled.
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$cargo = Join-Path $env:USERPROFILE ".cargo\bin\cargo.exe"
if (-not (Test-Path $cargo)) {
    throw "cargo.exe not found at $cargo. Install Rust with rustup or add cargo to PATH."
}

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

Write-Host "Starting tsql-server playground on localhost:1433 with TLS and sa / 12345..." -ForegroundColor Green
Write-Host "Use Server Name = localhost in SSMS." -ForegroundColor Green
& $cargo @args

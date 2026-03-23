# Garante que a machine do Podman ta rodando
$machineStatus = podman machine list 2>&1 | Select-String "Currently running"
if (-not $machineStatus) {
    Write-Host "Iniciando Podman machine..." -ForegroundColor Yellow
    podman machine start
    Start-Sleep -Seconds 3
}

# Verifica se o container ja existe
$existing = podman ps -a --filter "name=tsql_test_sqlserver" --format "{{.Names}}" 2>$null
if (-not $existing) {
    Write-Host "Criando container SQL Server..." -ForegroundColor Yellow
    podman run -d --name tsql_test_sqlserver -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD=Test@12345 -p 11433:1433 --memory=512m mcr.microsoft.com/azure-sql-edge:latest
} else {
    $running = podman ps --filter "name=tsql_test_sqlserver" --format "{{.Names}}" 2>$null
    if (-not $running) {
        Write-Host "Iniciando container existente..." -ForegroundColor Yellow
        podman start tsql_test_sqlserver
    } else {
        Write-Host "Container ja esta rodando." -ForegroundColor Green
    }
}

Write-Host "Aguardando SQL Server iniciar..." -ForegroundColor Yellow

$maxRetries = 30
$retry = 0

do {
    Start-Sleep -Seconds 2
    $retry++
    $result = podman exec tsql_test_sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "Test@12345" -Q "SELECT 1" 2>$null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "SQL Server pronto em localhost:11433" -ForegroundColor Green
        exit 0
    }
    Write-Host "Tentativa $retry/$maxRetries..." -ForegroundColor DarkGray
} while ($retry -lt $maxRetries)

Write-Host "Timeout aguardando SQL Server" -ForegroundColor Red
exit 1

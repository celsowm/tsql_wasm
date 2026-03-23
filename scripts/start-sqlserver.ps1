docker compose up -d

Write-Host "Aguardando SQL Server iniciar..." -ForegroundColor Yellow

$maxRetries = 30
$retry = 0

do {
    Start-Sleep -Seconds 2
    $retry++
    $result = docker exec tsql_test_sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "Test@12345" -Q "SELECT 1" 2>$null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "SQL Server pronto em localhost:11433" -ForegroundColor Green
        exit 0
    }
    Write-Host "Tentativa $retry/$maxRetries..." -ForegroundColor DarkGray
} while ($retry -lt $maxRetries)

Write-Host "Timeout aguardando SQL Server" -ForegroundColor Red
exit 1

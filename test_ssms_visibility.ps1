# Test script to verify SSMS table visibility fix
# This script tests the exact queries that SSMS uses to list databases and tables

Write-Host "=== Testing SSMS Database List Query ===" -ForegroundColor Cyan

# Test the database list query
$testDbQuery = @"
SELECT dtb.name AS [Database_Name]
FROM sys.databases AS dtb
WHERE (
    CAST(case when dtb.name in ('master','model','msdb','tempdb') then 1 else 0 end AS bit) = 0
    OR CAST(ISNULL(HAS_PERMS_BY_NAME(dtb.name, 'DATABASE', 'VIEW DATABASE STATE'), HAS_PERMS_BY_NAME(null, null, 'VIEW SERVER STATE')) AS bit) = 1
)
ORDER BY [Database_Name] ASC
"@

Write-Host "Query:" $testDbQuery
Write-Host ""
Write-Host "Expected result: Should return 'master' and 'iridium_wasm'"
Write-Host ""

# Build and run the test
Write-Host "Building test..." -ForegroundColor Yellow
cargo build --example test_ssms_full -p iridium_core 2>&1 | Out-Null

Write-Host "Running test..." -ForegroundColor Yellow
$output = cargo run --example test_ssms_full -p iridium_core 2>&1
Write-Host $output | Where-Object { $_ -match "Rows:|Result" }

Write-Host ""
Write-Host "=== Test Complete ===" -ForegroundColor Cyan


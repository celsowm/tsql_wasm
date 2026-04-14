@echo off
setlocal

set "IRIDIUM_SQL_DATA_DIR=%~dp0data"
if not exist "%IRIDIUM_SQL_DATA_DIR%" mkdir "%IRIDIUM_SQL_DATA_DIR%"

"%~dp0iridium-server.exe" %*

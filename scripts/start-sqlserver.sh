#!/bin/bash
set -e

REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$REPO_ROOT"

CREDENTIALS_PATH="$REPO_ROOT/scripts/credentials.json"
SQL_USER=$(jq -r '.sql_server_user' "$CREDENTIALS_PATH")
SQL_PASSWORD=$(jq -r '.sql_server_password' "$CREDENTIALS_PATH")

LOG_DIR="$REPO_ROOT/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/sqlserver-container.log"

log_line() {
    local timestamp=$(date +"%Y-%m-%d %H:%M:%S.%3N")
    echo "[$timestamp] $1" >> "$LOG_FILE"
    echo "$1"
}

echo "Ensuring Podman machine is running..."
if ! podman machine list | grep -q "Currently running"; then
    echo "Starting Podman machine..."
    podman machine start >> "$LOG_FILE" 2>&1
    sleep 3
fi

echo "Checking tsql_test_sqlserver container..."
if ! podman ps -a --filter "name=tsql_test_sqlserver" --format "{{.Names}}" | grep -q "tsql_test_sqlserver"; then
    echo "Creating container SQL Server..."
    podman run -d --name tsql_test_sqlserver -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD="$SQL_PASSWORD" -p 11433:1433 --memory=512m mcr.microsoft.com/azure-sql-edge:latest >> "$LOG_FILE" 2>&1
else
    if ! podman ps --filter "name=tsql_test_sqlserver" --format "{{.Names}}" | grep -q "tsql_test_sqlserver"; then
        echo "Starting existing container..."
        podman start tsql_test_sqlserver >> "$LOG_FILE" 2>&1
    else
        echo "Container already running."
    fi
fi

echo "Waiting for SQL Server to become ready..."
MAX_RETRIES=30
RETRY=0

while [ $RETRY -lt $MAX_RETRIES ]; do
    sleep 2
    RETRY=$((RETRY+1))
    if podman exec tsql_test_sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U "$SQL_USER" -P "$SQL_PASSWORD" -Q "SELECT 1" > /dev/null 2>&1; then
        echo "SQL Server ready on localhost:11433"
        exit 0
    fi
    echo "Retry $RETRY/$MAX_RETRIES..."
done

echo "Timeout waiting for SQL Server"
exit 1

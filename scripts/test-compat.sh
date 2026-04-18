#!/bin/bash
set -e

REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$REPO_ROOT"

CREDENTIALS_PATH="$REPO_ROOT/scripts/credentials.json"
SQL_PASSWORD=$(jq -r '.sql_server_password' "$CREDENTIALS_PATH")

cleanup_podman() {
    if ! command -v podman >/dev/null 2>&1; then
        return
    fi

    if podman machine list 2>/dev/null | grep -q "Currently running"; then
        echo "Podman machine is running; stopping iridium_test_sqlserver before compatibility run..."
        if podman ps -a --filter "name=iridium_test_sqlserver" --format "{{.Names}}" 2>/dev/null | grep -q "iridium_test_sqlserver"; then
            if podman ps --filter "name=iridium_test_sqlserver" --format "{{.Names}}" 2>/dev/null | grep -q "iridium_test_sqlserver"; then
                podman stop iridium_test_sqlserver
            fi
        fi
        podman machine stop
    fi
}

echo "============================================="
echo " STEP 1: Azure SQL Edge (Podman)"
echo "============================================="

cleanup_podman

if ! podman machine list | grep -q "Currently running"; then
    echo "Starting Podman machine..."
    podman machine start
    sleep 3
else
    echo "Podman machine already running."
fi

if ! podman ps -a --filter "name=iridium_test_sqlserver" --format "{{.Names}}" | grep -q "iridium_test_sqlserver"; then
    echo "Creating Azure SQL Edge container..."
    podman run -d --name iridium_test_sqlserver \
        -e ACCEPT_EULA=Y \
        -e MSSQL_SA_PASSWORD="$SQL_PASSWORD" \
        -p 11433:1433 \
        --memory=512m \
        mcr.microsoft.com/azure-sql-edge:latest
else
    if ! podman ps --filter "name=iridium_test_sqlserver" --format "{{.Names}}" | grep -q "iridium_test_sqlserver"; then
        echo "Starting existing container..."
        podman start iridium_test_sqlserver
    else
        echo "Container already running."
    fi
fi

echo "Waiting for Azure SQL Edge..."
AZURE_READY=false
for attempt in {1..30}; do
    if nc -z localhost 11433; then
        AZURE_READY=true
        break
    fi
    echo "  Attempt $attempt/30..."
    sleep 2
done

if [ "$AZURE_READY" = false ]; then
    echo "Azure SQL Edge not reachable!"
    exit 1
fi
echo "Azure SQL Edge ready."

echo ""
echo "============================================="
echo " STEP 2: Build"
echo "============================================="

if ! cargo build --package iridium_server --bin compat-query; then
    echo "Cargo build failed!"
    exit 1
fi
echo "compat-query built."

echo ""
echo "============================================="
echo " STEP 3: Compatibility Tests"
echo "============================================="

dotnet run --project scripts/compat-runner
TEST_EXIT=$?

exit $TEST_EXIT



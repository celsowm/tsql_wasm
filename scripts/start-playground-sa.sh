#!/bin/bash
set -e

REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$REPO_ROOT"

CREDENTIALS_PATH="$REPO_ROOT/scripts/credentials.json"
SQL_USER=$(jq -r '.sql_server_user' "$CREDENTIALS_PATH")
SQL_PASSWORD=$(jq -r '.sql_server_password' "$CREDENTIALS_PATH")

LOG_DIR="$REPO_ROOT/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/playground-sa.log"

export RUST_LOG="iridium_server=debug,iridium_core=info"

cleanup_podman() {
    if ! command -v podman >/dev/null 2>&1; then
        return
    fi

    if podman machine list 2>/dev/null | grep -q "Currently running"; then
        echo "Podman machine is running; stopping iridium_test_sqlserver before starting playground..."
        if podman ps -a --filter "name=iridium_test_sqlserver" --format "{{.Names}}" 2>/dev/null | grep -q "iridium_test_sqlserver"; then
            if podman ps --filter "name=iridium_test_sqlserver" --format "{{.Names}}" 2>/dev/null | grep -q "iridium_test_sqlserver"; then
                podman stop iridium_test_sqlserver
            fi
        fi
        podman machine stop
    fi
}

cleanup_podman

echo "Starting iridium-server playground on localhost:1433 with TLS and $SQL_USER / $SQL_PASSWORD..."
echo "Use Server Name = localhost in SSMS."
echo "Writing server log to $LOG_FILE"

cargo run --package iridium_server --bin iridium-server -- \
    --playground \
    --memory \
    --tls-gen \
    --host 127.0.0.1 \
    --port 1433 \
    --user "$SQL_USER" \
    --password "$SQL_PASSWORD" 2>&1 | tee -a "$LOG_FILE"


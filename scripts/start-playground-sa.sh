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

export RUST_LOG="tsql_server=debug,tsql_core=info"

echo "Starting tsql-server playground on localhost:1433 with TLS and $SQL_USER / $SQL_PASSWORD..."
echo "Use Server Name = localhost in SSMS."
echo "Writing server log to $LOG_FILE"

cargo run --package tsql_server --bin tsql-server -- \
    --playground \
    --tls-gen \
    --host 127.0.0.1 \
    --port 1433 \
    --user "$SQL_USER" \
    --password "$SQL_PASSWORD" 2>&1 | tee -a "$LOG_FILE"

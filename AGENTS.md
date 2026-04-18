# Agent Guidelines for Iridium SQL

## Testing

### Running Tests

**Core Engine Tests (Recommended for development):**
```bash
cargo test -p iridium_core
cargo test -p iridium_wasm
```

**Server Integration Tests (Requires Podman):**
```bash
podman machine start
podman start iridium_test_sqlserver
cargo test -p iridium_server
```

### Podman Container Setup

The project uses a SQL Server container for integration tests:

**Start Podman Machine:**
```bash
podman machine start
```

**Start SQL Server Container:**
```bash
podman start iridium_test_sqlserver
```

**Container Details:**
- Image: `mcr.microsoft.com/azure-sql-edge:latest`
- Container Name: `iridium_test_sqlserver`
- Port: 11433 (host) -> 1433 (container)
- SA Password: `Test@12345`

**Docker Compose:**
```bash
podman-compose up -d sqlserver
# or
podman-compose start sqlserver
```

### Known Test Issues

**iridium_server Integration Tests:**
- Currently marked as `#[ignore]` due to TDS handshake incompatibility with tiberius 0.12
- Tests fail with "early eof" during PRELOGIN/LOGIN handshake
- Root cause: Client requests encryption (ENCRYPT_ON) but server has TLS disabled
- To fix: Either enable TLS in test server with self-signed cert, or downgrade tiberius to 0.11

**Workaround:** Run integration tests with `--ignored` flag if you've fixed the TLS issue:
```bash
cargo test -p iridium_server -- --ignored
```

## Code Style

- Follow existing Rust conventions in the codebase
- Use `#[allow(dead_code)]` for intentionally unused fields
- Use `#[allow(unused_assignments)]` for incomplete feature code
- Prefix unused variables with underscore (`_var`)
- Keep debug logging with `eprintln!` during development, remove before commit

## Architecture

- `iridium_core`: T-SQL engine (parser, executor, storage)
- `iridium_server`: TDS 7.4 protocol server
- `iridium_wasm`: WebAssembly bindings

## Common Tasks

**Fix compilation errors:**
1. Check for missing exports in `iridium_core/src/lib.rs`
2. Verify all imports are used or marked with `#[allow(unused_imports)]`

**Fix test failures:**
1. Run with `--nocapture` to see debug output
2. Check if test requires Podman container
3. Look for parser bugs in statement parsing logic

**Add new features:**
1. Add AST types in `iridium_core/src/ast/`
2. Add parser in `iridium_core/src/parser/statements/`
3. Add executor in `iridium_core/src/executor/`
4. Update exports in `iridium_core/src/lib.rs`

**Keep versions synchronized before commits:**
1. Run `pwsh scripts/install-hooks.ps1` once per clone to enable the local pre-commit hook.
2. The hook runs `scripts/version-sync.mjs bump` so npm and crate versions stay aligned automatically.


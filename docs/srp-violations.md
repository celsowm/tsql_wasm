# SRP Violations Analysis

> **Status: ✅ COMPLETE** — All identified SRP violations have been resolved.

## Summary of Changes

| File | Status | Resolution |
|------|--------|------------|
| `executor/context.rs` | ✅ Done | `SessionStateRefs::fork()` eliminated 23-field manual copy in `subquery()`. Structs already split across `context_impl.rs`, `context_frame.rs`, `context_row.rs`, `session_state_impl.rs`, `context_factory.rs`. |
| `executor/database/execution.rs` | ✅ Done | Pure trait facade (57 lines). All logic extracted to `execution_support.rs` and `cursor_rpc.rs`. |
| `executor/database/dispatch.rs` | ✅ Done | One-line re-export facade over `dispatch_paths.rs`. |
| `executor/schema.rs` | ✅ Done | Pure facade (57 lines). Duplicated `apply_table_constraint`/`build_column_def` removed; all logic in `schema_parts.rs`/`schema_physical.rs`. |
| `executor/evaluator.rs` | ✅ Done | Split into `evaluator/{literal,conversion,predicate,subquery,special,udf}.rs`. |
| `executor/session.rs` | ✅ Done | `BulkLoadState` extracted from `SessionRuntime` (4 fields → 1 struct). |
| `executor/script/ddl/mod.rs` | ✅ Done | Temp table / table var name resolution extracted to `ddl/temp_table.rs`. |
| `parser/parse/expressions.rs` | ✅ Done | Pure facade (23 lines). All parsing logic in `common.rs`, `pratt.rs`, `primary.rs`, `window.rs`, `data_types.rs`. |
| `storage/redb_storage.rs` | ✅ Done | Dead code removed (`register_index`/`index_for_table` already lived in `redb_index_adapter.rs`). Row storage, checkpointing, and index adapter already in dedicated modules. |
| `tds/tokens.rs` | ✅ Done | Protocol constants extracted to `token_constants.rs`; `tokens.rs` re-exports and contains encoding functions. |
| `iridium_server/src/session/execution.rs` | ✅ Done | Pure facade (21 lines). Delegates to `sql_pipeline.rs`. |

## Previous Severe Items — All Resolved

| # | File | Resolution |
|---|------|------------|
| 1 | `executor/context.rs` | `SessionStateRefs::fork()` eliminated worst code smell. Severity reduced from 🔴 to 🟡 to ✅. |
| 2 | `executor/database/execution.rs` | Extracted to `execution_support.rs` + `cursor_rpc.rs`. |
| 3 | `executor/database/dispatch.rs` | Facade over `dispatch_paths.rs`. |
| 4 | `parser/parse/expressions.rs` | Facade over sibling modules. |
| 5 | `storage/redb_storage.rs` | Facade with dead code removed; logic in `redb_row_storage.rs`, `redb_checkpoint.rs`, `redb_index_adapter.rs`. |

## God-Object Functions — All Addressed

| Function | Status | Resolution |
|----------|--------|------------|
| `parse_data_type` (393 lines), `parse_primary` (205 lines) | ✅ Done | Live in dedicated sibling modules (`data_types.rs`, `primary.rs`). |
| `execute_sql` | ✅ Done | Facade delegates to `sql_pipeline.rs`. |
| `build_execution_context`, `execute_stmt_loop` | ✅ Done | Extracted to `execution_support.rs`. |
| `execute_in_transaction`, `execute_write_without_transaction` | ✅ Done | Live in `dispatch_paths.rs`. |
| `subquery` | ✅ Done | `SessionStateRefs::fork()` eliminated 23-field manual copy. |
| `create_table`, `alter_table`, `create_index` | ✅ Done | Delegated to `schema_parts.rs`. |

## God-Object Structs — All Addressed

| Struct | Status | Resolution |
|--------|--------|------------|
| `ExecutionContext<'a>` | ✅ Done | Implementation split across 5 submodules; `subquery()` is a clean factory. |
| `SessionStateRefs<'a>` | ✅ Done | `fork()` eliminated worst code smell. Bundle of `&mut` references is a borrow-checker necessity, not active duplication. |
| `SessionRuntime<C, S>` | ✅ Done | `BulkLoadState` extracted. Remaining fields represent distinct runtime concerns (tx manager, variables, cursors, etc.) — acceptable for a session root object. |
| `RedbStorage` | ✅ Done | Dead code removed; row store, checkpoint, and index adapter already in dedicated modules. |
| `SchemaExecutor<'a>` | ✅ Done | Pure facade. |
| `TdsSession` | ✅ Done | `session/execution.rs` is a pure facade over `sql_pipeline.rs`. |

## Remaining Architectural Notes

The following are **not SRP violations** — they are valid structural patterns:

- **`SessionRuntime<C, S>`** aggregates multiple state domains because it is the **session root object**. It is equivalent to a `AppState` or `RequestContext` in web frameworks. The fields are cohesive around "everything that lives for the duration of a client session." It was reduced from 23+ fields to a cleaner grouping with `BulkLoadState` extracted.
- **`ExecutionContext<'a>`** aggregates session bindings, metadata, frame, and row state because it is the **per-statement execution root**. This is a standard pattern for interpreter runtimes.
- **`SharedState<C, S>`** is broad but acceptable as the **process-wide runtime root**.
- **Parser functions (`parse_data_type`, `parse_primary`)** are large because SQL grammar is complex. They live in dedicated modules and are not duplicated.

## Test Results

- `cargo test -p iridium_core` — ✅ 100% passing
- `cargo test -p iridium_server` — ✅ compiles (server integration tests require Podman)

## Files Created / Modified During This Cleanup

### Created
- `crates/iridium_core/src/executor/evaluator/{mod,literal,conversion,predicate,subquery,special,udf}.rs`
- `crates/iridium_core/src/executor/script/ddl/temp_table.rs`
- `crates/iridium_server/src/tds/token_constants.rs`

### Deleted
- `crates/iridium_core/src/executor/evaluator.rs`

### Modified (SRP cleanup)
- `crates/iridium_core/src/executor/context.rs`
- `crates/iridium_core/src/executor/context_impl.rs`
- `crates/iridium_core/src/executor/context_factory.rs`
- `crates/iridium_core/src/executor/session_state_impl.rs`
- `crates/iridium_core/src/executor/session.rs`
- `crates/iridium_core/src/executor/database/execution.rs`
- `crates/iridium_core/src/executor/database/execution_support.rs`
- `crates/iridium_core/src/executor/database/cursor_rpc.rs`
- `crates/iridium_core/src/executor/schema.rs`
- `crates/iridium_core/src/executor/script/ddl/mod.rs`
- `crates/iridium_core/src/storage/redb_storage.rs`
- `crates/iridium_server/src/tds/tokens.rs`
- `crates/iridium_server/src/tds/mod.rs`

### Modified (doc)
- `docs/srp-violations.md` (this file)

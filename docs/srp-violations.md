# SRP Violations Analysis

## Status Atual

| File | Status | Notes |
|------|--------|-------|
| `executor/context.rs` | 🟠 Partial | Split into `context.rs`, `context_impl.rs`, `session_state_impl.rs`, and `context_factory.rs`; still centralizes frame/row/session glue behind compatibility wrappers. |
| `executor/database/execution.rs` | 🟠 Partial | Helpers extracted, but the file still owns coordination between session access, cursor RPC, scope cleanup, and batch execution. |
| `executor/database/dispatch.rs` | 🟠 Partial | Policy helpers were extracted, but routing/transaction branches still live in the file. |
| `parser/parse/expressions.rs` | 🟢 Mostly done | Broken into `common.rs`, `pratt.rs`, `primary.rs`, `window.rs`, and `data_types.rs`; the facade remains for compatibility. |
| `storage/redb_storage.rs` | 🟢 Mostly done | Split into row storage, checkpointing, and index adapter modules. |
| `executor/schema.rs` | 🟠 Partial | DDL entrypoints now delegate to `schema_parts.rs` and `schema_physical.rs`, but the facade still coordinates multiple object kinds. |
| `iridium_server/src/session/execution.rs` | 🟢 Done | SQL preprocessing and packet/result handling are split through `sql_pipeline.rs`. |

## 🔴 Severe (Top Priority)

| # | File | Problem | Recommended Split |
|---|------|---------|-------------------|
| 1 | `executor/context.rs` | 🟠 Partial: `ExecutionContext` + `SessionStateRefs` have been split across multiple files, but the compatibility layer still ties session, frame, and row behavior together. | `session_state.rs`, `frame_state.rs`, `row_context.rs`, `context_factory.rs` |
| 2 | `executor/database/execution.rs` | 🟠 Partial: support helpers were extracted, but the file still mixes batch orchestration, cursor RPC, and scope cleanup. | `session_access.rs`, `cursor_rpc.rs`, `batch_runner.rs`, `context_builder.rs` |
| 3 | `executor/database/dispatch.rs` | 🟢 Mostly done: routing now lives behind `dispatch_paths.rs`, while `dispatch.rs` is a facade. | `session_statements.rs`, `tx_dispatch.rs`, `read_dispatch.rs`, `write_dispatch.rs` |
| 4 | `parser/parse/expressions.rs` | 🟢 Mostly done: parser responsibilities were split into dedicated sibling modules; the file now acts as a facade. | `pratt.rs`, `primary.rs`, `functions.rs`, `window.rs`, `data_types.rs` |
| 5 | `storage/redb_storage.rs` | 🟢 Mostly done: row store, checkpointing, and index adaptation are now separated into dedicated modules. | `redb_row_store.rs`, `redb_checkpoint.rs`, `redb_index_adapter.rs` |

## 🟠 High

| # | File | Problem |
|---|------|---------|
| 6 | `executor/schema.rs` | 🟠 Partial: DDL entrypoints now delegate to `schema_parts.rs` and `schema_physical.rs`, but the facade still coordinates object kinds + storage side effects. |
| 7 | `session/execution.rs` (server) | 🟢 Done: SQL preprocessing and packet/result writing are separated via `sql_pipeline.rs`. |

## 🟡 Medium

| # | File | Problem |
|---|------|---------|
| 8 | `executor/evaluator.rs` | `eval_expr_inner` handles literals, operators, casts, subqueries, window lookup, sequences — too many categories |
| 9 | `executor/session.rs` | `SessionRuntime` is a data god object: session state + tx runtime + connection metadata + bulk load |
| 10 | `executor/script/ddl/mod.rs` | DDL dispatch + temp table name mapping + dirty-buffer propagation |
| 11 | `tds/tokens.rs` (server) | Protocol constants + metadata/row/envchange/error writing all in one file |

## ✅ Not SRP Issues (despite size)

`engine.rs`, `database/engine.rs`, `mod.rs`, `joins.rs`, `model.rs`, `server.rs`, `script/procedural/mod.rs` — these are thin facades or dispatchers, which is fine.

## Top God-Object Functions

- `parse_data_type` (393 lines), `parse_primary` (205 lines) — in `expressions.rs`
- `execute_sql` — mixes business logic + wire protocol
- `build_execution_context`, `execute_stmt_loop` — in `database/execution.rs`
- `execute_in_transaction`, `execute_write_without_transaction` — in `dispatch.rs`
- `create_table`, `alter_table`, `create_index` — in `schema.rs`

## Recommended Refactor Order

1. `executor/context.rs`
2. `executor/database/execution.rs`
3. `executor/database/dispatch.rs`
4. `parser/parse/expressions.rs`
5. `executor/schema.rs`
6. `storage/redb_storage.rs`
7. `iridium_server/src/session/execution.rs`

## Notes On Current Progress

- The parser, storage, and server-session items are now the most advanced splits in the current branch.
- `executor/context.rs` is still the largest remaining severe hotspot, but it has already been reduced from a single god file to a set of compatibility-oriented modules.
- `executor/database/execution.rs` is in a safer intermediate state: helper extraction is in place, but it still needs a second pass to fully separate policy from orchestration.
- `executor/database/dispatch.rs` is now mostly a facade over `dispatch_paths.rs`.
- `executor/schema.rs` is partially decomposed; the next step there is to push more of the DDL-specific logic out of the facade and into object-specific modules.
- `cargo test -p iridium_core` passes except for `concurrency_deadlock::test_deadlock_3_sessions`, which times out in this environment.

## Detailed File-by-File Assessment

### 1) `executor/engine.rs` — ✅ Low concern / OK

This is **not** a god object. It mostly holds `db`, owns a default session, and forwards calls to executor/analyzer/session manager. That is a valid facade responsibility. No heavy business logic. Keep as-is.

### 2) `executor/evaluator.rs` — 🟡 Medium concern

`eval_expr_inner` is the main issue. It handles:
- identifier resolution
- literal materialization
- scalar function dispatch
- binary/unary operators
- casts/convert/try-convert semantics
- CASE / IN / BETWEEN / LIKE
- subqueries / EXISTS / IN subquery
- window lookup
- sequence placeholder behavior

A central expression evaluator will always dispatch on `Expr`, so some branching is expected. The problem is that this function does more than dispatch: it also contains concrete behavior for many expression families.

**Recommended split**: Keep `eval_expr_inner` as a dispatcher, but move logic into `eval_literal_expr`, `eval_conversion_expr`, `eval_predicate_expr`, `eval_subquery_expr`, `eval_special_runtime_expr` (window/sequence).

`eval_udf_body` also mixes cloning catalog/storage, constructing `ScriptExecutor`, executing statements, and mapping `StmtOutcome` to scalar value.

### 3) `executor/mod.rs` — ✅ No SRP issue

This is just a module root. Large surface area is not an SRP problem by itself.

### 4) `executor/model.rs` — ✅ Low concern

Mostly data structures: `Group`, `BoundTable`, `ContextTable`, `Cursor`. Mostly cohesive. Mild smell: `Cursor` is session/runtime state living in a generic `model.rs`. Move `Cursor` to a cursor/session module if you touch cursor code later.

### 5) `parser/lexer.rs` — ✅ Low-to-medium concern

The file is cohesive around lexing. `lex()` handles all tokenization orchestration and quoted identifier mode branching. String/bracketed/quoted identifier parsing contains similar state-machine logic. Comments, whitespace, operators, punctuation, literals, and identifiers all live in one file. But this is still a single domain responsibility: lexical analysis. Not a real SRP violation yet.

### 6) `parser/parse/expressions.rs` — 🔴 Severe SRP violation

This file currently owns:
- Pratt/infix parsing
- primary expression parsing
- identifier/function disambiguation
- window function parsing
- CASE / CAST / CONVERT / TRY_* parsing
- generic comma list parsing
- stop-keyword policy
- SQL data type parsing

That is **multiple grammar subsystems** in one file.

**Worst functions:**
- `parse_pratt_expr` — parses infix operators, special syntax (`IS NULL`, `LIKE`, `BETWEEN`, `IN`, `NOT IN`, `NOT LIKE`, etc.), precedence logic, recursion control
- `parse_primary` — parses literals, variables, identifiers, functions, subqueries, unary ops, CASE, EXISTS, CAST/CONVERT, `NEXT VALUE FOR`, wildcard
- `parse_data_type` — duplicates logic for `Identifier` and `Keyword` token paths; does not belong in the same file as scalar expression parsing

**Recommended split**: `parse/expressions/pratt.rs`, `parse/expressions/primary.rs`, `parse/expressions/functions.rs`, `parse/expressions/window.rs`, `parse/types.rs`

### 7) `executor/session.rs` — 🟡 Medium concern

`SessionRuntime<C, S>` owns: clock, transaction manager, journal, variables, identities, temp/table vars, cursors, diagnostics, workspace, options, auth/client metadata, context info, session context, bulk-load state.

It is acting as: session state bag, transaction runtime bag, connection metadata bag, bulk import bag. This is a **data god object**.

`SharedState<C, S>` is also broad but more acceptable as the process-wide runtime root.

**Recommended split for `SessionRuntime`**: `SessionDataState`, `SessionExecutionState`, `SessionClientInfo`, `BulkLoadState`.

### 8) `executor/context.rs` — 🔴 Severe SRP violation

The file contains and couples: `SessionStateRefs`, `SessionMetadata`, `FrameState`, `WindowContext`, `RowContext`, `ExecutionContext`, scope cleanup guard, snapshot creation/restoration, subquery cloning, backward compatibility delegation layer.

**`ExecutionContext<'a>`** aggregates too much: mutable session refs, metadata snapshot, session options, frame state, row state, subquery cache.

**Worst functions:**
- `from_session` — constructs an enormous composite object from many unrelated pieces
- deprecated `new` — giant constructor with many parameters
- `subquery` — manually rebuilds a near-copy of the full context (context cloning + session view semantics + subquery setup)
- large delegation region — accessor forwarding indicates the public API is trying to hide too much internal structure

**`SessionStateRefs<'a>`** bundles many unrelated state domains: variables, identity tracking, temp tables, table vars, random state, cursors, diagnostics, bulk load state, context/session info, dirty buffer, identity insert.

**Recommended split**: `SessionBindings<'a>`, `ExecutionFrame`, `RowExecutionContext`, `SubqueryContextFactory`, `SnapshotManager`.

### 9) `executor/schema.rs` — 🟠 High concern

`SchemaExecutor` handles too many DDL concerns: types, tables, schemas, views, synonyms, sequences, indexes, alter-table mutations.

**Worst functions:**
- `create_table` — validates schema/table existence, collects column FK metadata, builds columns, applies table constraints, registers catalog entry, ensures physical storage, creates implied indexes
- `create_index` — catalog definition, index lookup, row scan, storage registration, index rebuild. Mixes **logical DDL** and **physical materialization**
- `alter_table` — dispatches many alter actions, mutates catalog, migrates stored rows, handles constraint removal, swallows some storage errors

**Recommended split**: `table_ddl.rs`, `index_ddl.rs`, `object_ddl.rs`

### 10) `executor/joins.rs` — ✅ Low concern / good enough

Small and reasonably cohesive: infer expression side, extract equi-join conditions, evaluate join keys. Not a problem.

### 11) `storage/redb_storage.rs` — 🔴 Severe SRP violation

`RedbStorage` does at least four jobs:
1. redb row persistence
2. in-memory index storage adapter
3. checkpoint/restore translation
4. serde-compatible persistence shim / initialization

Stores both `db: Option<Arc<Database>>` (persistent row store) and `indexes: BTreeMap<u32, BTreeIndex>` (in-memory index subsystem) — different responsibilities.

**Worst functions:**
- `insert_row` — meta-table management + append-position logic + serialization + insert
- `replace_table` — table wipe + meta reset + row rewrite
- `get_checkpoint_data` — full scan + conversion into in-memory checkpoint representation
- `restore_from_checkpoint` — global wipe + meta rebuild + row reinsertion

**Recommended split**: `redb_row_store.rs`, `redb_checkpoint.rs`, `redb_index_adapter.rs`

### 12) `iridium_server/src/server.rs` — ✅ Low concern

`TdsServer` does config validation, database/session pool setup, bind, accept loop, spawn sessions. Normal boundary object. Fine for now.

### 13) `iridium_server/src/session/execution.rs` — 🟠 High concern

**`execute_sql`** mixes: empty-batch handling, session validation, SSMS compatibility probes, `USE database` pre-processing, SQL execution, result post-processing, protocol token writing, packet writing, logging, error-to-response mapping, special sysdac probe coercion.

Mixes **business behavior** and **wire protocol serialization** in the same method.

`apply_use_database` mixes session state mutation, database engine update, and response packet construction/writing.

**Recommended split**: `sql_preprocessor.rs`, `batch_executor.rs`, `result_writer.rs`, `compat_probes.rs`

### 14) `tds/tokens.rs` — 🟡 Medium concern

Large file, but still fairly cohesive around TDS token encoding. Mixes protocol constants, textsize truncation policy, metadata/row/envchange/error writing, convenience result-set writer.

Split only if it keeps growing: `tokens/resultset.rs`, `tokens/messages.rs`, `tokens/envchange.rs`.

### 15) `executor/database/engine.rs` — ✅ Low concern / good facade

Thin wrapper that delegates. Not the problem.

### 16) `executor/database/execution.rs` — 🔴 Severe SRP violation

Mixes: session locking/access helper (`with_session`), trait implementation (`StatementExecutor`), cursor RPC API, bulk load API, execution context construction, statement loop semantics, batch execution, scope cleanup of table vars.

**Worst functions:**
- `cursor_rpc_open` — parse SQL, validate SELECT, allocate cursor, mutate session state, execute query, store result into cursor
- `build_execution_context` — massive destructuring of session runtime, constructs context using deprecated constructor
- `execute_stmt_loop` — statement iteration, tx-state sync, identity_insert sync, tx/non-tx branching, control-flow semantics, deadlock handling, result collection callback
- `execute_batch_core_inner` — creates context, enters scope, runs body, captures current DB, scope cleanup, physical table cleanup, writes current DB back to session

**Recommended split**: `session_access.rs`, `cursor_rpc.rs`, `batch_runner.rs`, `context_builder.rs`

### 17) `executor/database/dispatch.rs` — 🔴 Severe SRP violation

Handles: implicit transaction start policy, deadlock priority lookup, session statement handling, read committed workspace refresh, tx state updates, in-tx execution, auto-commit write/read execution, dirty read execution, FMTONLY/NOEXEC metadata mode.

**Worst functions:**
- `handle_session_statement` — handles unrelated session statement families
- `execute_in_transaction` — locking, context option sync, isolation policy, dirty/read-committed branching, workspace handling, execution, transaction state update
- `execute_write_without_transaction` — locking, storage snapshotting, execution, commit timestamp/version mutation, durability checkpointing, rollback on failure, WAL interaction
- `execute_non_transaction_statement` — high-level dispatcher with many policy branches

**Recommended split**: `session_statements.rs`, `tx_dispatch.rs`, `read_dispatch.rs`, `write_dispatch.rs`

### 18) `executor/script/ddl/mod.rs` — 🟡 Medium concern

Mixes DDL dispatch, temporary table naming/mapping (`#...`), table variable resolution (`@...`), direct storage mutation, dirty-buffer propagation.

**Worse methods:**
- `execute_alter_table` — name resolution, schema execution, row loading, dirty-buffer synchronization
- `execute_truncate_table` — resolution, catalog lookup, storage mutation, mutation executor dirty tracking

Keep `execute_ddl` as dispatch only, move temp/table-var name resolution into a separate adapter.

### 19) `executor/script/procedural/mod.rs` — ✅ Low concern / acceptable

Mostly a dispatcher over `ProceduralStatement`. Does not contain deep behavior. Fine.

## God-Object Structs

| Struct | Severity | Reason |
|--------|----------|--------|
| `ExecutionContext<'a>` | 🔴 High | session bindings, metadata, options, frame stack, row/group/window/cte state, subquery cache, cloning/forking behavior, scope management delegation |
| `SessionStateRefs<'a>` | 🔴 High | borrowed aggregation of many unrelated session domains — a "mutable bag of everything execution might touch" |
| `SessionRuntime<C, S>` | 🟠 Medium/High | data state + execution state + client metadata + bulk load state bundled together |
| `RedbStorage` | 🔴 High | persistent rows + in-memory indexes + checkpoint bridge |
| `SchemaExecutor<'a>` | 🟠 Medium/High | all DDL object categories + physical storage side effects |
| `TdsSession` | 🟡 Medium | connection/session state, SQL preprocessing, execution dispatch, result serialization/writing, protocol compatibility quirks |

## Risks and Guardrails

### Risks
- Refactoring `ExecutionContext` can break borrow relationships and lifetimes
- Splitting dispatch code can subtly alter transaction semantics
- Splitting parser code can change precedence or SQL edge-case handling
- Splitting storage code can break checkpoint/index behavior

### Guardrails
- Add/keep tests around:
  - transaction isolation + implicit transactions
  - deadlock/xact_abort behavior
  - `SET` options
  - temp tables / table variables / scope cleanup
  - cursor RPC open/fetch/close/deallocate
  - parser precedence and CAST/CONVERT/CASE/IN/BETWEEN/LIKE
  - checkpoint restore and index rebuild
- Refactor by **extracting helpers first**, moving files second
- Keep the public trait surfaces stable initially

## Target Architecture (Advanced Path)

If incremental cleanup is not enough, consider a clear split between:

- **`ExecutionContext`** = minimal per-statement runtime state
- **`SessionRuntime`** = persistent session state only
- **`Dispatcher`** = chooses execution path
- **`StatementRunner`** = executes one statement in one mode
- **`SchemaService`** = logical DDL only
- **`PhysicalSchemaApplier`** = storage/index side effects
- **`TdsResultWriter`** = packet/token writing only

Only pursue this after the incremental splits above.

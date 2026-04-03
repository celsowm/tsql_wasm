# Anti-Pattern Audit: `tsql_core`

## 🔴 P0 — Correctness / Wrong-Result Bugs

| # | Issue | File | Description |
|---|---|---|---|
| 1 | **Ambiguous column returns first non-null** | `identifier.rs:101-112` | When multiple tables have the same column name, it silently returns the first non-null value instead of erroring. Produces wrong results. |
| 2 | **`ORDER BY` silently falls back to column 0** | `projection.rs:103` | `unwrap_or(0)` — unresolved ORDER BY sorts by the wrong column instead of erroring. |
| 3 | **`update_rows` inverted error semantics** | `storage/mod.rs:79-84` | Uses `HashMap::insert()` which returns `None` for *new* keys, then treats that as an error. Semantically backwards. |
| 4 | **Single vs batch scope inconsistency** | `execution.rs` | `execute_single_statement` doesn't call `enter_scope()`, so variable/table-var lifetime differs by API entry point. |
| 5 | **`_multi` cleanup bug on BREAK/CONTINUE** | `execution.rs:318-327` | Returns early with an error but skips scope cleanup, leaking scoped table vars/temp state. |
| 6 | **`_multi` cleans up against wrong storage on errors** | `execution.rs:289-296` | Error path always cleans against shared storage even when a transaction workspace is active. |
| 7 | **`@@ERROR` hardcoded to 0, `@@SPID` to 1** | `identifier.rs:15,23` | `last_error` and `session_id` exist in context but aren't wired to these globals. |
| 8 | **`schema_or_dbo()` hardcodes `"dbo"`** | `catalog/mod.rs:91` | Multi-schema support is structurally broken. |
| 9 | **Ignores `update_rows` error in workspace refresh** | `dispatch.rs:100` | `let _ = workspace.storage.update_rows(...)` can silently desync workspace. |

## 🟠 P1 — Structural / Architectural Anti-Patterns

| # | Issue | File | Description |
|---|---|---|---|
| 10 | **`ExecutionContext` god struct (30+ fields)** | `context.rs` | Mixes session state, row-eval state, module stack, transaction metadata, and diagnostics. |
| 11 | **3 near-identical context copy methods** | `context.rs:132-260` | `subquery()`, `with_outer_row()`, `with_outer_row_extended()` — ~40 lines each, manually duplicated. |
| 12 | **Constructor with 13+ `&mut` params** | `context.rs:74-130` | Repeated verbatim 3 times in `execution.rs`. |
| 13 | **`execute_batch_statements` / `_multi` duplication** | `execution.rs` | ~100 lines of nearly identical logic, each a drift target. |
| 14 | **`ControlFlow` duplicates `StmtOutcome`** | `context.rs:27-31` vs `error.rs:55-64` | Two parallel control-flow mechanisms with divergence risk. |
| 15 | **`eval_udf_body` downcast cloning** | `evaluator.rs:65-77` | Hardcodes `CatalogImpl`, `InMemoryStorage`, `RedbStorage` — breaks open/closed principle. |
| 16 | **Full catalog/storage clone per UDF call** | `evaluator.rs:65-91` | O(database-size) per scalar UDF invocation, disastrous in row-level use. |
| 17 | **Full-state cloning as transaction model** | `transaction.rs:126-137` | `begin()`/`save()` clone entire catalog+storage. O(data) per txn/savepoint. |
| 18 | **`Catalog` trait leaks `&mut Vec<T>`** | `catalog/mod.rs:159-160` | `get_tables_mut()` lets callers bypass invariants; couples forever to `Vec`. |
| 19 | **Identifier resolution in inner eval loop** | `identifier.rs` | Per-row linear column name scan — binder/planner work done at eval time. |
| 20 | **Read-only SELECT takes global write lock** | `dispatch.rs:177` | `state.storage.write()` serializes reads. |
| 21 | **READ COMMITTED refreshes all tables on every SELECT** | `dispatch.rs:91-112` | O(all-tables × all-rows) refresh per query. |
| 22 | **`window_context` keyed by AST expressions** | `context.rs:54` | Using AST nodes as runtime lookup keys is brittle. |

## 🟡 P2 — Code Smells / Cleanup

| # | Issue | File | Description |
|---|---|---|---|
| 23 | **`deduplicate_projected_rows` O(n²) with string keys** | `projection.rs:127-138` | Uses `Vec<String>` + `contains()` instead of `HashSet<Vec<Value>>`. |
| 24 | **Duplicated `to_i64` already diverged** | `types/mod.rs:235` vs `value_helpers.rs:11` | One handles `SqlVariant`, the other doesn't. |
| 25 | **Ad hoc string normalization everywhere** | Many files | `to_uppercase()`, `eq_ignore_ascii_case()`, `"DBO."` stripping scattered across engine. |
| 26 | **Unused `_current_row` param** | `context.rs:219` | `with_outer_row_extended` — dead parameter, design fossil. |
| 27 | **Dead file `scalar_fn.rs`** | `scalar_fn.rs` | Contains only a deprecation comment. |
| 28 | **Likely dead context fields** | `context.rs` | `session_database`, `session_user`, `session_app_name`, `session_host_name` — appear unwired. |
| 29 | **Manual scope lifecycle (no RAII guard)** | `execution.rs` | `enter_scope()`/`cleanup_scope_table_vars()` manually paired — directly causes bug #5. |
| 30 | **Duplicate `LockManager` vs `LockTable`** | `transaction.rs:32` vs `locks/` | Two lock-tracking mechanisms with unclear authority. |

## 🔵 SOLID Violations

### S — Single Responsibility Principle

| # | Issue | File | Description |
|---|---|---|---|
| S1 | **`ExecutionContext` owns 5+ unrelated concerns** | `context.rs` | Session state (user, app, host), variable scoping, control flow, cursor management, transaction metadata, identity tracking, dirty buffering, and row context all live in one struct. Each is an independent reason to change. |
| S2 | **`ScriptExecutor::execute()` is a 250-line dispatcher + DDL/DML/procedural executor** | `script/mod.rs` | One method matches ~40 statement variants, creates `SchemaExecutor` inline, builds `RoutineDef`/`TriggerDef` objects, and handles cursor registration. It's a router, a factory, and an executor in one. |
| S3 | **`dispatch.rs` handles locking, execution, durability, dirty reads, and rollback** | `database/dispatch.rs` | `execute_non_transaction_statement()` is a ~230-line function that acquires locks, selects isolation strategy, builds workspace snapshots, runs scripts, persists checkpoints, handles errors, and releases locks. Each is a separate concern. |
| S4 | **`CatalogImpl` is both a data store and a domain service** | `catalog/mod.rs` | It stores all schema objects (tables, indexes, routines, views, triggers, types) AND provides allocation logic, lookup logic, and mutation logic. Adding any new object type requires modifying this single struct. |
| S5 | **`tooling.rs` is a 1200+ line grab-bag** | `tooling.rs` | Contains `SessionOptions`, `CompatibilityReport`, `ExplainPlan`, `ExecutionTrace`, SQL analysis, statement slicing, SET option handling, and routine formatting — all unrelated features in one file. |
| S6 | **`SchemaExecutor` handles DDL + constraint logic + row migration** | `schema.rs` | `create_table()` builds columns, processes constraints (PK, FK, unique, check, default), and manages storage. `alter_table()` also migrates existing row data. Table creation and row data migration are separate responsibilities. |

### O — Open/Closed Principle

| # | Issue | File | Description |
|---|---|---|---|
| O1 | **`eval_udf_body` hardcodes concrete storage types** | `evaluator.rs:65-77` | Uses `as_any().downcast_ref::<InMemoryStorage>()` and `downcast_ref::<RedbStorage>()`. Adding a new `Storage` implementation requires modifying this function. Should use a `clone_boxed()` method on the trait instead. |
| O2 | **`ScriptExecutor::execute()` requires modification for every new statement type** | `script/mod.rs` | Every new T-SQL statement type requires adding a branch to this giant match. There's no dispatch table, visitor pattern, or statement handler registry. |
| O3 | **`resolve_identifier` hardcodes all `@@` globals** | `identifier.rs:12-28` | Every new system variable requires adding a match arm. A lookup table or registry would make this extensible without modification. |
| O4 | **`Statement` enum growth forces changes across the entire executor** | `script/mod.rs`, `dispatch.rs`, `table_util.rs`, `tooling.rs` | Adding a statement requires touching the AST enum, parser, script executor, transaction exec, table_util collectors, and tooling analyzer — 6+ files minimum. |
| O5 | **`DbError` enum requires modification for new error categories** | `error.rs` | `Parse`, `Semantic`, `Execution`, `Storage`, `Deadlock` — adding a new error class (e.g., `Timeout`, `Permission`) requires changing the enum and all match arms (`class()`, `code()`). |

### L — Liskov Substitution Principle

| # | Issue | File | Description |
|---|---|---|---|
| L1 | **`Storage::update_rows` has inconsistent contract** | `storage/mod.rs:79-84` | `InMemoryStorage::update_rows` uses `HashMap::insert` which succeeds on new keys but the error check assumes the key must exist. The method's behavior doesn't match its semantic contract ("update existing rows"), and `RedbStorage` may behave differently — callers can't substitute implementations safely. |
| L2 | **`Catalog` trait requires `as_any()` for downcasting** | `catalog/mod.rs:208-209` | The trait exposes `as_any()` and `as_any_mut()`, which exist solely so callers can downcast to concrete types. This defeats the purpose of the trait abstraction — if you need the concrete type, the trait isn't providing the right interface. A true LSP-compliant design would add `clone_boxed()` to the trait. |
| L3 | **`eval_udf_body` rejects unknown `Storage` implementations** | `evaluator.rs:75-77` | Returns `Err("UDF requires cloneable storage")` for any Storage implementation that isn't `InMemoryStorage` or `RedbStorage`. A new Storage impl that passes all trait requirements still fails at runtime — violating substitutability. |
| L4 | **`SessionStatement` variants rejected at runtime by `ScriptExecutor`** | `script/mod.rs:123-125, 263-270` | `SetOption`, `SetIdentityInsert`, `SetTransactionIsolationLevel` are valid `Statement` variants but `ScriptExecutor::execute()` returns hard errors for them. The caller must know which executor handles which statement variants — the `Statement` type doesn't communicate this. |

### I — Interface Segregation Principle

| # | Issue | File | Description |
|---|---|---|---|
| I1 | **`Catalog` trait has 30+ methods** | `catalog/mod.rs:154-209` | Any implementation must provide schema management, table CRUD, index CRUD, routine CRUD, view CRUD, trigger CRUD, table type CRUD, ID allocation, and `as_any()`. Most callers need only 2-3 of these. Should be split into `SchemaRegistry`, `TableRegistry`, `RoutineRegistry`, etc. |
| I2 | **`Storage` trait forces checkpoint support on all implementations** | `storage/mod.rs:22-36` | `get_checkpoint_data()` and `restore_from_checkpoint()` are required even if a storage backend doesn't support checkpointing. A read-only or streaming storage would have to stub these out. |
| I3 | **`ExecutionContext` exposes all state to all callers** | `context.rs:33-71` | Every function that takes `&mut ExecutionContext` can access cursors, print output, dirty buffers, session metadata, transaction state, etc. — most only need variables and row context. No narrowing of the interface is possible. |
| I4 | **`DatabaseInner` implements 4 unrelated traits via one `Arc<SharedState>`** | `database/` | `StatementExecutor`, `CheckpointManager`, `SessionManager`, `SqlAnalyzer`, and `RandomSeed` are all implemented on `DatabaseInner`. Callers that need only session management are forced to depend on the entire execution and persistence stack. |
| I5 | **`DurabilitySink` couples persistence with recovery** | `durability.rs:38-41` | `persist_checkpoint()` and `latest_checkpoint()` are in the same trait. A write-only audit log sink and a recovery-capable sink have different requirements but must implement both. |

### D — Dependency Inversion Principle

| # | Issue | File | Description |
|---|---|---|---|
| D1 | **`eval_udf_body` depends on concrete `CatalogImpl` and `InMemoryStorage`** | `evaluator.rs:65-77` | Core evaluation logic directly references concrete types instead of going through trait interfaces. The high-level evaluator module depends on low-level implementation details. |
| D2 | **`database/mod.rs` hardcodes type aliases to concrete implementations** | `database/mod.rs:64-68` | `type Database = DatabaseInner<CatalogImpl, InMemoryStorage>` and `type Engine = EngineInner<CatalogImpl, InMemoryStorage>` bake concrete types into the public API. Code using `Database` or `Engine` is transitively coupled to `CatalogImpl` and `InMemoryStorage`. |
| D3 | **`SchemaExecutor` is created inline with raw catalog/storage refs** | `script/mod.rs:42-88` | `SchemaExecutor { catalog: self.catalog, storage: self.storage }` is repeated 8 times. Instead of injecting the dependency, the executor constructs its collaborator inline, coupling the two. |
| D4 | **`dispatch.rs` directly constructs `ScriptExecutor`** | `database/dispatch.rs:80-85,114-119,124-129,179-183,204-208` | The dispatch layer creates `ScriptExecutor` 5 times with raw refs. There's no factory or dependency injection — changing how script execution is configured requires modifying the dispatch layer. |
| D5 | **`EngineInner` delegates to concrete `DatabaseInner` via public field** | `database/engine.rs:24` | `pub db: DatabaseInner<C, S>` — Engine exposes its internal database as a public field. Higher-level code reaches through Engine to access Database internals, creating tight coupling. |
| D6 | **`persistence.rs` directly references `parking_lot`, `dashmap`, and `Arc`** | `database/persistence.rs` | Concurrency primitives are hardwired into the domain layer rather than abstracted. Testing or swapping concurrency strategies requires changing the persistence module. |

## Recommended Fix Order

### Phase 1 — Correctness Patches (P0)

Fix bugs #1-9. These can produce wrong results or corrupt state.

1. Make ambiguous identifier resolution error out
2. Remove `ORDER BY` fallback-to-column-0; pre-resolve and error if unresolved
3. Fix `update_rows` semantics and stop ignoring its result
4. Make `execute_single_statement()` use the same scope lifecycle as batch
5. Fix `_multi` cleanup on all early returns
6. Fix `_multi` cleanup target when transaction workspace is active
7. Wire `@@ERROR` / `@@SPID` correctly or remove dead state
8. Replace `schema_or_dbo()` hardcode or block multi-schema features until fixed
9. Stop ignoring `update_rows` errors in workspace refresh

### Phase 2 — Collapse Execution Duplication (P1)

1. Extract `build_execution_context(session, state, session_id)` helper
2. Unify `execute_batch_statements` / `_multi` into one batch driver parameterized by result collection strategy
3. Add a small RAII scope guard so cleanup always runs (fixes #29)
4. Standardize on one control-flow representation: `StmtOutcome` (remove `ControlFlow`)

### Phase 3 — Tame Core Abstractions (P1-P2)

1. Split `ExecutionContext` into smaller borrowed subcontexts:
   - `SessionBindings<'a>`: borrowed mutable session state
   - `ExecutionFrame`: per-statement/module/loop/control state
   - `RowContext`: outer/apply/current group/window state
2. Stop exposing mutable `Vec`s from `Catalog` trait
3. Add lookup maps to `CatalogImpl` keyed by normalized names/IDs
4. Remove downcast cloning in UDF execution
5. Unify duplicated `to_i64` helpers
6. Replace `deduplicate_projected_rows` with `HashSet<Vec<Value>>`
7. Remove dead file `scalar_fn.rs` and unused context fields

### Phase 4 — SOLID Remediation (S/O/L/I/D)

1. **Split `Catalog` trait** into `SchemaRegistry`, `TableRegistry`, `RoutineRegistry`, `ViewRegistry`, `TriggerRegistry` (I1, S4)
2. **Add `clone_boxed()` to `Catalog` and `Storage` traits**, remove `as_any()`/`as_any_mut()` (L2, L3, O1, D1)
3. **Separate `Storage` from `CheckpointableStorage`** via a sub-trait (I2)
4. **Split `tooling.rs`** into `session_options.rs`, `compatibility.rs`, `explain.rs`, `trace.rs` (S5)
5. **Extract `SchemaExecutor` creation** into `ScriptExecutor` as a helper method to DRY the 8 inline constructions (D3)
6. **Split `DatabaseInner` traits** into separate service structs or use delegation instead of one god implementor (I4)
7. **Move `@@` globals to a registry/lookup table** instead of hardcoded match arms (O3)
8. **Make `Statement` dispatch extensible** — consider a visitor or handler-map pattern for `ScriptExecutor::execute()` (O2, O4)
9. **Hide `EngineInner::db` field** behind methods, stop leaking internal structure (D5)

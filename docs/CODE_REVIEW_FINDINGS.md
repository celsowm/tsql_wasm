# Code Review Findings: TSQL-WASM Core Engine

This document summarizes the anti-patterns, performance bottlenecks, and architectural concerns identified during the review of the `tsql_core` crate.

## 1. Anti-Patterns

### 1.1 Monolithic Files and "God Objects"
- **DONE:** `crates/tsql_core/src/parser/lower.rs` has been removed. The logic is now correctly split into `parser/lower/*.rs` (DML, DDL, Procedural, Common), following SRP.
- **IMPROVED:** `crates/tsql_core/src/executor/window.rs` has been refactored to be more efficient, though it remains a complex module due to the nature of window functions.
- **IMPROVED:** `ExecutionContext` now delegates scope/session/row responsibilities to their owning sub-structs, but it is still a large coordinator type.

### 1.2 "Stringly-Typed" Programming
- Identifiers are frequently passed as raw `String` objects.

### 1.3 Panic Risks
- **DONE:** Production `unwrap()` call sites in `parser/` and `executor/` have been replaced with explicit error handling or defensive fallbacks.
- **DONE:** Stale `unwrap()` calls in the old `lower.rs` are gone. `lower_object_name` is now defensive.

---

## 2. Performance & Scalability Bottlenecks

### 2.1 Inefficient Sorting in Window Functions
- **FIXED:** `WindowExecutor` now uses a Schwartzian Transform (Decorate-Sort-Undecorate) via the `WindowRow` wrapper. Expressions are evaluated exactly once per row.
- **FIXED:** `RANK` and `DENSE_RANK` now use $O(N)$ calculation based on sorted peer identification instead of $O(N^2)$ re-comparison.
- **FIXED:** Peer-matching in `resolve_bound_optimized` uses cached values, eliminating redundant `eval_expr` calls during frame resolution.

### 2.2 Memory Scalability (Storage Trait)
- **FIXED:** `Storage` now exposes `scan_rows`, an iterator-based streaming API, and `get_rows` is only a compatibility shim.
- **FIXED:** The redb backend no longer materializes every row before handing data to callers.
- **IMPROVED:** The main read-heavy callers (`query_planner`, mutation validation, merge, schema refresh) now consume the stream directly or only collect at the final boundary.

### 2.3 Excessive Cloning
- **PARTIAL:** Frequent `.clone()` calls still show up in hot paths such as `ExecutionContext::subquery`, join row construction, projection, binder/evaluator dispatch, and dirty-buffer snapshotting.
- **Observation:** Some cloning is intentional because the current ownership model materializes rows and AST fragments as owned values.
- **Recommendation:** Prefer borrowed AST/value access where possible, and consider `Arc` or shared row buffers for large immutable payloads that are repeatedly copied.

### 2.4 Subquery Overhead
- `eval_scalar_subquery` clones the entire `Catalog` and `Storage` to isolate execution. This is extremely expensive for non-trivial databases.

---

## 3. SOLID Principles Review

| Principle | Status | Observation |
| :--- | :--- | :--- |
| **S**ingle Responsibility | **Improved** | `ExecutionContext` is now mostly a coordinator, with scope, session, and row behavior delegated to the sub-structs that own that state. |
| **O**pen/Closed | **Improved** | Scalar-function dispatch now uses registry lookups for the system, datetime, string, and math categories, but parser and script-level AST dispatch still rely on large `match` blocks, so some central edits remain unavoidable. |
| **L**iskov Substitution | **High** | Trait-based boundaries (`Storage`, `Catalog`, `CheckpointableStorage`) work well and the concrete backends are substitutable in practice. |
| **I**nterface Segregation | **Moderate** | `Storage` is still a broad interface even after the streaming read fix; it mixes scanning, mutation, table lifecycle, cloning, and checkpointing concerns. |
| **D**ependency Inversion | **High** | The engine generally depends on traits and injected services rather than concrete storage or catalog implementations. |

---

## 4. Security & Stability Concerns

### 4.1 Unbounded Recursion
- **FIXED:** The `Parser` (state.rs) and `eval_expr` (evaluator.rs) now implement explicit recursion depth limits (`MAX_PARSER_DEPTH=8`, `MAX_RECURSION_DEPTH=32`). This prevents stack overflow panics from deeply nested expressions or statements.
- **TESTED:** Verified with `tests/recursion_limit.rs`.

### 4.2 Resource Exhaustion
- The biggest storage-layer allocation hot spots have been addressed by moving reads to `scan_rows`.
- Some code paths still collect rows intentionally when a full table snapshot is semantically required, but the engine is no longer forced to materialize every read upfront.

---

## 5. Strategic Recommendations

1. **Keep reducing central state:** `ExecutionContext` is better factored now, but continue splitting it if you want a stronger SRP boundary.
2. **Registry-based Evaluator:** The registry now covers the main scalar categories; keep extending the same pattern to the remaining evaluator dispatch points if you want to keep improving OCP.
3. **Reduce cloning pressure:** Audit the clone-heavy executor paths (`context`, `joins`, `projection`, `dirty_buffer`, `subquery`) and replace repeated full-value copies with shared or borrowed data where practical.
4. **Streaming Storage:** Done for the core read path; keep migrating any remaining `get_rows` callers to `scan_rows` when they do not need a full table snapshot.
5. **Safe Error Handling:** Keep preferring explicit error propagation in new production code; remaining `.unwrap()` usage should stay limited to tests or hard invariants.

# Anti-Patterns Report

> Generated: 2026-03-31

---

## 🔴 Critical

### 1. Control Flow via Error Channel

**Location:** `crates/tsql_core/src/error.rs` (lines 26-33)

`DbError::Break`, `DbError::Continue`, and `DbError::Return` use the `Result::Err` path for **normal** control flow (T-SQL `BREAK`, `CONTINUE`, `RETURN` statements). This forces every handler in the executor to special-case these "non-errors" and risks:

- Accidentally swallowing real errors when matching on `Err(_)`
- Leaking control signals past their intended scope
- Confusing `TRY...CATCH` logic, which must explicitly re-raise control flow signals

**Affected files:**

| File | What it does |
|---|---|
| `executor/script/control_flow.rs` (L50-65) | Catches `Break`/`Continue` inside `WHILE` loops |
| `executor/script/mod.rs` (L120-135, L255-265) | Intercepts signals during script/block execution |
| `executor/script/procedural/try_catch.rs` (L15-25) | Must bypass `TRY...CATCH` for control signals |
| `executor/script/procedural/routine.rs` (L155-165) | Handles `Return` in stored procedures/functions |
| `executor/evaluator.rs` (L80-90) | Converts `DbError::Return` into evaluation result |
| `executor/database/session.rs` (L295-635) | Multiple locations managing signal propagation |

**Recommended fix:**

Replace with a dedicated control flow enum on the success path:

```rust
enum StmtOutcome {
    Ok,
    Break,
    Continue,
    Return(Option<Value>),
}

// Statement execution returns:
Result<StmtOutcome, DbError>
```

---

### 2. Unsafe `unwrap()` in Production Paths

Multiple `unwrap()` calls in parsers and executors that will **panic** on unexpected input instead of returning a proper `DbError`.

**Parser unwraps:**

| File | Pattern | Risk |
|---|---|---|
| `parser/statements/select.rs` (L67-115) | `unwrap()` on keyword indices | Panics on malformed SELECT |
| `parser/statements/procedural/variable.rs` (L155-167) | `next_char.unwrap()` after `is_none()` check | Panics on truncated input |
| `parser/statements/dml/insert.rs` (L50-75) | `unwrap()` on keyword indices | Panics on malformed INSERT |
| `parser/statements/ddl.rs` (L781-790) | `upper.find('(').unwrap()` | Panics if DECIMAL has no parens |

**Executor unwraps:**

| File | Pattern | Risk |
|---|---|---|
| `executor/result.rs` (L22-40) | `serde_json::to_value(...).unwrap()` | Panics on non-serializable values |
| `executor/json.rs` (L190-200) | `map.get_mut(&key).unwrap()` | Panics if key missing |
| `executor/aggregates.rs` (L150-240) | `args.first().unwrap()` in AVG/MIN/MAX | Panics on empty args |

**Server unwraps:**

| File | Pattern | Risk |
|---|---|---|
| `tsql_server/src/session.rs` (L270-280) | `self.session_id.unwrap()` | Panics if session not initialized |

**Recommended fix:**

Replace each `unwrap()` with a proper error return:

```rust
// Before
let idx = tokens.iter().position(|t| t == "FROM").unwrap();

// After
let idx = tokens.iter().position(|t| t == "FROM")
    .ok_or_else(|| DbError::Parse("expected FROM keyword".into()))?;
```

---

## 🟡 Moderate

### 3. Excessive Cloning in Hot Paths

Large data structures (`Vec<Row>`, catalogs, storage snapshots) are cloned inside loops or on every iteration, causing unnecessary memory allocations.

**Recursive CTE execution:**

| File | Pattern |
|---|---|
| `executor/script/cte_proxy.rs` (L50-85) | Clones `result.rows` and `all_rows` on every recursive iteration |

**Window functions:**

| File | Pattern |
|---|---|
| `executor/window.rs` (L310-330) | Clones `win_expr` and `values` inside per-row loops |
| `executor/window.rs` (L460-465) | `iter().map(\|(_, r)\| r.clone())` on partition slices |

**Transaction/snapshot management:**

| File | Pattern |
|---|---|
| `executor/transaction.rs` (L200-240) | Clones entire `begin_catalog`, `begin_storage`, and snapshots |
| `executor/transaction_exec.rs` (L90-110) | Clones `table_versions` and workspace storage |

**DML operations:**

| File | Pattern |
|---|---|
| `executor/mutation/mod.rs` — via `script/dml.rs` (L85-105) | Clones `target_rows` and individual rows in update loops |

**Recommended fixes:**

- Use `Rc<[Row]>` or `Arc<[Row]>` for shared immutable row sets
- Use copy-on-write (`Cow<'_, [Row]>`) for rows that are rarely mutated
- Use persistent/structural-sharing data structures for transaction snapshots
- Avoid cloning expressions inside per-row loops — lift them out of the loop

---

### 4. String-Based Dispatch Instead of Typed Enums

Function names, SQL keywords, date parts, and aggregate names are dispatched via string matching (`.to_uppercase()` + `match` on `&str`). This is fragile, has no compile-time exhaustiveness checking, and incurs unnecessary allocations.

**Scalar function dispatch:**

| File | Example |
|---|---|
| `executor/scalar/mod.rs` (L41-100) | `match name.to_uppercase().as_str() { "GETDATE" => ..., "LEN" => ... }` |

**Aggregate dispatch:**

| File | Example |
|---|---|
| `executor/aggregates.rs` (L30-40) | `match name.to_uppercase().as_str() { "COUNT" => ..., "SUM" => ... }` |

**Date part dispatch:**

| File | Example |
|---|---|
| `executor/scalar/datetime.rs` (L83-153) | `match part { "year" \| "yy" => ..., "month" \| "mm" => ... }` |

**Parser keyword dispatch:**

| File | Example |
|---|---|
| `parser/tokenizer.rs` (L242-277) | String comparisons for AND, OR, CASE, etc. |
| `parser/expression/window.rs` (L28-44) | Window function name matching |
| `parser/statements/ddl.rs` (L406-450) | Constraint/column option parsing |

**System function dispatch:**

| File | Example |
|---|---|
| `executor/scalar/system.rs` (L107-113, L352-385) | COLUMNPROPERTY, HASHBYTES algorithm dispatch |

**Recommended fix:**

Parse identifiers into typed enums at the parser level:

```rust
enum ScalarFn {
    GetDate,
    IsNull,
    Len,
    Upper,
    // ...
}

enum DatePart {
    Year,
    Month,
    Day,
    // ...
}
```

This gives compile-time exhaustiveness checks and avoids repeated `.to_uppercase()` allocations at runtime.

---

### 5. God Functions (Excessive Length and Responsibility)

Several functions exceed 100-200+ lines with deep nesting and handle too many concerns in one place.

| File | Function | Lines | Issue |
|---|---|---|---|
| `executor/scalar/mod.rs` (L23-249) | `eval_function` | ~220 | Single `match` dispatching **all** T-SQL built-in functions |
| `parser/mod.rs` (L299-468) | `parse_sql_with_quoted_ident` | ~170 | Giant `if/else` for every SQL statement type |
| `parser/tokenizer.rs` (L62-231) | `tokenize_expr_with_quoted_ident` | ~170 | Nested `match` + `if/else` for lexing |
| `parser/mod.rs` (L81-191) | `split_statements` | ~110 | Complex state machine for batch splitting |
| `executor/mutation/mod.rs` (L35-132) | `execute_triggers` | ~100 | Manages pseudo-tables, trigger nesting, side effects |
| `executor/query_planner.rs` (L91-185) | `build_physical_plan` | ~90 | Table binding + join reordering + strategy selection |

**Recommended fix:**

Break into focused sub-functions by category:

```rust
// Before: one 220-line match in eval_function
// After:
fn eval_function(...) -> Result<Value, DbError> {
    match category {
        FnCategory::String => eval_string_fn(name, args, ctx),
        FnCategory::DateTime => eval_datetime_fn(name, args, ctx),
        FnCategory::Math => eval_math_fn(name, args, ctx),
        FnCategory::System => eval_system_fn(name, args, ctx),
        // ...
    }
}
```

---

## 🟢 Low Severity

### 6. `Mutex::lock().unwrap()` Panics on Poison

All `Arc<Mutex<>>` usage throughout the codebase uses `.lock().unwrap()`, which panics if another thread panicked while holding the lock (mutex poisoning).

**Affected files:**

| File | Pattern |
|---|---|
| `executor/transaction_exec.rs` (L110-220) | Repeated `lock().unwrap()` on transaction state |
| `executor/dirty_buffer.rs` (L71-98) | Lock held during dirty read view construction |
| `executor/mutation/mod.rs` (L141-204) | Multiple sequential `push_op` calls, each acquiring a lock |
| `executor/script/mod.rs` (L302-361) | Frequent lock/unlock in script execution |
| `executor/database/session.rs` (L41-116) | Many small methods each independently locking `inner` |
| `executor/database/persistence.rs` (L22) | `SharedState` wrapped in `Mutex` |

**Impact:** Low for the WASM target (single-threaded), but problematic for `tsql_server` under concurrent connections.

**Recommended fixes:**

- Switch to `parking_lot::Mutex` (no poisoning, faster on uncontended paths)
- Or use `.lock().unwrap_or_else(|e| e.into_inner())` to recover from poisoning
- Consider `RwLock` for read-heavy shared state (e.g., catalog lookups)

---

## Summary

| # | Anti-Pattern | Severity | Effort to Fix |
|---|---|---|---|
| 1 | Control flow via error channel | 🔴 Critical | High — requires `StmtOutcome` enum threaded through executor |
| 2 | Unsafe `unwrap()` in production | 🔴 Critical | Medium — mechanical replacement with `?` |
| 3 | Excessive cloning in hot paths | 🟡 Moderate | High — requires `Rc`/`Arc`/`Cow` refactoring |
| 4 | String-based dispatch | 🟡 Moderate | Medium — add enums to AST, update parser + executor |
| 5 | God functions | 🟡 Moderate | Low — extract sub-functions, no API changes |
| 6 | Mutex poisoning panics | 🟢 Low | Low — swap to `parking_lot` or add recovery |

# Codebase Analysis: Anti-Patterns, DRY Violations & SOLID Violations

## 🔴 1. God Parameter List (Anti-Pattern)

Almost every expression/evaluation function passes the same **6 parameters**: `(expr, row, ctx, catalog, storage, clock)`. This tuple appears in **dozens** of functions across `evaluator.rs`, `predicates.rs`, `scalar/`, `joins.rs`, `query/`, and all built-in handlers.

**Files:** `evaluator.rs`, `joins.rs`, `scalar/mod.rs`, `scalar/udf.rs`, `scalar/builtin_registry.rs`, all `ScalarHandler` signatures.

**Fix:** Bundle `catalog`, `storage`, and `clock` into a single `EvalEnv` struct and pass that instead, reducing 6 parameters to 3-4.

---

## 🔴 2. Inconsistent Identifier Normalization (DRY Violation)

A centralized `string_norm.rs` module exists with `normalize_identifier()` (uses `to_uppercase()`), yet:
- **Catalog** uses `.to_lowercase()` everywhere for HashMap keys (`catalog/mod.rs`, all registry files)
- **Executor** uses `.to_uppercase()` via `normalize_identifier()`
- **Parser** does its own `to_uppercase()` directly
- Random `.eq_ignore_ascii_case()` calls are scattered in `joins.rs`, `projection.rs`, `foreign_keys.rs`, `execution.rs`

**Impact:** Mixing `.to_lowercase()` and `.to_uppercase()` for the same purpose across layers is fragile and a source of subtle bugs.

---

## 🔴 3. Massive Generic Bound Repetition (DRY Violation)

The trait bounds below are **copy-pasted on 15+ functions** across `dispatch.rs`, `execution.rs`, `transaction_exec.rs`, `persistence/`, `analyzer.rs`, `session.rs`:
```rust
C: Catalog + Serialize + DeserializeOwned + Clone + 'static + Default,
S: Storage + CheckpointableStorage + Serialize + DeserializeOwned + Clone + 'static + Default,
```

**Fix:** Define a trait alias or supertrait: `trait EngineStorage: Storage + CheckpointableStorage + Serialize + DeserializeOwned + Clone + 'static + Default {}`.

---

## 🔴 4. Null-Row Construction Duplication (DRY Violation)

The pattern for creating a NULL-padded `ContextTable` row for outer joins is **duplicated 7 times** identically in `joins.rs` (4×), `from_tree.rs` (2×), and `apply.rs` (1×):
```rust
ContextTable { table: ctx.table.clone(), alias: ctx.alias.clone(), row: None, storage_index: None }
```

**Fix:** Add `ContextTable::null_row(&self) -> ContextTable` method.

---

## 🔴 5. Duplicated Match Arms in `eval_udf_body` (DRY Violation)

In `evaluator.rs:120-170`, the read-only and write branches of `eval_udf_body` contain **identical** match arms:
```rust
match executor.execute_batch(stmts, ctx) {
    Ok(StmtOutcome::Return(Some(val))) => Ok(val),
    Ok(StmtOutcome::Return(None)) => Ok(Value::Null),
    Ok(StmtOutcome::Ok(_)) => Ok(Value::Null),
    Ok(_) => Ok(Value::Null),
    Err(e) => Err(e),
}
```
This block is literally **copy-pasted twice** (lines 146-152 and 162-168).

---

## 🔴 6. `unsafe` for Borrow Splitting (Anti-Pattern)

`execution.rs` uses **raw pointer casts** (`as *mut`) and `unsafe` blocks to work around the borrow checker for session field splitting (lines 295-322, 427-454). `evaluator.rs` uses `UnsafeCell` for read-only UDFs (lines 130-145). These are unnecessary — Rust's struct field borrowing and interior mutability patterns (e.g., `RefCell`, or splitting the struct) can handle this safely.

---

## 🟡 7. Wrapper Type Boilerplate (Anti-DRY via `Deref` + `delegate_db_traits!`)

In `database/mod.rs`, **4 wrapper types** (`Database`, `PersistentDatabase`, `Engine`, `PersistentEngine`) each manually implement `Deref`/`DerefMut` and then delegate **5 traits** via a macro (`delegate_db_traits!`). This is ~200 lines of pure boilerplate.

**SOLID (ISP):** The `StatementExecutor` trait has 6 methods. Callers that only need `execute_session_batch_sql` still depend on the entire trait.

---

## 🟡 8. `ExecutionContext` as a God Object (SRP Violation)

`ExecutionContext` in `context.rs` is a **430+ line** struct that owns session refs, metadata, frame state, row context, and window context. It has **60+ accessor/delegation methods** that simply forward to sub-fields:
```rust
pub fn variables(&self) -> &Variables { self.session.variables() }
pub fn loop_depth(&self) -> usize { self.frame.loop_depth }
pub fn trigger_depth(&self) -> usize { self.frame.trigger_depth }
// ... 40+ more
```
These "backward compatibility" delegations add zero value and bloat the type.

---

## 🟡 9. `DbError` Mixed Granularity (SRP / OCP Violation)

`DbError` has **both** generic string variants (`Execution(String)`, `Semantic(String)`) and strongly-typed variants (`TableNotFound { schema, table }`). The `class()`, `number()`, and `code()` methods have **giant match arms** listing every variant. Adding a new error type requires touching 3+ methods. Much of the codebase still uses `DbError::Execution(format!(...))` instead of the typed variants.

---

## 🟡 10. Builtin Registry Linear Scan (Performance Anti-Pattern)

`builtin_registry.rs` uses `lookup_builtin_handler()` which chains **8 sequential linear scans** through `const` arrays via `.find()`. With ~90 registered functions, every unknown function name triggers scanning all 90 entries. A `HashMap` (built once with `lazy_static`/`OnceLock`) would be O(1).

---

## 🟡 11. Session Lookup Repetition (DRY Violation)

In `execution.rs`, the pattern:
```rust
let session_mutex = self.state.sessions.get(&session_id)
    .ok_or_else(|| DbError::Execution(format!("session {} not found", session_id)))?;
```
is repeated **6 times** in the same file (lines 37-40, 51-54, 65-67, 83-85, 92-96, 109-113).

---

## Summary Table

| # | Issue | Type | Severity | Files Affected |
|---|-------|------|----------|---------------|
| 1 | God parameter list (6 params everywhere) | Anti-Pattern | 🔴 High | 30+ files |
| 2 | Inconsistent identifier normalization | Anti-DRY | 🔴 High | catalog/, executor/, parser/ |
| 3 | Repeated generic bounds (C, S) | Anti-DRY | 🔴 High | 15+ functions |
| 4 | Null-row construction duplication | Anti-DRY | 🔴 Medium | joins.rs, from_tree.rs, apply.rs |
| 5 | Duplicated UDF match arms | Anti-DRY | 🔴 Medium | evaluator.rs |
| 6 | Unsafe borrow splitting | Anti-Pattern | 🔴 High | execution.rs, evaluator.rs |
| 7 | Wrapper Deref+delegate boilerplate | Anti-DRY | 🟡 Medium | database/mod.rs |
| 8 | ExecutionContext god object | Anti-SRP | 🟡 Medium | context.rs |
| 9 | DbError mixed granularity | Anti-OCP/SRP | 🟡 Medium | error.rs |
| 10 | Linear scan function registry | Anti-Pattern | 🟡 Low | builtin_registry.rs |
| 11 | Session lookup repetition | Anti-DRY | 🟡 Low | execution.rs |

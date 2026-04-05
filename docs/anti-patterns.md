# Anti-Patterns in `tsql_core`

> Review date: 2026-04-04  
> Codebase size: ~32,000 lines across 130+ Rust files

---

## Summary

| # | Severity | Category | File | Description | Status |
|---|----------|----------|------|-------------|--------|
| 1 | 🔴 High | Performance | `executor/transaction.rs` | Full catalog+storage clone on savepoints | ❌ Open |
| 2 | 🔴 High | Performance | `executor/window.rs` | Excessive cloning in hot loops | ✅ Fixed |
| 3 | 🔴 High | Bug | `executor/window.rs` | Errors silently swallowed as NULL | ✅ Fixed |
| 4 | 🔴 High | Bug | `executor/transaction.rs` | Panic via `.expect()` in library code | ✅ Fixed |
| 5 | 🟡 Medium | Design | `types/mod.rs` | String-typed date/time/UUID primitives | ✅ Fixed |
| 6 | 🟡 Medium | Design | `error.rs` | Overly broad stringly-typed errors | ✅ Fixed |
| 7 | 🟡 Medium | Bug | `executor/json.rs` | `.expect()` instead of Entry API | ✅ Fixed |
| 8 | 🟡 Medium | Performance | `catalog/table_registry.rs` | O(N) table deletion with Vec + rebuild | ✅ Fixed |
| 9 | 🟡 Medium | Design | `executor/window.rs` | Runtime type inference by data peeking | ✅ Fixed |
| 10 | 🟡 Medium | Performance | `executor/evaluator.rs` | Dynamic dispatch in recursive hot path | ✅ Fixed |
| 11 | 🟢 Low | Maintainability | `parser/parse/statements/*.rs` | `parse_multipart_name` duplicated 7× | ✅ Fixed |
| 12 | 🟢 Low | Design | `executor/database/mod.rs` | Leaky type aliases expose storage backends | ✅ Fixed |

Additionally, **7 Clippy warnings** were found (collapsible ifs, needless `Ok(?)`-wrapping, derivable impls, `map_entry`, etc.).

---

## 1. Full Catalog + Storage Clone on Savepoints

**File:** `crates/tsql_core/src/executor/transaction.rs` (lines 230–237)  
**Severity:** 🔴 High  
**Category:** Performance

### Problem

When creating a transaction savepoint, the entire `catalog`, `storage`, and `extra` state are deep-cloned:

```rust
tx.savepoints.push(Savepoint {
    name,
    catalog_snapshot: catalog.clone(),
    storage_snapshot: storage.clone(),
    extra_snapshot: extra.clone(),
    write_intent_len: tx.write_set.len(),
});
```

Since `InMemoryStorage` holds all database rows, this copies every row in every table in memory. What should be O(1) becomes O(N) in total data size.

### Recommendation

Introduce a delta-based approach (MVCC or Write-Ahead Log). The transaction manager should only track row-level changes/write intents rather than cloning the entire backend.

---

## 2. Excessive Cloning in Hot Loops

**File:** `crates/tsql_core/src/executor/window.rs` (lines 305–307)  
**Severity:** 🔴 High  
**Category:** Performance

### Problem

For every row in the result set, both the key and the value are cloned to build a temporary evaluation context:

```rust
for (key, values) in &results_map {
    window_map.insert(key.clone(), values[idx].clone());
}
```

This results in O(N × M) allocations where N = row count, M = window expressions.

### Recommendation

Have the evaluation context accept references (`&Value`) or index pointers into the original results map instead of deep-copying values for every row.

---

## 3. Errors Silently Swallowed as NULL

**File:** `crates/tsql_core/src/executor/window.rs` (line 312)  
**Severity:** 🔴 High  
**Category:** Bug

### Problem

```rust
projected_row.push(
    eval_expr(&item.expr, row, ctx, self.catalog, self.storage, self.clock)
        .unwrap_or(Value::Null)
);
```

Any execution error (divide by zero, arithmetic overflow, type mismatch) is completely hidden from the user and silently returned as `NULL`. This can cause silent data corruption.

### Recommendation

Bubble up the error using the `?` operator so the query terminates with a visible fault:

```rust
projected_row.push(
    eval_expr(&item.expr, row, ctx, self.catalog, self.storage, self.clock)?
);
```

---

## 4. Panic via `.expect()` in Library Code

**File:** `crates/tsql_core/src/executor/transaction.rs` (lines 155–158)  
**Severity:** 🔴 High  
**Category:** Bug

### Problem

```rust
let tx = self
    .active
    .take()
    .expect("active tx must exist at depth > 0");
```

If there is ever a bug in transaction depth tracking, a rogue `COMMIT` or `ROLLBACK` will crash the entire process instead of returning a structured error.

### Recommendation

Return a proper `DbError::Execution` if the active transaction is unexpectedly `None`:

```rust
let tx = self
    .active
    .take()
    .ok_or_else(|| DbError::Execution("active tx must exist at depth > 0".into()))?;
```

---

## 5. String-Typed Date/Time/UUID Primitives

**File:** `crates/tsql_core/src/types/mod.rs` (lines 94–98)  
**Severity:** 🟡 Medium  
**Category:** Design

### Problem

```rust
Date(String),
Time(String),
DateTime(String),
DateTime2(String),
UniqueIdentifier(String),
```

Representing core temporal and UUID types as plain `String` forces the executor to constantly parse and re-format strings at runtime during comparisons, arithmetic, and formatting.

### Recommendation

Replace with strongly-typed primitives:
- `Date` → `chrono::NaiveDate`
- `DateTime` / `DateTime2` → `chrono::NaiveDateTime`
- `Time` → `chrono::NaiveTime`
- `UniqueIdentifier` → `uuid::Uuid`

---

## 6. Overly Broad Stringly-Typed Errors

**File:** `crates/tsql_core/src/error.rs` (lines 12–24)  
**Severity:** 🟡 Medium  
**Category:** Design

### Problem

```rust
pub enum DbError {
    Parse(String),
    Semantic(String),
    Execution(String),
    Storage(String),
    // ...
}
```

Wrapping arbitrary error messages in broad `String` variants prevents callers from programmatically distinguishing between specific failure scenarios (e.g., "table not found" vs. "type mismatch").

### Recommendation

Refactor into strongly-typed sub-variants:

```rust
pub enum DbError {
    TableNotFound { schema: String, table: String },
    TypeMismatch { expected: DataType, found: DataType },
    ColumnNotFound { column: String },
    // ...
}
```

---

## 7. `.expect()` Instead of Entry API

**File:** `crates/tsql_core/src/executor/json.rs` (line 196)  
**Severity:** 🟡 Medium  
**Category:** Bug

### Problem

```rust
map.insert(key.clone(), JsonValue::Object(serde_json::Map::new()));
// ...
current = map.get_mut(&key).expect("JSON key was just inserted");
```

While currently safe, dual-lookups with panicking methods are fragile and non-idiomatic.

### Recommendation

Use the `Entry` API:

```rust
current = map.entry(key.clone()).or_insert_with(|| JsonValue::Object(serde_json::Map::new()));
```

---

## 8. O(N) Table Deletion with Vec + Rebuild

**File:** `crates/tsql_core/src/catalog/table_registry.rs` (lines 44–46)  
**Severity:** 🟡 Medium  
**Category:** Performance

### Problem

```rust
self.tables.remove(idx);       // O(N) shift
self.indexes.retain(|idx| idx.table_id != table_id);
self.rebuild_maps();           // O(N) full index rebuild
```

`Vec::remove` forces a linear shift of all subsequent elements, and `rebuild_maps()` regenerates all lookup indices from scratch.

### Recommendation

Use `HashMap<u32, TableDef>` or a generational arena (e.g., `slotmap` crate) for O(1) table operations.

---

## 9. Runtime Type Inference by Data Peeking

**File:** `crates/tsql_core/src/executor/window.rs` (lines 318–326)  
**Severity:** 🟡 Medium  
**Category:** Design

### Problem

```rust
let mut column_types = vec![DataType::VarChar { max_len: 4000 }; projection.len()];
for col_idx in 0..projection.len() {
    for row in &final_projected_rows {
        if !row[col_idx].is_null() {
            column_types[col_idx] = row[col_idx].data_type().unwrap_or(DataType::VarChar { max_len: 4000 });
            break;
        }
    }
}
```

Guessing output column types by scanning populated rows *after* evaluation is fragile — fails on entirely null columns or empty result sets.

### Recommendation

Implement static type binding during the Binder/Planner phase based on AST definitions, not dynamic runtime inspection.

---

## 10. Dynamic Dispatch in Recursive Hot Path

**File:** `crates/tsql_core/src/executor/evaluator.rs` (lines 27–29)  
**Severity:** 🟡 Medium  
**Category:** Performance

### Problem

```rust
pub(crate) fn eval_expr(
    expr: &Expr,
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,   // vtable dispatch
    storage: &dyn Storage,   // vtable dispatch
    clock: &dyn Clock,       // vtable dispatch
) -> Result<Value, DbError> {
```

`eval_expr` is called recursively on every AST node for every row. The dynamic vtable lookups hurt CPU branch prediction and prevent inlining.

### Recommendation

Resolve catalog/storage bindings in a Binder phase ahead of time. Make `eval_expr` generic (`<C: Catalog, S: Storage>`) so the compiler can monomorphize and inline.

---

## 11. `parse_multipart_name` Duplicated 7 Times

**File:** `crates/tsql_core/src/parser/parse/statements/` — found in `query.rs`, `other.rs`, `drop.rs`, `dml.rs`, `ddl.rs`, `create.rs`, `alter.rs`  
**Severity:** 🟢 Low  
**Category:** Maintainability

### Problem

The same `parse_multipart_name` function is copy-pasted identically across 7 parser files. If the grammar for multipart names ever changes, all 7 copies must be updated.

### Recommendation

Extract into a shared parser utility in `crates/tsql_core/src/parser/parse/mod.rs` and import where needed.

---

## 12. Leaky Type Aliases Expose Storage Backends

**File:** `crates/tsql_core/src/executor/database/mod.rs` (lines 89–93)  
**Severity:** 🟢 Low  
**Category:** Design

### Problem

```rust
pub type Database = persistence::DatabaseInner<CatalogImpl, InMemoryStorage>;
pub type PersistentDatabase = persistence::DatabaseInner<CatalogImpl, crate::storage::RedbStorage>;
```

These public type aliases expose the exact storage backend implementations, making it a breaking change to swap storage layers.

### Recommendation

Wrap behind an opaque public struct:

```rust
pub struct Database(DatabaseInner<CatalogImpl, InMemoryStorage>);
```

---

## Clippy Warnings

| Warning | File | Line |
|---------|------|------|
| `collapsible_if` | `parser/parse/statements/control_flow.rs` | 47, 64 |
| `collapsible_else_if` | `parser/parse/statements/create.rs` | 100 |
| `needless_question_mark` | `parser/parse/mod.rs` | 343 |
| `collapsible_match` | `parser/parse/mod.rs` | 400 |
| `should_implement_trait` | `parser/token/keyword.rs` | 18 |
| `derivable_impls` | `storage/redb_storage.rs` | 35 |
| `manual_next_back` | `storage/redb_storage.rs` | 101 |
| `map_entry` | `storage/mod.rs` | 94 |
| `unwrap_or_default` | `storage/mod.rs` | 112 |

All are auto-fixable with `cargo clippy --fix`.

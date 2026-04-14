# iridium_core Code Review

> Reviewed: 2026-04-05

---

## Part 1 - Anti-Patterns

### 1. `unsafe` raw pointer aliasing to bypass the borrow checker (`execution.rs`) - **Critical (UB)**

`execute_batch_core_inner` and `execute_single_statement` create multiple mutable borrows from the same raw pointer while borrows from `ExecutionContext` are still alive.

```rust
let session_ptr = session as *mut SessionRuntime<C, S>;
let mut ctx = unsafe { let session_ref = &mut *session_ptr; ... };
let exec_res = unsafe { let session_ref = &mut *session_ptr; ... };
```

This is UB-risky aliasing (`&mut` overlap through `session_ptr`) and should be removed.

**Fix:** split `SessionRuntime` into disjoint borrowed pieces, or refactor flow so the second mutable borrow only happens after `ctx` is fully dropped.

---

### 2. `UnsafeCell` + raw pointer cast to get `&mut` from `&` (`evaluator.rs`) - **High (UB risk)**

Read-only UDF path casts immutable trait object refs to mutable via raw pointers + `UnsafeCell`:

```rust
let cat_ptr = catalog as *const dyn Catalog as *mut dyn Catalog;
let mut executor = ScriptExecutor {
    catalog: unsafe { &mut **cat_cell.get() },
    ...
};
```

Even with `is_read_only_statement()` gating, this is still unsound as a general mutability model.

**Fix:** provide a read-only execution path using `&dyn Catalog`/`&dyn Storage`, or clone explicitly for isolated mutable execution.

---

### 3. Duplicated `execute_physical_plan` logic (`query/mod.rs`) - **Medium**

`execute_physical_plan` and `execute_to_joined_rows` duplicate the same scan/join/apply/pivot/unpivot/filter pipeline.

**Fix:** extract a shared internal method returning `Vec<JoinedRow>` and reuse from both call sites.

---

### 4. Repeated session lookup boilerplate - **Low/Medium**

The session lookup + lock pattern repeats many times across database execution/analyzer/persistence layers.

**Fix:** centralize in a small helper on shared state/service layer.

---

### 5. `format!("{:?}", expr)` as window-function key (`evaluator.rs`) - **Medium**

Using debug-string serialization as identity key is fragile and can drift with formatting changes.

**Fix:** assign stable IDs during planning/binding.

---

### 6. `get_insert_columns` clones unnecessarily (`mutation/insert.rs`) - **Low**

`stmt_columns.clone()` copies `Option<Vec<String>>` unnecessarily.

**Fix:** use `as_ref()`/`as_deref()` or pass ownership.

---

### 7. `collect::<Vec<_>>().is_empty()` instead of `.any()` (`mutation/insert.rs`) - **Low**

Allocates when only existence check is needed.

**Fix:** use `.any(|t| !t.is_instead_of)`.

---

### 8. `CatalogImpl` deserialization map rebuild dependency (`catalog/mod.rs`) - **Medium (mitigated in main path)**

`#[serde(skip)]` maps require `rebuild_maps()`. Main checkpoint path already calls rebuild (`RecoveryCheckpoint::from_json`), so production flow is protected. Risk remains for any future direct deserialization path that forgets rebuild.

**Fix:** enforce rebuild in `Deserialize` boundary (custom deserializer or serde conversion wrapper).

---

### 9. Magic number ID seeds without explanation (`catalog/mod.rs`) - **Low**

`next_table_id: 1234567890`, `next_index_id: 234567890`, `next_object_id: -1` are non-obvious.

**Fix:** replace with named constants + rationale comments.

---

### 10. O(tables x rows) FK scan on delete/update (`mutation/validation.rs`) - **Low (known limitation)**

FK enforcement scans all tables and rows in referenced checks.

**Fix:** document as known limitation and leave clear extension point for indexed FK lookup.

---

### 11. Deprecated constructor kept with full 18-arg body (`context.rs`) - **Low**

`ExecutionContext::new()` is deprecated and appears unused in tree but still maintained.

**Fix:** remove after compatibility window, or keep behind stricter internal-only gate.

---

### 12. Duplicate writes to trigger pseudo-table aliases (`mutation/mod.rs`) - **Low (correctness/cleanliness)**

`INSERTED` and `DELETED` are each inserted twice in `temp_map` during trigger setup.

**Fix:** remove duplicate insert calls.

---

### Anti-Pattern Summary

| # | Issue | Severity |
|---|-------|----------|
| 1 | `unsafe` double `&mut` aliasing in `execution.rs` | Critical (UB) |
| 2 | `UnsafeCell` cast to bypass immutability in `evaluator.rs` | High (UB risk) |
| 3 | Duplicated scan/filter pipeline in `query/mod.rs` | Medium |
| 4 | Repeated session lookup boilerplate | Low/Medium |
| 5 | `Debug` string as key for window functions | Medium |
| 6 | Unnecessary clone in `get_insert_columns` | Low |
| 7 | `collect().is_empty()` instead of `.any()` | Low |
| 8 | Deserialization requires `rebuild_maps()` | Medium (mitigated) |
| 9 | Magic ID seeds with no explanation | Low |
| 10 | O(tables x rows) FK scan on delete/update | Low |
| 11 | Dead deprecated constructor | Low |
| 12 | Duplicate `temp_map` inserts in trigger setup | Low |

---

## Part 2 - SOLID Compliance

### S - Single Responsibility Principle - ⚠️ Partial

`ScriptExecutor` remains broad (DDL/DML/procedural/cursor/CTE orchestration), and `execute_triggers` still combines pseudo-table lifecycle + execution orchestration.

Positive: specialized executors (`QueryExecutor`, `MutationExecutor`, `GroupExecutor`, `WindowExecutor`, `SchemaExecutor`) are good decomposition points.

---

### O - Open/Closed Principle - ✅ Good

`StatementVisitor` closed AST dispatch and `Catalog` abstraction support extension in expected ways. Large `match` blocks in SQL engines are often unavoidable due to fixed grammar.

---

### L - Liskov Substitution Principle - ⚠️ Partial (design smell)

`ScriptExecutor` implements full `StatementVisitor`, but intentionally returns errors for session/transaction statements handled at engine layer. This is documented and intentional, but the type system does not encode the restriction.

Not a hard behavioral bug by itself, but a contract-design smell.

**Fix option:** split visitor traits (core statements vs engine-only statements) to make unsupported operations unrepresentable.

---

### I - Interface Segregation Principle - ⚠️ Partial

`Catalog` is composed from focused subtraits and docs recommend narrow bounds, but many call sites still use wide `&dyn Catalog`/`&mut dyn Catalog`.

`StatementExecutor` currently includes `set_session_metadata`, which is orthogonal to statement execution.

Positive: `Storage` vs `CheckpointableStorage` split is clean.

---

### D - Dependency Inversion Principle - ✅ Good

Core execution paths depend on abstractions (`Catalog`, `Storage`, `Clock`, `Journal`, durability traits). Weak spots remain around concrete wrapper APIs (`EngineInner<CatalogImpl, ...>`) and generic coupling in durability (`DurabilitySink<C>`), but overall DIP usage is solid.

---

### SOLID Summary

| Principle | Status | Key Notes |
|-----------|--------|-----------|
| S | ⚠️ Partial | `ScriptExecutor` still broad; trigger orchestration does multiple concerns |
| O | ✅ Good | Closed AST + trait-based behavior extension works well |
| L | ⚠️ Partial | Engine-only statements accepted by trait but rejected at runtime |
| I | ⚠️ Partial | Wide catalog usage in practice; metadata API mixed into statement executor |
| D | ✅ Good | Strong abstraction usage; some concrete-type coupling remains |


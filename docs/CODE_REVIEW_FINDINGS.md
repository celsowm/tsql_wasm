# Code Review Findings: TSQL-WASM Core Engine

This document summarizes the anti-patterns, performance bottlenecks, and architectural concerns identified during the review of the `tsql_core` crate.

## 1. Anti-Patterns

### 1.1 Monolithic Files and "God Objects"
- **DONE:** `crates/tsql_core/src/parser/lower.rs` has been removed. The logic is now correctly split into `parser/lower/*.rs` (DML, DDL, Procedural, Common), following SRP.
- **IMPROVED:** `crates/tsql_core/src/executor/window.rs` has been refactored to be more efficient, though it remains a complex module due to the nature of window functions.
- **TODO:** `ExecutionContext` / `SessionStateRefs` still bundle excessive mutable state and references.

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
- **Problem:** The `Storage::get_rows` method returns a `Vec<StoredRow>`, forcing the entire table into memory.
- **Recommendation:** Transition to an iterator-based or streaming approach (`impl Iterator<Item = ...>`) to support large datasets.

### 2.3 Excessive Cloning
- Frequent `.clone()` calls on AST nodes, `JoinedRow`, and large `Value` types (Binary/VarChar) increase CPU and memory overhead.
- **Recommendation:** Use `Arc` for large data buffers and pass AST nodes by reference where possible.

### 2.4 Subquery Overhead
- `eval_scalar_subquery` clones the entire `Catalog` and `Storage` to isolate execution. This is extremely expensive for non-trivial databases.

---

## 3. SOLID Principles Review

| Principle | Status | Observation |
| :--- | :--- | :--- |
| **S**ingle Responsibility | **Partial** | Violated by monolithic files (`lower.rs`) and state-heavy "God Objects" (`ExecutionContext`). |
| **O**pen/Closed | **Low** | The evaluator (`eval_expr`) and lowerer use large `match` statements over enums, requiring core code changes to add new features. |
| **L**iskov Substitution | **High** | Strong use of traits (`Storage`, `Catalog`) allows for seamless backend swapping. |
| **I**nterface Segregation | **Moderate** | The `Storage` trait is "fat," combining read, write, and table management operations. |
| **D**ependency Inversion | **High** | Excellent use of dependency injection; the engine depends on abstractions, not concretions. |

---

## 4. Security & Stability Concerns

### 4.1 Unbounded Recursion
- **FIXED:** The `Parser` (state.rs) and `eval_expr` (evaluator.rs) now implement explicit recursion depth limits (`MAX_PARSER_DEPTH=8`, `MAX_RECURSION_DEPTH=32`). This prevents stack overflow panics from deeply nested expressions or statements.
- **TESTED:** Verified with `tests/recursion_limit.rs`.

### 4.2 Resource Exhaustion
- The lack of streaming in the `Storage` layer combined with `Vec` allocations makes the engine vulnerable to OOM (Out-of-Memory) errors when processing large tables.

---

## 5. Strategic Recommendations

1. **Refactor `lower.rs`:** Split into `dml.rs`, `ddl.rs`, and `procedural.rs`.
2. **Registry-based Evaluator:** Move from a single `match` in `eval_expr` to a registry of function/operator handlers to improve OCP.
3. **Streaming Storage:** Refactor the `Storage` trait to return iterators.
4. **Safe Error Handling:** Keep preferring explicit error propagation in new production code; remaining `.unwrap()` usage should stay limited to tests or hard invariants.

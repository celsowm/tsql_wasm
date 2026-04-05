# Code Review Findings: TSQL-WASM Core Engine

This document summarizes the anti-patterns, performance bottlenecks, and architectural concerns identified during the review of the `tsql_core` crate.

## 1. Anti-Patterns

### 1.1 Monolithic Files and "God Objects"
- **`crates/tsql_core/src/parser/lower.rs` (56KB, 1000+ lines):** Acts as a central hub for all AST transformations. This makes maintenance difficult and increases the risk of merge conflicts.
- **`crates/tsql_core/src/executor/window.rs` (30KB, 600+ lines):** Manages all window function logic in a single, complex file.
- **`ExecutionContext` / `SessionStateRefs`:** These structs bundle excessive mutable state and references, leading to complex lifetimes and high coupling across the execution layer.

### 1.2 "Stringly-Typed" Programming
- Identifiers are frequently passed as raw `String` objects. While centralized normalization exists in `string_norm.rs`, the type system does not enforce it, allowing for accidental use of unnormalized strings.

### 1.3 Panic Risks
- **`unwrap()` in Production Code:** Several instances of `.unwrap()` were found in core logic (e.g., `lower.rs` and `locks/mod.rs`), which could lead to engine-wide panics on malformed input or unexpected states.

---

## 2. Performance & Scalability Bottlenecks

### 2.1 Inefficient Sorting in Window Functions
- **Problem:** `WindowExecutor` evaluates partition/order expressions inside the `sort_by` closure, leading to $O(M \cdot N \log N)$ evaluations for $N$ rows and $M$ expressions.
- **Recommendation:** Implement a Schwartzian Transform (Decorate-Sort-Undecorate) to evaluate expressions exactly once per row.

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
- The `eval_expr` function is recursive but lacks a depth limit. Deeply nested expressions could trigger a stack overflow.
- **Recommendation:** Implement a `MAX_RECURSION_DEPTH` check.

### 4.2 Resource Exhaustion
- The lack of streaming in the `Storage` layer combined with `Vec` allocations makes the engine vulnerable to OOM (Out-of-Memory) errors when processing large tables.

---

## 5. Strategic Recommendations

1. **Refactor `lower.rs`:** Split into `dml.rs`, `ddl.rs`, and `procedural.rs`.
2. **Registry-based Evaluator:** Move from a single `match` in `eval_expr` to a registry of function/operator handlers to improve OCP.
3. **Streaming Storage:** Refactor the `Storage` trait to return iterators.
4. **Safe Error Handling:** Audit and replace all `.unwrap()` calls in `src/` with proper error propagation using `DbError`.

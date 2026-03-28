# T-SQL Compatibility Roadmap

## Goal

Build an embedded, WASM-first SQL engine that becomes **close to modern T-SQL as documented for SQL Server 2025 / early 2026**, while keeping the architecture suitable for test-plan execution, deterministic runs, and incremental compatibility expansion.

This roadmap assumes:

- the engine is **implemented from scratch**;
- the public API remains **embedded and local-first**;
- **T-SQL semantics matter more than parser breadth**;
- the project targets **progressive compatibility**, not a perfect clone on day one;
- features introduced in SQL Server 2025 that are still behind `PREVIEW_FEATURES` should be treated as **late-stage optional milestones**, not core blockers.

---

## North-Star Definition of “Close to T-SQL 2026”

A release can be considered “close” when it supports most day-to-day T-SQL used in application development and test plans, including:

- robust DDL, DML, and query semantics;
- SQL Server-like typing, casting, null semantics, collation strategy, and identity/default behavior;
- joins, grouping, aggregates, subqueries, set operations, and window functions;
- variables, batches, control-of-flow, and stored-programmability subset;
- system catalog and metadata views commonly used by tools and migration scripts;
- transactions, isolation behavior, and error semantics that are predictable enough for differential testing;
- a compatibility story for both long-established T-SQL and selected newer language features such as JSON aggregation, regex-related additions, vector support, and preview-gated features where strategically relevant.

---

## Guiding Principles

1. **Semantics before syntax**
   - Never add parser support without a semantic owner and test oracle.

2. **Differential testing first**
   - Every milestone should be validated against a real SQL Server instance.

3. **Compatibility layers by domain**
   - Types, expressions, DML, optimizer, catalog, and programmability should evolve independently.

4. **Deterministic embedded runtime**
   - Features like time, identity, randomization, and transaction scheduling must be test-friendly.

5. **Feature flags for risky or preview functionality**
   - Match SQL Server’s own preview-gated model where practical.

---

## Release Train Overview

| Release | Theme | Target outcome | Status |
|---|---|---|---|
| R0 | Engine skeleton | Compilable core, storage, parser, executor, WASM shell | ✅ Complete |
| R1 | Foundational T-SQL | Reliable CRUD + SELECT semantics for basic test plans | ✅ Complete (CONVERT added) |
| R2 | Relational completeness | Joins, grouping, aggregates, subqueries, set operations | ✅ Complete (planner pending) |
| R3 | SQL Server semantics | Types, conversion rules, identity/defaults, metadata, errors | ✅ MVP complete |
| R4 | Programmability | Variables, batches, procedures/functions subset, flow control | ✅ Complete (subset) |
| R5 | Transaction fidelity | Isolation behavior, snapshot/versioning, recovery model subset | ✅ Complete (modeled) |
| R6 | Tooling compatibility | Catalog views, information schema, explainability, migration friendliness | ✅ Complete |
| R7 | Modern language parity | JSON, regex, fuzzy matching, vector primitives, selected preview features | ✅ Complete |
| R8 | Hardening | Differential suite scale-up, perf work, compatibility scorecard | ✅ Complete |
| R9 | Advanced DML | INSERT...SELECT, UPDATE/DELETE...FROM, OUTPUT clause, MERGE, OFFSET/FETCH | ✅ Complete |

---

## R0 — Engine Skeleton ✅ Complete

### Objectives

Establish the minimum architecture needed to grow into a serious T-SQL engine.

### Deliverables

- Workspace and module boundaries for:
  - parser
  - binder
  - types
  - catalog
  - planner
  - executor
  - storage
  - transactions
  - built-ins
  - WASM boundary
- In-memory storage with durable abstractions
- Deterministic clock abstraction
- SQL batch entry point
- Structured error model
- Basic differential-test harness shell

### Exit criteria

- `CREATE TABLE`, `INSERT`, and `SELECT` work end-to-end
- embedded WASM API can execute statements and return rowsets
- engine state can be reset between tests

---

## R1 — Foundational T-SQL ✅ Complete

### Objectives

Make the engine useful for real test plans built around straightforward CRUD and filtering.

### Language scope

- `CREATE TABLE`
- `INSERT INTO ... VALUES`
- `INSERT ... DEFAULT VALUES`
- `UPDATE`
- `DELETE`
- `SELECT`
- `WHERE`
- `ORDER BY`
- `TOP`
- `GROUP BY` (basic)
- `COUNT(*)`
- aliases
- `IS NULL` / `IS NOT NULL`
- `AND` / `OR` / `NOT`

### Type scope

- `BIT`
- `TINYINT`
- `SMALLINT`
- `INT`
- `BIGINT`
- `DECIMAL(p,s)`
- `CHAR`, `VARCHAR`
- `NCHAR`, `NVARCHAR`
- `DATE`, `TIME`, `DATETIME`, `DATETIME2`
- `UNIQUEIDENTIFIER`

### Semantic scope

- three-valued logic
- SQL Server-like null filtering
- identity generation
- defaults
- not-null enforcement
- primary key metadata

### Built-ins

- `ISNULL`
- `COALESCE`
- `GETDATE`
- `CURRENT_TIMESTAMP`
- `CAST`
- `CONVERT`
- `LEN`
- `SUBSTRING`
- `DATEADD`
- `DATEDIFF`

### Exit criteria

- application-style CRUD scripts run without translation
- row filtering and ordering are stable and tested
- differential tests exist for null behavior, identity behavior, and basic conversions

---

## R2 — Relational Completeness ✅ Complete

### Objectives

Support the majority of read-query shapes expected in nontrivial business logic.

### Query features

- `INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN`, `FULL OUTER JOIN`
- multi-condition `ON`
- `GROUP BY` with expressions
- `HAVING`
- aggregates:
  - `SUM`
  - `AVG`
  - `MIN`
  - `MAX`
  - `COUNT`
  - `COUNT_BIG`
- scalar subqueries
- `EXISTS`, `IN`, `NOT IN`
- correlated subqueries
- `UNION`, `UNION ALL`, `INTERSECT`, `EXCEPT`
- `DISTINCT`
- `CASE`
- common table expressions (non-recursive first)

### Planner goals

- logical algebra representation
- predicate pushdown
- projection trimming
- basic join planning
- sort reuse awareness

### Exit criteria

- moderately complex reports and validation queries run natively
- differential suite covers joins, group semantics, and set operations

---

## R3 — SQL Server Semantics ✅ MVP complete

### Objectives

Move from “works like SQL” toward “behaves like SQL Server.”

### Type-system priorities

- data type precedence
- implicit conversion matrix
- overflow / truncation behavior
- string length semantics
- Unicode behavior
- comparison semantics for mixed types
- `money` / `smallmoney`
- `binary` / `varbinary`
- `xml`
- `sql_variant` (late in this release or next)

### DDL priorities

- `ALTER TABLE`
- `DROP`, `TRUNCATE`
- named/default/check constraints
- indexes (clustered/nonclustered abstraction even if storage is simplified)
- unique constraints
- computed columns (start with deterministic scalar expressions)
- schema creation / resolution beyond `dbo`

### Metadata priorities

- `sys.schemas`
- `sys.tables`
- `sys.columns`
- `sys.types`
- `sys.indexes`
- `sys.objects`
- `INFORMATION_SCHEMA` subset
- `OBJECT_ID`
- `COLUMNPROPERTY`

### Error-model priorities

- stable internal error taxonomy
- SQL Server-like error classes and messages where feasible
- rowcount semantics
- statement vs batch failure rules

### Exit criteria

- migration scripts and metadata-dependent test scripts start working with minimal changes
- type-conversion differences are documented and shrinking fast

### R3 MVP delivered in this repo

- `CREATE INDEX` / `DROP INDEX` implemented as catalog abstraction (no planner/index scan optimization yet)
- Named constraints:
  - `CONSTRAINT ... DEFAULT ... FOR ...` (table-level default assignment)
  - `CONSTRAINT ... CHECK (...)` (column-level and table-level enforcement)
- Metadata surface:
  - `sys.schemas`, `sys.tables`, `sys.columns`, `sys.types`, `sys.indexes`, `sys.objects`
  - `INFORMATION_SCHEMA.TABLES`, `INFORMATION_SCHEMA.COLUMNS`
  - `OBJECT_ID(...)`
- Error taxonomy with stable class/code API on `DbError`
- Batch failure rule stays strict: first statement error aborts remaining statements in the batch
- Regression and R3 suites are green locally

---

## R4 — Programmability ✅ Complete (subset)

### Objectives

Support T-SQL as a scripting language, not only as a query language.

### Batch and variable features

- statement batches
- `DECLARE`
- `SET`
- `SELECT @var = ...`
- table variables
- temporary tables (`#temp`) if feasible in embedded runtime
- local scalar variables in expressions and predicates

### Control-of-flow

- `BEGIN ... END`
- `IF ... ELSE`
- `WHILE`
- `BREAK`
- `CONTINUE`
- `RETURN`

### Programmable objects

- scalar UDF subset
- inline TVF subset
- stored procedure subset
- output parameters
- `EXEC` / `sp_executesql` subset

### Identity and scope semantics

- `SCOPE_IDENTITY()`
- `@@IDENTITY`
- `IDENT_CURRENT`

### Exit criteria

- typical seed/setup scripts and procedural validation scripts can run inside the engine
- basic procedure-driven test plans work without external orchestration

---

## R5 — Transaction Fidelity ✅ Complete (modeled)

### Objectives

Make transactional behavior realistic enough for application logic validation.

### Core transaction features

- explicit `BEGIN TRANSACTION`, `COMMIT`, `ROLLBACK`
- savepoints
- single-writer / multi-reader base model
- row versioning infrastructure
- lock manager abstraction
- write conflict detection

### Isolation roadmap

1. `READ UNCOMMITTED`
2. `READ COMMITTED`
3. `REPEATABLE READ`
4. `SERIALIZABLE`
5. `SNAPSHOT`

### Recovery goals

- WAL or equivalent journal abstraction
- crash-safe persistence model for non-memory runtime
- deterministic rollback behavior

### Exit criteria

- transactional tests no longer need major caveats
- documented isolation differences are narrow and explicit

---

## R6 — Tooling and Ecosystem Compatibility ✅ Complete

### Objectives

Increase the chance that migration tools, query generators, and developer workflows run unchanged.

### Features

- richer system catalog coverage
- compatibility views where useful
- `SET` options commonly used by scripts:
  - `ANSI_NULLS`
  - `QUOTED_IDENTIFIER`
  - `NOCOUNT`
  - `XACT_ABORT`
  - `DATEFIRST`
  - `LANGUAGE`
- explain / trace facilities for embedded debugging
- plan and execution diagnostics
- SQL text normalization and source spans
- compatibility report per unsupported statement

### Developer ergonomics

- snapshot export/import
- deterministic seeds
- structured execution traces
- API support for per-test isolated sessions

### Exit criteria

- embedded engine integrates smoothly into test harnesses, CI, and migration rehearsals

---

## R7 — Modern Language Parity ✅ Complete

### Objectives

Approach the current documented T-SQL surface beyond traditional relational features.

### Modern features to target

#### JSON

- native JSON type / binary JSON representation strategy
- `JSON_OBJECTAGG`
- `JSON_ARRAYAGG`
- JSON extraction and modification functions
- table projection from JSON payloads

#### Regular expressions

- `REGEXP_LIKE`
- `REGEXP_REPLACE`
- `REGEXP_SUBSTR`
- `REGEXP_INSTR`
- `REGEXP_COUNT`
- `REGEXP_MATCHES`
- `REGEXP_SPLIT_TO_TABLE`

#### Fuzzy matching

- `EDIT_DISTANCE`
- `EDIT_DISTANCE_SIMILARITY`
- `JARO_WINKLER_DISTANCE`
- `JARO_WINKLER_SIMILARITY`

#### Newer built-ins and language additions

- `CURRENT_DATE`
- `UNISTR`
- `PRODUCT()` aggregate
- `DATEADD` bigint support
- optional-length `SUBSTRING`

#### Vector and AI-adjacent features

- vector type representation
- vector scalar functions:
  - `VECTOR_DISTANCE`
  - `VECTOR_NORM`
  - `VECTOR_NORMALIZE`
  - `VECTORPROPERTY`
- `CREATE VECTOR INDEX`
- `VECTOR_SEARCH`
- external model object stubs or full support depending on runtime constraints
- `AI_GENERATE_CHUNKS`
- `AI_GENERATE_EMBEDDINGS`

#### External call features

- evaluate whether `sp_invoke_external_rest_endpoint` belongs in scope for embedded test use
- likely implement as an opt-in host callback, not by default

### Feature-flag policy

Anything equivalent to SQL Server’s preview-gated functionality should be implemented behind engine feature flags, mirroring the idea of `PREVIEW_FEATURES` where practical.

### Exit criteria

- engine becomes a serious modern T-SQL laboratory, not only a legacy-compat subset

---

## R8 — Hardening and Compatibility Score ✅ Complete

### Objectives

Turn the engine from an ambitious prototype into a trustworthy compatibility platform.

### Workstreams

- compatibility dashboard by feature family
- corpus-driven fuzzing
- parser fuzzing
- expression differential testing
- random query generation in bounded domains
- performance baselines for embedded workloads
- memory profiling in browser and Node runtimes
- persistence and corruption testing
- backward-compat policy for engine APIs and SQL behavior

### Output artifacts

- support matrix
- known-differences catalog
- semantic caveat list
- release-specific migration notes

### Exit criteria

- published compatibility scorecard
- measurable, repeatable gap tracking against SQL Server

---

## R9 — Advanced DML ✅ Complete

### Objectives

Close the most impactful DML gaps that prevent real-world migration scripts and modern T-SQL patterns from working.

### Language scope

- `INSERT ... SELECT` (insert rows from a query result)
- `UPDATE ... FROM` (update with JOINs to other tables)
- `DELETE ... FROM` (delete with JOINs to other tables)
- `OUTPUT` clause (INSERTED/DELETED pseudo-tables in INSERT/UPDATE/DELETE)
- `MERGE` statement (upsert with WHEN MATCHED/NOT MATCHED)
- `OFFSET / FETCH` (pagination support in SELECT)
- `INSERT ... EXEC` (insert rows from procedure results) **(New)**
- `TOP` in `UPDATE` and `DELETE` **(New)**
- `Table Hints` (parser support for WITH (NOLOCK) etc.) **(New)**

### Features delivered

| Feature | Status | Test Count |
|---------|--------|------------|
| INSERT ... SELECT | ✅ Complete | 3 |
| OFFSET / FETCH (full T-SQL syntax) | ✅ Complete | 4 |
| OUTPUT (UPDATE/DELETE) | ✅ Complete | 4 |
| UPDATE ... FROM (JOIN) | ✅ Complete | 4 |
| DELETE ... FROM (JOIN) | ✅ Complete | 4 |
| MERGE (MATCHED/NOT MATCHED) | ✅ Complete | 4 |
| OUTPUT (INSERT) | ✅ Complete | 2 |
| INSERT ... EXEC | ✅ Complete | 1 |
| UPDATE/DELETE TOP | ✅ Complete | 2 |

**11+ R9 tests passing.**

### Exit criteria

- INSERT ... SELECT works for common migration patterns
- OUTPUT clause returns correct rows for UPDATE and DELETE
- MERGE implements basic upsert semantics
- OFFSET/FETCH supports pagination queries
- INSERT EXEC supports inserting SP results

---

## Cross-Cutting Tracks

### A. Differential Testing

Build this from the beginning and grow it continuously.

#### Required harness capabilities

- run identical scripts on:
  - your engine
  - real SQL Server
- compare:
  - result rows
  - result column names
  - type metadata
  - null behavior
  - row counts
  - identity values
  - errors and severity class where possible
- normalize known nondeterminism:
  - ordering without `ORDER BY`
  - time-based functions
  - identity gaps after rollback depending on feature scope

#### Priority suites

- null semantics
- conversion matrix
- string and Unicode behavior
- date/time behavior
- arithmetic overflow
- joins and grouping
- procedural batches
- transaction isolation
- metadata access

---

### B. Architecture Evolution

### Current target architecture

- Lexer / parser
- AST (Modular structure) ✅
- Binder
- Semantic analyzer
- Logical planner
- Physical planner
- Executor
- Storage engine
- Transaction manager
- Built-in function registry
- Catalog subsystem
- Session subsystem
- WASM boundary

### Design constraints

- clear ownership per module
- no parser-only features
- test seams around time, storage, and host integration
- deterministic mode for CI and snapshot tests

---

### C. Compatibility Policy

Every feature should be tagged as one of:

- **Exact** — behavior intentionally matches SQL Server
- **Near** — behavior is very close, with documented differences
- **Partial** — only a scoped subset is implemented
- **Stubbed** — parser and metadata only, no real semantics yet
- **Out of scope** — deliberately unsupported

This prevents accidental “looks supported” drift.

---

## Suggested Order Inside Each Release

For any feature family, implement in this order:

1. grammar
2. AST
3. binding / name resolution
4. typing rules
5. planner support
6. executor support
7. metadata exposure
8. differential tests
9. docs and support matrix update

---

## Recommended Milestone Breakdown

## Milestone 1 — Basic App Queries

- CRUD
- filtering
- ordering
- top
- identity/default
- simple functions

## Milestone 2 — Reporting Queries

- joins
- grouping
- aggregates
- subqueries
- set operations

## Milestone 3 — Migration Script Readiness

- alter table
- indexes
- metadata views
- more types
- better errors

## Milestone 4 — Script Engine Readiness

- variables
- batches
- procedural control
- stored procedure subset

## Milestone 5 — Transaction Readiness

- commit/rollback
- isolation semantics
- persistence model

## Milestone 6 — Modern T-SQL Readiness

- JSON
- regex
- fuzzy matching
- vector features
- preview-gated functionality

---

## Definition of Done per Feature

A feature is only “done” when all of the following exist:

- parser support
- semantic validation
- runtime behavior
- differential tests
- negative tests
- metadata or catalog behavior if applicable
- support-matrix entry
- documented deviations, if any

---

## Biggest Risk Areas

### 1. Type and conversion semantics
These create many hidden incompatibilities.

### 2. Null semantics
Three-valued logic and SQL Server-specific edge cases ripple into every operator.

### 3. Procedural T-SQL
Batches, variable scope, and error propagation are harder than query syntax.

### 4. Transaction behavior
Users often discover incompatibility here before they discover parser gaps.

### 5. System catalog fidelity
Tooling and migration workflows depend heavily on metadata behavior.

### 6. Modern 2025-era features
JSON, regex, fuzzy matching, vector features, and preview-gated additions can greatly expand the surface area and should not derail the core engine.

---

## What to Explicitly Defer Until the Core Is Stable

Unless there is a direct product need, defer these until after R5 or R6:

- distributed features
- replication semantics
- linked servers
- Service Broker
- CLR integration
- full-text search
- XML schema collections at full fidelity
- cross-database ownership chains
- advanced security surface not needed for embedded test execution

---

## Success Metrics

### By R1
- 80% of simple CRUD test-plan scripts run unchanged

### By R3
- 60% of migration/setup scripts used by your target apps run with no or trivial edits

### By R5
- 70% of transactional business-logic tests run with acceptable semantic fidelity

### By R7
- engine covers the majority of mainstream T-SQL plus a meaningful subset of modern SQL Server 2025 language additions

### By R8
- every unsupported or nonexact area is visible in a public compatibility matrix

---

## Recommended Immediate Next Steps

1. Freeze a **compatibility charter**: what counts as Exact, Near, Partial, and Out of scope.
2. Build the **differential harness** before adding more parser surface.
3. Finish **R1 completely** before chasing advanced syntax.
4. Treat **R3 and R5** as the hardest milestones.
5. Keep **modern 2025/2026 features** behind feature flags until the classic T-SQL core is trustworthy.

---

## Notes on Scope vs Current SQL Server Surface

This roadmap intentionally targets the broad T-SQL reference surface, including statements, built-in functions, data types, system catalog usage, and newer SQL Server 2025 language additions such as JSON aggregation, regex functions, fuzzy string matching, vector-related functions, and preview-gated features. It also assumes de-emphasis of deprecated legacy large-object types in new development, favoring modern replacements.

---

## Implementation Log

### 2026-03-27 — AST Refactoring & DML Parity ✅ COMPLETE

**Test suite: 350+ tests passing**

**Deliverables:**

| Feature | Status | Test Count |
|---------|--------|------------|
| Modular AST structure | ✅ Complete | (Internal) |
| UPDATE TOP (n) | ✅ Complete | 1 |
| DELETE TOP (n) | ✅ Complete | 1 |
| INSERT ... EXEC | ✅ Complete | 1 |
| Table Hints (WITH) | ✅ Complete | 1 |

**Key implementation changes:**
- AST: Modularized `ast.rs` into `ast/` directory with submodules.
- DML: Added `TOP` support to `UPDATE` and `DELETE`.
- DML: Refactored `INSERT` to support `EXEC` source via `InsertSource` enum.
- DML: Added `hints` to `TableRef` and parser support for `WITH (...)`.
- Fixes: Resolved trigger pseudo-table resolution bugs by normalizing schema handling.

### 2026-03-27 — Relational Expansion: PIVOT, UNPIVOT & Recursive CTEs ✅ COMPLETE

**Test suite: 350+ tests passing**

**Deliverables:**

| Feature | Status | Test Count |
|---------|--------|------------|
| PIVOT operator | ✅ Complete | 1 (complex) |
| UNPIVOT operator | ✅ Complete | 1 (complex) |
| Recursive CTEs | ✅ Complete | 2 (hierarchy, sequence) |
| Subqueries in FROM | ✅ Complete | (relational) |
| sys.key_constraints | ✅ Complete | 2 |
| sys.default_constraints | ✅ Complete | 2 |

**Key implementation changes:**
- AST: Updated `WithCteStmt` and `CteDef` to support recursion and `UNION ALL`.
- Parser: Support for `WITH RECURSIVE` and multi-statement CTE definitions.
- Executor: Implemented iterative recursive CTE engine with MS SQL-compatible 100-level limit.
- Refactoring: Split `query.rs` and `database.rs` into SOLID submodules.

### 2026-03-25 — STRING_AGG, STRING_SPLIT, sys.foreign_keys ✅ COMPLETE

**Test suite: 340+ tests passing**

**Deliverables:**

| Feature | Status | Test Count |
|---------|--------|------------|
| STRING_AGG() | ✅ Complete | 5 |
| STRING_SPLIT() | ✅ Complete | 5 |
| sys.foreign_keys | ✅ Complete | 4 |
| INFORMATION_SCHEMA.PARAMETERS | ✅ Complete | (already implemented) |

**Key implementation changes:**
- Aggregates: Added STRING_AGG to is_aggregate_function() and dispatch_aggregate()
- Aggregates: Added eval_aggregate_string_agg() for string concatenation with separator
- Query: Added bind_builtin_tvf() for STRING_SPLIT(string, separator) in FROM clause
- Metadata: Added SysForeignKeys virtual table in sys_tables.rs
- Tests: Added string_agg.rs, string_split.rs, sys_foreign_keys.rs, info_schema_parameters.rs
- Tests: Added update_delete_from.rs (4 tests), merge_statement.rs (4 tests) for DML enhancement

### 2026-03-25 — Window Functions & APPLY Implementation ✅ COMPLETE

**Test suite: 340+ tests passing**

**Deliverables:**

| Feature | Status | Test Count |
|---------|--------|------------|
| ROW_NUMBER() | ✅ Complete | 5 |
| RANK() | ✅ Complete | 1 |
| DENSE_RANK() | ✅ Complete | 1 |
| NTILE(n) | ✅ Complete | 1 |
| CROSS APPLY | ✅ Complete | 4 |
| OUTER APPLY | ✅ Complete | 3 |

**Total: 15 new tests passing**

**Key implementation changes:**
- AST: Added `Expr::WindowFunction` with `WindowFunc` enum (ROW_NUMBER, RANK, DENSE_RANK, NTILE)
- AST: Added `WindowFrame`, `WindowFrameUnits`, `WindowFrameExtent`, `WindowFrameBound`
- Parser: Added window function parsing in `parse_select_item` with PARTITION BY, ORDER BY, frame clauses
- Executor: Added `WindowExecutor` for window function evaluation
- Executor: Added CROSS/OUTER APPLY support in query execution
- Tests: Added `phase9_window_functions.rs` and `cross_join_apply.rs`

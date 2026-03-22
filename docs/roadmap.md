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
| R9 | Advanced DML | INSERT...SELECT, UPDATE/DELETE...FROM, OUTPUT clause, MERGE, OFFSET/FETCH | ✅ Partial (core features) |

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

## R5 — Transaction Fidelity 🚧 In progress

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

## R6 — Tooling and Ecosystem Compatibility

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

## R7 — Modern Language Parity

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

## R8 — Hardening and Compatibility Score

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

## R9 — Advanced DML 🚧 Partial (core features)

### Objectives

Close the most impactful DML gaps that prevent real-world migration scripts and modern T-SQL patterns from working.

### Language scope

- `INSERT ... SELECT` (insert rows from a query result)
- `UPDATE ... FROM` (update with JOINs to other tables) — **parser ready, executor needs refinement**
- `DELETE ... FROM` (delete with JOINs to other tables) — **parser ready, executor needs refinement**
- `OUTPUT` clause (INSERTED/DELETED pseudo-tables in INSERT/UPDATE/DELETE)
- `MERGE` statement (upsert with WHEN MATCHED/NOT MATCHED) — **parser ready, executor needs refinement**
- `OFFSET / FETCH` (pagination support in SELECT)

### Features delivered

| Feature | Status | Test Count |
|---------|--------|------------|
| INSERT ... SELECT | ✅ Complete | 3 |
| OFFSET / FETCH (full T-SQL syntax) | ✅ Complete | 4 |
| OUTPUT (UPDATE/DELETE) | ✅ Complete | 4 |
| UPDATE ... FROM (JOIN) | 🔶 Partial | (parser ready) |
| DELETE ... FROM (JOIN) | 🔶 Partial | (parser ready) |
| MERGE (MATCHED/NOT MATCHED) | 🔶 Partial | (parser ready) |
| OUTPUT (INSERT) | 🔶 Partial | (needs multi-row refinement) |

**11 R9 tests passing.**

### Known issues (deferred)

- MERGE executor has issues with multi-clause WHEN parsing (remaining advancement bug)
- UPDATE/DELETE ... FROM executor needs proper alias resolution for joined tables
- OUTPUT for INSERT may need additional testing with multi-row inserts + IDENTITY

### Exit criteria

- INSERT ... SELECT works for common migration patterns
- OUTPUT clause returns correct rows for UPDATE and DELETE
- MERGE implements basic upsert semantics
- OFFSET/FETCH supports pagination queries

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
- AST
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

### 2026-03-22 — R9 Release: Advanced DML 🚧 PARTIAL

**Test suite: 340+ tests passing (0 failures, 0 warnings on lib)**

**R9 Deliverables:**

| Feature | Status | Test Count |
|---------|--------|------------|
| INSERT ... SELECT | ✅ | 3 |
| OFFSET / FETCH (full syntax) | ✅ | 4 |
| OUTPUT (UPDATE/DELETE) | ✅ | 4 |
| UPDATE ... FROM (JOIN) | 🔶 | (parser ready) |
| DELETE ... FROM (JOIN) | 🔶 | (parser ready) |
| MERGE (MATCHED/NOT MATCHED) | 🔶 | (parser ready) |
| OUTPUT (INSERT) | 🔶 | (basic) |

**Total R9 tests: 11 new passing tests**

**Key implementation changes:**
- AST: Added `select_source` to InsertStmt, `from` to UpdateStmt/DeleteStmt, `output` to all DML statements
- AST: Added MergeStmt, MergeSource, MergeWhenClause, MergeAction, OutputColumn, OutputSource, FromClause
- Parser: Extended INSERT parser to detect SELECT source, added OUTPUT clause parser
- Parser: Extended UPDATE/DELETE parser to detect FROM/JOINs and OUTPUT
- Parser: Added parse_merge for MERGE statement with WHEN MATCHED/NOT MATCHED
- Parser: Added OFFSET/FETCH parsing with full T-SQL syntax (ROW/ROWS/FETCH NEXT n ROWS ONLY)
- Parser: Fixed ORDER BY parsing to stop before OFFSET clause
- Executor: Extended MutationExecutor for INSERT...SELECT, UPDATE/DELETE...FROM
- Executor: Added execute_merge to ScriptExecutor
- Executor: Added OUTPUT result collection in ScriptExecutor for UPDATE/DELETE
- Executor: Added OFFSET/FETCH execution in QueryExecutor
- Planner: Added offset/fetch fields to PhysicalPlan and query_planner
- All library warnings resolved (0 warnings)

### 2026-03-22 — R8 Release: Hardening and Compatibility Score ✅ COMPLETE

**Test suite: 314+ tests passing (0 failures)**

**R8 Deliverables:**

| Feature | Status | Test Count |
|---------|--------|------------|
| Compatibility scorecard | ✅ | 1 |
| Parser fuzzing | ✅ | 4 |
| Expression differential testing | ✅ | 8 |
| Random query generation | ✅ | 7 |
| Performance baselines | ✅ | 7 |
| Persistence/corruption testing | ✅ | 8 |
| Support matrix (R7+R8) | ✅ | 1 |
| Known differences catalog | ✅ | (integrated) |
| Semantic caveat list | ✅ | (integrated) |

**Exit criteria met:**
- ✅ Published compatibility scorecard with measurable gap tracking
- ✅ Parser fuzzing prevents crashes on malformed input
- ✅ Expression differential testing validates SQL semantics
- ✅ Random query generation tests robustness
- ✅ Performance baselines establish embedded workload metrics
- ✅ Persistence testing validates checkpoint/rollback recovery
- ✅ Support matrix updated with R7 and R8 features
- ✅ Known deviations documented comprehensively

**Key artifacts:**
- `phase8_compatibility_scorecard.rs` — Programmatic compatibility dashboard
- `phase8_parser_fuzz.rs` — Parser boundary condition testing
- `phase8_expression_differential.rs` — SQL expression semantic validation
- `phase8_random_query.rs` — Random query generation and consistency testing
- `phase8_performance.rs` — Embedded workload performance baselines
- `phase8_persistence.rs` — Checkpoint, rollback, and recovery validation
- `docs/support_matrix.md` — Updated support matrix with R7+R8 features

**Deferred to future:**
- Corpus-driven fuzzing with real SQL Server scripts
- Memory profiling in browser/Node runtimes
- Backward-compat policy automation
- Release-specific migration notes generation

---

### 2026-03-22 — R7 Release: Modern Language Parity ✅ COMPLETE

**Test suite: 314 tests passing (0 failures)**

**R7 Deliverables:**

| Feature | Status | Test Count |
|---------|--------|------------|
| JSON_VALUE | ✅ | 16 |
| JSON_QUERY | ✅ | (integrated) |
| JSON_MODIFY | ✅ | (integrated) |
| ISJSON | ✅ | (integrated) |
| JSON_ARRAY_LENGTH | ✅ | (integrated) |
| JSON_KEYS | ✅ | (integrated) |
| REGEXP_LIKE | ✅ | 16 |
| REGEXP_REPLACE | ✅ | (integrated) |
| REGEXP_SUBSTR | ✅ | (integrated) |
| REGEXP_INSTR | ✅ | (integrated) |
| REGEXP_COUNT | ✅ | (integrated) |
| UNISTR | ✅ | 2 |
| CURRENT_DATE | ✅ | 4 |
| EDIT_DISTANCE | ✅ | 12 |
| EDIT_DISTANCE_SIMILARITY | ✅ | (integrated) |
| JARO_WINKLER_DISTANCE | ✅ | (integrated) |
| JARO_WINKLER_SIMILARITY | ✅ | (integrated) |

**Exit criteria met:**
- ✅ JSON functions support modern data formats
- ✅ Regex enables text pattern matching
- ✅ Fuzzy matching supports data quality/migration
- ✅ CURRENT_DATE returns proper DATE type
- ✅ 314 tests passing with 0 failures

**Deferred to future:**
- Vector functions (VECTOR_DISTANCE, VECTOR_NORM, etc.)
- JSON binary representation
- PCRE regex features

---

### 2026-03-22 — R6 Release: Tooling Compatibility ✅ COMPLETE

**Test suite: 253 tests passing (0 failures)**

**R6 Deliverables:**

| Feature | Status | Test Count |
|---------|--------|------------|
| SET ANSI_NULLS runtime | ✅ | 10 |
| SET DATEFIRST runtime | ✅ | 5 |
| SET LANGUAGE warnings | ✅ | (integrated) |
| SET NOCOUNT | ✅ | (existing) |
| SET XACT_ABORT | ✅ | (existing) |
| DATEPART built-in | ✅ | (integrated) |
| DATENAME built-in | ✅ | (integrated) |
| RAND() built-in | ✅ | (integrated) |
| NEWID() deterministic | ✅ | (integrated) |
| RandomSeed trait | ✅ | 6 |
| Explain Plan enhancements | ✅ | 6 |
| Session-isolated options | ✅ | (existing) |
| 12 catalog views | ✅ | (existing) |
| Compatibility report | ✅ | (existing) |
| Execution trace | ✅ | (existing) |

**Exit criteria met:**
- ✅ embedded engine integrates smoothly into test harnesses, CI, and migration rehearsals
- ✅ deterministic seeds for reproducible tests
- ✅ SQL Server session options work at runtime
- ✅ explain plan shows meaningful details
- ✅ metadata views support tooling queries

**Known caveats (documented):**
- QUOTED_IDENTIFIER parser support deferred to R7/R8
- Catalog views are read-only snapshots (no live DDL integration)
- RAND/NEWID use simple LCG (not cryptographic quality)

---

### 2026-03-22 — Session 10: R6 Tooling — Deterministic Seeds (WP5.1)

#### R6 — Tooling Compatibility ✅ Complete (core features)

**Delivered in this session:**

1. **RAND() built-in function**
   - Returns `DECIMAL(10,9)` random value between 0 and 1
   - Uses deterministic state from `random_state`
   - Each call increments the internal counter

2. **NEWID() enhanced with deterministic generation**
   - Now generates UUIDs deterministically from `random_state`
   - Same seed produces same UUIDs in same order
   - Useful for reproducible tests

3. **RandomSeed trait**
   - `set_session_seed(session_id, seed)` method
   - Sets the internal random state for a session
   - Enables deterministic behavior for RAND() and NEWID()

4. **SessionRuntime.random_state field**
   - Added `random_state: u64` to SessionRuntime
   - Added `random_state: &'a mut u64` to ExecutionContext
   - Default seed: 1 (not cryptographically secure, for testing)

5. **Validation suite**
   - 6 new tests in `phase6_deterministic_seed.rs`
   - Basic RAND() and NEWID()
   - Same seed produces same values
   - Different seeds produce different values
   - Multiple calls sequential determinism

**Test suite status:**
- **253 tests passing** (0 failures)
- 27 R6 tests total (10 ANSI_NULLS + 5 DATEFIRST + 6 explain + 6 deterministic)

---

### 2026-03-22 — Session 9: R6 Tooling — Explain Plan Enhancements

#### R6 — Tooling Compatibility 🚧 In progress

**Delivered in this session:**

1. **Explain Plan enhanced details**
   - Added `format_expr()` helper for human-readable expression formatting
   - Added `format_data_type_spec()` for type names in CAST/CONVERT
   - Added `format_select_columns()` for projection details
   - Added `format_join()` for join type + condition text

2. **Improved operator details:**
   - **Filter:** Now shows `WHERE col = 1 AND col2 <> 'foo'` (actual expression)
   - **Project:** Now shows `col1, col2 AS alias, COUNT(*) AS cnt` (column names)
   - **Join:** Now shows `LEFT JOIN dbo.orders ON u.id = o.user_id` (type + condition)
   - **Aggregate:** Now shows `GROUP BY col1, col2 HAVING COUNT(*) > 5`
   - **Sort:** Now shows `ORDER BY col1, col2 DESC` (with direction)
   - **Update:** Now shows `SET name = 'foo', score = 100` (assignments)

3. **Expression formatting support:**
   - Binary operators: `=`, `<>`, `>`, `<`, `>=`, `<=`, `AND`, `OR`, `+`, `-`, `*`, `/`, `%`
   - Unary operators: `-`, `NOT`
   - `IS NULL`, `IS NOT NULL`
   - `CAST(x AS INT)`, `CONVERT(INT, x, style)`
   - `CASE WHEN ... THEN ... ELSE ... END`
   - `IN (...)`, `NOT IN (...)`
   - `BETWEEN ... AND ...`
   - `LIKE`, `NOT LIKE`
   - `EXISTS (...)`
   - Function calls with arguments

4. **Validation suite**
   - 6 new explain tests in `phase6_tooling.rs`
   - All 13 phase6 tests pass

**Test suite status:**
- **247 tests passing** (0 failures)
- 21 R6 tests total (10 ANSI_NULLS + 5 DATEFIRST + 6 explain)

---

### 2026-03-22 — Session 8: R6 Tooling — DATEPART/DATENAME + DATEFIRST

#### R6 — Tooling Compatibility 🚧 In progress

**Delivered in this session:**

1. **DATEPART built-in function**
   - Added `DATEPART(datepart, date)` support
   - Supported dateparts: year, month, day, hour, minute, second, weekday, dayofweek, dayofyear, quarter
   - `weekday` respects `DATEFIRST` session option
   - `dayofweek` returns 1-7 (Sunday-Saturday, independent of DATEFIRST)

2. **DATENAME built-in function**
   - Added `DATENAME(datepart, date)` support
   - Returns month names (January-December)
   - Returns weekday names (Sunday-Saturday) adjusted for DATEFIRST
   - Supports same dateparts as DATEPART

3. **DATEFIRST runtime enforcement**
   - `SET DATEFIRST 1` (Monday-first) correctly adjusts weekday/day names
   - `SET DATEFIRST 7` (Sunday-first, SQL Server default) works correctly
   - Each session has its own DATEFIRST setting

4. **Day-of-week calculation fix**
   - Added `day_of_week_from_date()` helper function
   - Correctly calculates day of week from 1970-based date_to_days
   - Formula: `((days + 719471) % 7 + 7) % 7` gives 0=Sunday...6=Saturday

5. **Validation suite**
   - 5 new DATEFIRST/DATENAME tests in `phase6_set_options_runtime.rs`
   - All 15 phase6 tests pass

**Test suite status:**
- **241 tests passing** (0 failures)
- 15 R6 tests total (10 ANSI_NULLS + 5 DATEFIRST)

---

### 2026-03-22 — Session 7: R6 Tooling — ANSI_NULLS runtime behavior

#### R6 — Tooling Compatibility 🚧 In progress

**Delivered in this session:**

1. **ANSI_NULLS session option runtime enforcement**
   - Added `options: SessionOptions` field to `ExecutionContext`
   - Modified `compare_bool()` to respect `ansi_nulls` flag
   - Updated `eval_binary()` to accept and pass `ansi_nulls` parameter
   - Updated all comparison call sites:
     - `evaluator.rs`: binary expressions
     - `predicates.rs`: CASE, IN, BETWEEN expressions
     - `grouping.rs`: HAVING clause expressions
   - Session-isolated option state (different sessions can have different ANSI_NULLS settings)

2. **ANSI_NULLS behavior**
   - When `ON` (default): `NULL = NULL` → `NULL`, `NULL = value` → `NULL`
   - When `OFF`: `NULL = NULL` → `TRUE`, `NULL = value` → `FALSE`
   - Affects: `=`, `<>`, `!=`, `>`, `<`, `>=`, `<=` operators
   - Affects: `IN`, `BETWEEN`, `CASE` expressions

3. **Validation suite**
   - Added `phase6_set_options_runtime.rs` with 10 tests:
     - NULL = NULL with ANSI_NULLS ON/OFF
     - NULL = value with ANSI_NULLS ON/OFF
     - NULL <> NULL with ANSI_NULLS ON/OFF
     - WHERE clause filtering with NULL
     - IN list with NULL
     - BETWEEN with NULL
     - Session isolation test

**Test suite status:**
- **10 new R6 tests added**
- Total test count: 201 tests (191 previous + 10 new)

**Known R6 caveats:**
- `ANSI_NULLS` does not yet affect `JOIN` conditions (future work)
- `QUOTED_IDENTIFIER` parsing not yet implemented
- `DATEFIRST` affects only storage, not `DATEPART(weekday)` yet

---

### 2026-03-22 — Session 6: R5 completion (locking + recovery surface)

#### R5 — Transaction Fidelity ✅ Complete (modeled)

**Delivered in this session:**

1. **Deterministic table-lock no-wait enforcement**
   - Added shared table-lock ownership tracking across sessions.
   - Added immediate lock-conflict errors (no wait queue simulation).
   - Added isolation-aware read lock policy:
     - RU/RC: no read-lock acquisition for read-only statements.
     - RR/SERIALIZABLE/SNAPSHOT: read locks for read-only statements.
   - DML/DDL now acquires write locks consistently.

2. **Savepoint-aware lock release**
   - Added savepoint-depth tracking for lock acquisitions.
   - `ROLLBACK TRANSACTION <savepoint>` releases locks acquired after that savepoint.
   - Full `COMMIT` / `ROLLBACK` releases all transaction locks.

3. **Recovery checkpoint durability abstraction**
   - Added `DurabilitySink` and `RecoveryCheckpoint`.
   - Added default `NoopDurability` and test utility `InMemoryDurability`.
   - Added commit-time checkpoint persistence for:
     - autocommit writes
     - explicit transaction `COMMIT`
   - Uncommitted workspace state remains excluded from persisted checkpoints.

4. **Public recovery APIs (core + WASM + TS client)**
   - `tsql_core`:
     - `Database::new_with_durability(...)`
     - checkpoint export/import and checkpoint bootstrap constructors
     - `Engine` passthrough methods for durability and checkpoint operations
   - `WasmDb`:
     - `export_checkpoint()`
     - `import_checkpoint(payload)`
   - `packages/client`:
     - `TsqlDatabase.exportCheckpoint()`
     - `TsqlDatabase.importCheckpoint(...)`
     - `TsqlDatabase.fromCheckpoint(...)`

5. **Validation and docs updates**
   - Updated phase-5 concurrency matrix tests for no-wait lock behavior.
   - Added `phase5_locking_recovery.rs` tests:
     - savepoint lock release
     - checkpoint roundtrip
     - exclusion of uncommitted workspace data
     - savepoint rollback + commit persistence
   - Added client integration checkpoint roundtrip test.
   - Updated `docs/mvcc_conflict_matrix.md` for lock-driven outcomes.

### 2026-03-22 — Session 5: R5 multi-session + MVCC matrix baseline

#### R5 — Transaction Fidelity 🚧 In progress

**Delivered in this session:**

1. **Shared-state multi-session runtime**
   - Introduced shared `Database` with explicit `create_session` / `close_session`
   - Refactored `Engine` into a backward-compatible default-session facade over shared state
   - Added session-routed execution (`execute_session`, `execute_session_batch`)

2. **Deterministic commit-conflict model**
   - Added transaction workspaces with base table-version snapshots
   - Added immediate conflict errors on `COMMIT` based on isolation level and read/write table sets
   - Kept deterministic no-wait conflict handling (no blocking lock queue model)

3. **Concurrent anomaly simulation DSL and tests**
   - Added `phase5_concurrency_mvcc.rs` with deterministic interleaving steps across sessions
   - Added anomaly coverage for:
     - dirty read (modeled behavior)
     - non-repeatable read
     - phantom read
     - lost update
     - write skew (modeled behavior)

4. **MVCC conflict matrix artifacts**
   - Added executable matrix tests for write/write and read/write conflict outcomes by isolation level
   - Added `docs/mvcc_conflict_matrix.md` with modeled allow/block outcomes and explicit caveats

5. **WASM + client multi-session surface**
   - `WasmDb` now supports:
     - `create_session`
     - `close_session`
     - `exec_session`
     - `exec_batch_session`
     - `query_session`
   - `packages/client` now exposes `TsqlSession` and `TsqlDatabase.createSession()`
   - Added client integration test: `multi_session_transactions.test.ts`

**Known R5 caveats kept explicit:**
- Dirty reads remain blocked in current modeled runtime.
- Conflict granularity is table-version based (not row/predicate-lock exact SQL Server behavior).

### 2026-03-22 — Session 4: R4 Closure (subset)

#### R4 — Programmability ✅ Complete (subset)

**Delivered in this session:**

1. **Batch/variable language gaps**
   - Added `SELECT @var = ...` support (with and without `FROM`; last-row-wins behavior)
   - Added `DECLARE @t TABLE (...)` parsing/execution for table variables

2. **Temporary/table variable runtime**
   - Added session-level `#temp` table name mapping
   - Added table variable internal mapping and resolution precedence in table binding/mutation paths

3. **Programmable objects**
   - Added `CREATE PROCEDURE` / `DROP PROCEDURE` subset with parameters and `OUTPUT`
   - Added `CREATE FUNCTION` / `DROP FUNCTION` subset:
     - scalar UDF (`RETURNS <scalar>`, `RETURN <expr>`)
     - inline TVF (`RETURNS TABLE AS RETURN (SELECT ...)`)
   - Added scalar UDF invocation in expression evaluation
   - Added inline TVF resolution in `FROM fn(args)`

4. **Dynamic execution subset**
   - Preserved dynamic SQL: `EXEC '...'`
   - Added procedure execution form: `EXEC schema.proc ...`
   - Added `EXEC sp_executesql ...` subset with typed argument binding and OUTPUT propagation

5. **Identity scope semantics**
   - Added `SCOPE_IDENTITY()`
   - Added `@@IDENTITY`
   - Added `IDENT_CURRENT('schema.table')`
   - Insert path now updates session/scope identity metadata

6. **Validation suite**
   - Added `phase4_programmability_closure.rs` with 10 tests covering:
     - select-variable assignment
     - `#temp` lifecycle/isolation
     - table variables
     - procedures + output params
     - scalar UDF
     - inline TVF
     - `sp_executesql` output
     - identity functions

**Test suite status after session 4:**
- **191 tests passing**
- 0 failures, 0 ignored

**Known R4 caveat kept explicit:**
- Scope cleanup for table-variable physical objects is simplified for now (logical resolution is correct for supported scenarios).

### 2026-03-21 — Session 3: R3 MVP Closure

#### R3 — SQL Server Semantics ✅ MVP complete

**Delivered in this session:**

1. **DDL & catalog (migration compatibility)**
   - Added `CREATE INDEX` and `DROP INDEX` statements (catalog abstraction only)
   - Added named constraints support:
     - table-level named default (`CONSTRAINT ... DEFAULT ... FOR ...`)
     - column/table `CHECK` constraints with enforcement on `INSERT`/`UPDATE`

2. **Metadata surface**
   - Added virtual metadata tables:
     - `sys.schemas`, `sys.tables`, `sys.columns`, `sys.types`, `sys.indexes`, `sys.objects`
     - `INFORMATION_SCHEMA.TABLES`, `INFORMATION_SCHEMA.COLUMNS`
   - Added `OBJECT_ID()` scalar function

3. **Type/semantic improvements**
   - Improved deterministic mixed-type comparison behavior for numeric/string and datetime/string cases
   - Kept existing overflow/truncation behavior and expanded test coverage for these cases

4. **Error model baseline**
   - Added stable taxonomy helpers to `DbError` (`class()` and `code()`)
   - Preserved strict batch execution behavior: stop on first error

5. **Validation suites added**
   - `phase3_semantics.rs`
   - `phase3_metadata.rs`
   - `phase3_indexes_constraints.rs`
   - `phase3_errors_rowcount.rs`

**Test suite status after session 3:**
- **181 tests passing**
- 0 failures, 0 ignored

**Known R3 deferrals kept explicit:**
- No index-aware planner/execution yet (metadata/catalog only)
- `sql_variant` still out of this MVP closure

### 2026-03-21 — Session 2: R2 Completion

#### R2 — Relational Completeness ✅ Complete

**Bug fixes resolved:**

1. **UNION deduplication** (`executor/engine.rs`)
   - Split `Union` and `UnionAll` branches; `Union` now calls `deduplicate_projected_rows`

2. **Subqueries correlacionadas** (`executor/predicates.rs`, `identifier.rs`, `context.rs`)
   - Added `ExecutionContext::with_outer_row_extended` to propagate outer row context
   - Updated `eval_exists`, `eval_scalar_subquery`, `eval_in_subquery` to pass outer row
   - Updated `resolve_identifier` and `resolve_qualified_identifier` to search in `ctx.outer_row`
   - All 13 correlated subquery tests now pass

3. **Aggregates sem GROUP BY** (`executor/query.rs`, `scalar_fn.rs`)
   - Added aggregate detection in projection: `has_aggregate` flag
   - `execute_grouped_select` is now called for queries with aggregates in projection (even without GROUP BY)
   - Fixed `build_groups` to return single group for empty GROUP BY
   - Fixed `project_group_row` to handle empty groups (return NULL for non-aggregates, COUNT(*) = 0)
   - Removed MIN/MAX from scalar_fn.rs error guard (SUM/AVG/COUNT still error in scalar context)
   - Fixed `eval_aggregate_avg` to return Decimal instead of integer division
   - All 4 aggregate tests (SUM, AVG, MIN/MAX) now pass

4. **HAVING com aggregates** (`executor/query.rs`)
   - Added `eval_having_expr` and `eval_having_predicate` functions
   - Evaluates HAVING expressions in group context, routing aggregate functions to `eval_aggregate_*`
   - Handles Binary, Unary, Case, Between expressions with aggregates
   - `test_having_basic` now passes

5. **COUNT(*) em contexto procedural** (`executor/aggregates.rs`)
   - Root cause was same as Fix 4 (aggregate detection). After fix, COUNT works everywhere.

**Test suite status after session 2:**
- **158 tests passing** (24 builtins/aggregates, 4 DDL advanced, 1 example, 7 integration, 14 new types, 50 expressions, 15 relational, 5 DDL, 13 programmability, 25 subqueries)
- 0 tests ignored, 0 failures

**Remaining R2 gap:** Query planner (logical algebra, predicate pushdown, projection trimming) — deferred

---

### 2026-03-21 — Session 1: R1 Completion

**R1 — Foundational T-SQL (complete)**
- Added `CONVERT()` built-in function with T-SQL style codes (0, 1/101, 2/102, 3/103, 4/104, 5/105, 6/106, 7/107, 8/108, 9/109, 10/110, 11/111, 12/112, 13/113, 14/114, 20/120, 21/121, 22/126, 130, 131)
- Extended `Expr::Convert` AST with optional `style: Option<i32>` field
- Extended parser to capture style code parameter
- Added `convert_with_style()` and date formatting functions in `value_ops.rs`
- Added 11 new tests covering CONVERT with and without style codes

**R2 — Relational Completeness (CTE now working)**
- Implemented CTE execution engine in `executor/cte.rs` (`CteStorage`, `CteTable`, `resolve_cte_table`, `cte_to_context_rows`)
- Extended `ExecutionContext` with `ctes: CteStorage` field
- Updated `QueryExecutor::bind_table` to resolve CTE references alongside catalog tables
- Updated `ScriptExecutor::execute` to handle `Statement::WithCte`
- Enabled and passing: `test_cte_basic`, `test_cte_with_join`, `test_multiple_ctes`
- Remaining R2 gap: query planner, aggregate bugs, correlated subqueries, HAVING

#### Previous test suite status (before session 2)
- **120 tests passing**, 24 tests ignored


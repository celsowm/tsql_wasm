# Compatibility Backlog

## Purpose

This backlog converts the roadmap into execution-sized work. It is intentionally biased toward end-to-end compatibility work instead of isolated parser or executor changes.

Priority values:

- `P0`: current blocker for parity measurement or common client/app workflows
- `P1`: major compatibility gap for supported scenarios
- `P2`: important, but can follow after the main parity loop is stable
- `P3`: later-scope server features or hardening items

## P0

### B001: Create and maintain the compatibility matrix

- Phase: 0
- Priority: P0
- Goal: keep one source of truth for what is exact, subset, shim, or unsupported
- Deliverables:
  - maintain `docs/compatibility-matrix.md`
  - add owner and test references per row
  - update matrix in the same change as compatibility work
- Exit criteria:
  - no feature ships without a matrix status

### B002: Upgrade the differential harness

- Phase: 0
- Priority: P0
- Goal: compare behavior, not just values
- Primary areas:
  - `scripts/test-compat.ps1`
  - `scripts/compat-runner/*`
- Deliverables:
  - compare rowcount
  - compare column names and type shape
  - compare error number / class / message pattern
  - compare multi-result-set shape where relevant
- Exit criteria:
  - a failing differential run tells you exactly what diverged

### B003: Inventory explicit unsupported branches

- Phase: 0
- Priority: P0
- Goal: convert hidden unsupported behavior into tracked work
- Primary areas:
  - parser unsupported branches
  - RPC unsupported branches
  - metadata stubs
- Deliverables:
  - backlog item per explicit unsupported path
  - matrix row linked to each item
  - unsupported parser branches tracked from `crates/tsql_core/src/parser/parse/mod.rs` and `crates/tsql_core/src/executor/tooling/session_options.rs`
  - RPC fallback currently visible in `crates/tsql_server/src/session/mod.rs`:
    unsupported RPC requests now return an explicit parse error
- Exit criteria:
  - no explicit unsupported branch exists without a tracking item

### B004: Expand SSMS contract coverage

- Phase: 2 and 3
- Priority: P0
- Goal: move beyond bootstrap and table listing
- Primary areas:
  - `crates/tsql_server/tests/ssms_object_explorer_contract.rs`
  - `crates/tsql_server/tests/fixtures/ssms_object_explorer_cases.json`
- Deliverables:
  - add contract cases for indexes
  - add contract cases for constraints
  - add contract cases for routines
  - add contract cases for database and server property probes
  - keep the fixture and test scopes aligned as new Object Explorer pages are added
- Exit criteria:
  - Object Explorer scenarios cover more than initial metadata startup

## P1

### B005: Track grouped join / `PIVOT` / `UNPIVOT` compatibility corners

- Phase: 1
- Priority: P1
- Goal: keep the remaining grouped-join and pivoting corners visible and tested
- Primary areas:
  - `crates/tsql_core/src/parser/lower/dml.rs`
  - query executor / transformer modules
- Deliverables:
  - end-to-end query tests against SQL Server reference behavior
  - keep grouped-join aliasing and derived subquery set-op behavior covered
- Exit criteria:
  - the remaining compatibility corners are described by tests, not surprise failures

### B006: Broaden RPC coverage beyond `sp_executesql` and `sp_prepexec`

- Phase: 3
- Priority: P1
- Goal: make unsupported RPC requests explicit and expand supported RPC coverage
- Primary areas:
  - `crates/tsql_server/src/tds/rpc/*`
  - `crates/tsql_server/src/session/mod.rs`
- Deliverables:
  - supported RPC selector inventory
  - explicit support plan for SSMS / ADS / driver-required RPC procedures
  - regression tests for each added RPC family
  - tracked fallback for unsupported RPC packets remains visible in session handling
- Exit criteria:
  - no required client path depends on silent RPC ignoring

### B007: Replace HADR and availability metadata stubs with a defined compatibility model

- Phase: 2 and 7
- Priority: P1
- Goal: remove ambiguous empty-row behavior where tools probe these views
- Primary areas:
  - `crates/tsql_core/src/executor/metadata/sys/hadr.rs`
- Deliverables:
  - either real modeled data, or a deliberate documented shim contract
  - SSMS probe tests covering expected behavior
- Exit criteria:
  - availability-related metadata is not an undocumented empty stub

### B008: Expand metadata fidelity for object, index, constraint, routine, and parameter queries

- Phase: 2
- Priority: P1
- Goal: reduce metadata-specific tool breakage
- Primary areas:
  - `crates/tsql_core/src/executor/metadata/*`
  - `crates/tsql_core/src/executor/scalar/metadata/*`
- Deliverables:
  - coverage for common `sys.*` and property-function probes
  - column-shape tests for metadata rowsets
  - differential metadata suite
- Exit criteria:
  - supported tools do not need ad hoc metadata hacks

### B009: Make client compatibility explicit by matrix

- Phase: 3
- Priority: P1
- Goal: define what is supported rather than inferring it from anecdotal success
- Deliverables:
  - client matrix for SSMS, ADS, `sqlcmd`, `tedious`, `tiberius`
  - smoke tests and status per client
- Exit criteria:
  - each supported client has a written contract and regression coverage

### B010: Tighten transaction and lock differential testing

- Phase: 4
- Priority: P1
- Goal: move from local concurrency behavior to SQL Server-aligned behavior
- Primary areas:
  - `crates/tsql_core/tests/phase5_transactions.rs`
  - `crates/tsql_core/tests/phase5_row_locking.rs`
  - `crates/tsql_core/tests/concurrency_deadlock.rs`
- Deliverables:
  - differential multi-session matrix
  - savepoint and nested transaction behavior matrix
  - deadlock victim and timeout behavior checks
- Exit criteria:
  - concurrency deviations are known and reproducible

## P2

### B011: Implement physical index usage in the planner

- Phase: 5
- Priority: P2
- Goal: close the gap between index catalog support and actual execution behavior
- Primary areas:
  - `crates/tsql_core/src/catalog/index_registry.rs`
  - `crates/tsql_core/src/executor/query/*`
- Deliverables:
  - physical index structures used by scans
  - tests proving seek / scan behavior for supported scenarios
- Exit criteria:
  - README limitation about planner-only table scans can be removed or narrowed

### B012: Design durable persistence beyond checkpoint export/import

- Phase: 5
- Priority: P2
- Goal: define a real recovery model
- Primary areas:
  - `crates/tsql_core/src/executor/database/persistence/*`
  - `crates/tsql_core/src/executor/journal.rs`
  - `crates/tsql_core/src/storage/*`
- Deliverables:
  - persistence architecture decision
  - WAL or equivalent logging model
  - restart and crash-recovery tests
- Exit criteria:
  - checkpoint-only persistence is no longer the only durability story

### B013: Build a security model design doc

- Phase: 6
- Priority: P2
- Goal: avoid retrofitting principals and permissions into a catalog that assumes global visibility
- Deliverables:
  - principal model
  - role model
  - permission evaluation model
  - metadata visibility rules
  - explicit decision on integrated auth scope
- Exit criteria:
  - security implementation can start without redesigning metadata assumptions later

### B014: Add permission-aware metadata visibility

- Phase: 6
- Priority: P2
- Goal: align object discovery with SQL Server-style visibility rules
- Primary areas:
  - metadata executors
  - server login and session context
- Deliverables:
  - permission-filtered metadata queries
  - tests by principal and role
- Exit criteria:
  - metadata is no longer globally visible by default for protected scenarios

## P3

### B015: Classify admin-surface feature families

- Phase: 7
- Priority: P3
- Goal: stop leaving major SQL Server feature families undefined
- Deliverables:
  - one row per feature family in the compatibility matrix
  - status of `implement`, `shim`, or `unsupported`
- Exit criteria:
  - no major server feature family remains ambiguous

### B016: Add parser and protocol fuzz hardening gates

- Phase: 8
- Priority: P3
- Goal: keep compatibility stable as surface area expands
- Deliverables:
  - parser fuzz targets
  - TDS/protocol fuzz targets
  - seeded repro corpus
- Exit criteria:
  - regressions are caught through automated hardening, not manual discovery

### B017: Add long-run soak and performance regression checks

- Phase: 8
- Priority: P3
- Goal: detect compatibility cliffs under repeated or concurrent usage
- Deliverables:
  - soak suite
  - memory and latency thresholds for key flows
- Exit criteria:
  - supported scenarios have guardrails, not just correctness tests

### B018: Implement missing INFORMATION_SCHEMA views

- Phase: 2
- Priority: P2
- Goal: provide real metadata for standard INFORMATION_SCHEMA views
- Primary areas:
  - `crates/tsql_core/src/executor/metadata/info_schema_empty.rs`
- Deliverables:
  - implementation for `COLUMN_DOMAIN_USAGE`, `DOMAINS`, `DOMAIN_CONSTRAINTS`
  - implementation for `TABLE_PRIVILEGES`, `COLUMN_PRIVILEGES`
  - implementation for `VIEW_COLUMN_USAGE`, `VIEW_TABLE_USAGE`
  - implementation for `ROUTINE_COLUMNS`
- Exit criteria:
  - these views return data aligned with the engine's catalog instead of empty sets

### B019: Implement Partitioning and HADR metadata stubs

- Phase: 2 and 7
- Priority: P2
- Goal: replace stubs for partitioning and availability groups with modeled data or documented shims
- Primary areas:
  - `crates/tsql_core/src/executor/metadata/sys/partition.rs`
  - `crates/tsql_core/src/executor/metadata/sys/hadr.rs`
- Deliverables:
  - implementation for `partition_functions`, `partition_schemes`, etc.
  - implementation for `availability_replicas`, `availability_groups`, etc.
- Exit criteria:
  - metadata probes for partitioning and HADR behave predictably for tools

### B020: Handle or explicitly reject unsupported session options

- Phase: 1
- Priority: P2
- Goal: move from silent "Unsupported" variant to explicit behavior for SET options
- Primary areas:
  - `crates/tsql_core/src/parser/parse/mod.rs`
  - `crates/tsql_core/src/executor/tooling/session_options.rs`
- Deliverables:
  - inventory of common `SET` options that hit the `Unsupported` branch
  - explicit implementation or `DbError::Unsupported` for each
- Exit criteria:
  - no unknown `SET` option is silently captured as an internal `Unsupported` enum without a user-visible result

### B021: Expand RPC support for cursors and prepared statements

- Phase: 3
- Priority: P1
- Goal: support driver-level RPC calls beyond simple batch execution
- Primary areas:
  - `crates/tsql_server/src/tds/rpc/parser.rs`
  - `crates/tsql_server/src/session/mod.rs`
- Deliverables:
  - support for `sp_cursoropen`, `sp_cursorfetch`, `sp_cursorclose`
  - support for `sp_prepare`, `sp_execute`, `sp_unprepare`
- Exit criteria:
  - ADO.NET and other drivers using these RPCs can function correctly

## Suggested First Execution Slice

1. Finish B001 and keep the matrix current.
2. Do B002 so every later phase can be measured properly.
3. Do B003 to expose the real unsupported backlog.
4. Run B004 and B008 in parallel, because metadata and tooling compatibility are already active pressure points.
5. Start B006 once the client matrix in B009 is defined, so protocol work is driven by actual client requirements.

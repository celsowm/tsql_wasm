# tsql_wasm Compatibility Matrix

## Purpose

This document is the current compatibility scoreboard for the project.

Status values:

- `exact`: intended SQL Server-equivalent behavior is implemented and covered
- `compatible subset`: useful support exists, but the surface or semantics are still incomplete
- `shim`: behavior exists mainly to satisfy tooling or client compatibility, not as a full implementation
- `unsupported`: not implemented for the supported scope

The baseline below is seeded from the current README, tests, and explicit code-level limitations. It should be updated alongside feature work.

## Language Surface

| Area | Status | Notes | Evidence |
|---|---|---|---|
| Core DDL (`CREATE/ALTER/DROP/TRUNCATE TABLE`, schemas, indexes) | compatible subset | Broad support exists, but index execution behavior is not SQL Server-like yet | `README.md`, `crates/tsql_core/tests/phase3_ddl.rs`, `crates/tsql_core/tests/phase3_indexes_constraints.rs` |
| Core DML (`SELECT/INSERT/UPDATE/DELETE`) | compatible subset | Strong coverage exists, but edge-case behavior still needs differential closure | `README.md`, `crates/tsql_core/tests/sqlserver/*`, `crates/tsql_core/tests/phase9_dml_advanced.rs` |
| Query features (`JOIN`, `GROUP BY`, `HAVING`, set ops, CTEs) | compatible subset | Major surface is present, including grouped-join alias lowering and set-op subqueries; remaining compatibility corners are documented in B003 / B005 | `README.md`, `crates/tsql_core/src/parser/lower/dml.rs`, `docs/compatibility-backlog.md` |
| Window functions | compatible subset | Supported and tested, but not yet declared exact | `README.md`, `crates/tsql_core/tests/phase9_window_functions.rs` |
| Procedural T-SQL (`DECLARE`, `SET`, `IF`, `WHILE`, `TRY/CATCH`) | compatible subset | Broad procedural subset is present | `README.md`, `crates/tsql_core/tests/phase4_programmability.rs`, `crates/tsql_core/tests/try_catch_test.rs` |
| Stored procedures / UDF / TVF | compatible subset | Supported as a subset, not full SQL Server programmability | `README.md`, `crates/tsql_core/tests/phase4_programmability_closure.rs`, `crates/tsql_core/tests/p1_16_read_only_udf_regression.rs` |
| Dynamic SQL / `sp_executesql` | compatible subset | Important paths exist, but broader RPC and dynamic execution parity is incomplete | `README.md`, `crates/tsql_core/src/executor/script/procedural/sp_executesql.rs`, `crates/tsql_server/src/tds/rpc/parser.rs` |
| Cursors | compatible subset | Syntax and executor support exist, but client/protocol parity is not yet complete | `crates/tsql_core/tests/cursor_extended_test.rs`, `crates/tsql_core/src/executor/script/procedural/cursor.rs` |
| Temporary tables and table variables | compatible subset | Supported in core engine coverage | `README.md`, `crates/tsql_core/tests/tvp_and_xact_state.rs` |
| Full SQL Server syntax surface | unsupported | The engine supports a large subset, not the complete language | aggregate repo state |

## Types and Built-ins

| Area | Status | Notes | Evidence |
|---|---|---|---|
| Common numeric, string, date/time, GUID types | compatible subset | README documents a practical subset, not the full SQL Server type system | `README.md` |
| Conversion semantics (`CAST`, `CONVERT`, `TRY_CAST`, `TRY_CONVERT`) | compatible subset | Supported in important paths, but full parity still needs differential validation | `README.md`, `crates/tsql_core/tests/try_cast_try_convert.rs`, `crates/tsql_core/tests/sqlserver_conversion.rs` |
| String built-ins | compatible subset | Common functions are present and tested | `README.md`, `crates/tsql_core/tests/new_string_functions.rs`, `crates/tsql_core/tests/sqlserver_strings.rs` |
| Math built-ins | compatible subset | Good coverage, still not a claim of exact parity | `README.md`, `crates/tsql_core/tests/new_math_functions.rs`, `crates/tsql_core/tests/sqlserver_math.rs` |
| Date/time built-ins | compatible subset | Common functions are present and tested | `README.md`, `crates/tsql_core/tests/sqlserver_dates.rs` |
| Metadata / system built-ins | compatible subset | Important helpers exist, but many SQL Server properties and system functions remain incomplete | `crates/tsql_core/tests/new_system_functions.rs`, `crates/tsql_core/src/executor/scalar/system/*` |
| Full collation and type-fidelity behavior | unsupported | Not declared or evidenced as SQL Server-complete | aggregate repo state |

## Metadata and Catalog

| Area | Status | Notes | Evidence |
|---|---|---|---|
| `INFORMATION_SCHEMA` | compatible subset | Extensive support exists, but several views remain as empty stubs | `crates/tsql_core/tests/information_schema.rs`, `crates/tsql_core/src/executor/metadata/info_schema_empty.rs` |
| `INFORMATION_SCHEMA.VIEW_TABLE_USAGE` | compatible subset | Backed by catalog view dependencies for supported views | `crates/tsql_core/src/executor/metadata/info_schema_views.rs`, `crates/tsql_core/tests/information_schema.rs` |
| `INFORMATION_SCHEMA.ROUTINE_COLUMNS` | compatible subset | Backed for inline table-valued functions with a single source table | `crates/tsql_core/src/executor/metadata/info_schema_routine_columns.rs`, `crates/tsql_core/tests/information_schema.rs` |
| `INFORMATION_SCHEMA.VIEW_COLUMN_USAGE` | compatible subset | Backed for supported views with direct column lineage across base tables, joins, grouping, predicates, and nested subqueries | `crates/tsql_core/src/executor/metadata/info_schema_views.rs`, `crates/tsql_core/tests/information_schema.rs`, `crates/tsql_server/tests/ssms_object_explorer_contract.rs` |
| `INFORMATION_SCHEMA` domain views (`COLUMN_DOMAIN_USAGE`, `DOMAINS`, `DOMAIN_CONSTRAINTS`) | shim | Still intentionally empty where the engine does not yet have domain metadata | `crates/tsql_core/src/executor/metadata/info_schema_empty.rs`, `crates/tsql_core/tests/information_schema.rs` |
| Core `sys.*` catalog views | compatible subset | Several important views exist, but partitioning and other areas are currently empty stubs | `README.md`, `crates/tsql_core/src/executor/metadata/sys/*`, `crates/tsql_core/src/executor/metadata/sys/partition.rs` |
| SSMS Object Explorer bootstrap / table enumeration / property probes | shim | Current contract replay covers bootstrap, tables, indexes, constraints, routines, triggers, view column usage, and database/server property probes | `crates/tsql_server/tests/ssms_object_explorer_contract.rs`, `crates/tsql_server/tests/fixtures/ssms_object_explorer_cases.json` |
| HADR / availability metadata | shim | Stub views exist and intentionally return empty results | `crates/tsql_core/src/executor/metadata/sys/hadr.rs` |
| Full server-level metadata surface | unsupported | Not present as a complete implementation | aggregate repo state |

## TDS and Client Protocol

| Area | Status | Notes | Evidence |
|---|---|---|---|
| TDS login / prelogin / basic batch execution | compatible subset | Core flows exist and are tested | `crates/tsql_server/src/session/mod.rs`, `crates/tsql_server/tests/basic.rs` |
| TLS support | compatible subset | TLS is implemented, but parity depends on negotiation and client behavior details | `crates/tsql_server/src/tls.rs`, `crates/tsql_server/src/tds_tls_io.rs`, `crates/tsql_server/tests/security.rs` |
| RPC support | compatible subset | Only `sp_executesql` and `sp_prepexec` are supported; other RPC packets now return an explicit unsupported error in session handling | `crates/tsql_server/src/tds/rpc/parser.rs`, `crates/tsql_server/src/session/mod.rs`, `docs/compatibility-backlog.md` |
| Full SQL Server RPC surface | unsupported | Explicit unsupported RPC requests now produce an error response instead of being silently ignored | `crates/tsql_server/src/session/mod.rs` |
| SSMS / ADS connectivity | compatible subset | Supported in meaningful paths, but still dependent on compatibility-focused shims and partial protocol coverage | `README.md`, `crates/tsql_server/tests/ssms_object_explorer_contract.rs` |
| ADO.NET / ODBC / JDBC parity | unsupported | Not defined as complete today | aggregate repo state |

## Transactions, Locking, and Recovery

| Area | Status | Notes | Evidence |
|---|---|---|---|
| Multi-session transactions | compatible subset | There is meaningful concurrency and transaction coverage | `README.md`, `crates/tsql_core/tests/phase5_transactions.rs` |
| Locking and deadlocks | compatible subset | Locking and deadlock behavior are tested, but not yet declared SQL Server-equivalent | `crates/tsql_core/tests/concurrency_deadlock.rs`, `crates/tsql_core/tests/phase5_row_locking.rs` |
| Savepoints / nested transaction state / `XACT_STATE` | compatible subset | Targeted support exists | `crates/tsql_core/tests/nested_transactions_state.rs`, `crates/tsql_core/tests/tvp_and_xact_state.rs` |
| Checkpoint export / import recovery | compatible subset | Implemented and tested for embedded workflows | `README.md`, `crates/tsql_core/tests/phase5_locking_recovery.rs`, `crates/tsql_core/tests/phase8_persistence.rs` |
| WAL / page persistence / crash recovery | unsupported | README explicitly calls this out as missing | `README.md` |

## Physical Engine

| Area | Status | Notes | Evidence |
|---|---|---|---|
| Logical index catalog | compatible subset | Index metadata exists and persists | `README.md`, `crates/tsql_core/tests/phase8_persistence.rs` |
| Planner index usage | unsupported | README explicitly states planner still uses table scans | `README.md` |
| Cost-based optimization / statistics | unsupported | No evidence of SQL Server-class optimizer behavior | aggregate repo state |

## Security and Visibility

| Area | Status | Notes | Evidence |
|---|---|---|---|
| SQL authentication for server login | compatible subset | Basic username/password path exists in `tsql_server` | `crates/tsql_server/src/session/mod.rs`, `crates/tsql_server/src/lib.rs` |
| TLS-backed client login flow | compatible subset | Implemented for server connectivity | `crates/tsql_server/src/session/mod.rs`, `crates/tsql_server/tests/security.rs` |
| Principals, roles, grants, deny, revoke | unsupported | No full SQL Server security model is present | aggregate repo state |
| Metadata visibility by permission | unsupported | Not present as a full access-control model | aggregate repo state |
| Integrated / Windows authentication | unsupported | Not implemented in the current server | aggregate repo state |

## Admin and Server Features

| Area | Status | Notes | Evidence |
|---|---|---|---|
| Playground server for local tooling | shim | Useful for compatibility testing, not a SQL Server-equivalent admin surface | `README.md`, `crates/tsql_server/src/playground/*`, `scripts/start-playground-sa.ps1` |
| Backup / restore | unsupported | No implementation present | aggregate repo state |
| SQL Agent | unsupported | No implementation present | aggregate repo state |
| Linked servers | unsupported | No implementation present | aggregate repo state |
| Service Broker | unsupported | No implementation present | aggregate repo state |
| CDC / replication / temporal / partitioning / columnstore / full-text / SQLCLR | unsupported | No implementation present | aggregate repo state |

## Immediate Gaps to Convert into Work

- Expand the matrix row-by-row with exact source links once features change.
- Split `compatible subset` rows into finer-grained subfeatures where the status is hiding too much variability.
- Replace `aggregate repo state` evidence with concrete doc and test references as features are implemented.
- Add a dated snapshot section once the matrix starts changing frequently.

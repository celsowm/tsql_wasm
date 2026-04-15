# Iridium SQL Compatibility Matrix

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
| Core DDL (`CREATE/ALTER/DROP/TRUNCATE TABLE`, schemas, indexes) | compatible subset | Broad support exists, but index execution behavior is not SQL Server-like yet | `README.md`, `crates/iridium_core/tests/phase3_ddl.rs`, `crates/iridium_core/tests/phase3_indexes_constraints.rs` |
| Core DML (`SELECT/INSERT/UPDATE/DELETE`) | compatible subset | Strong coverage exists, but edge-case behavior still needs differential closure | `README.md`, `crates/iridium_core/tests/sqlserver/*`, `crates/iridium_core/tests/phase9_dml_advanced.rs` |
| `MERGE` statement | compatible subset | Fully implemented including `WHEN MATCHED`, `WHEN NOT MATCHED`, and `WHEN NOT MATCHED BY SOURCE` with UPDATE/DELETE actions, conditions, and OUTPUT clause | `crates/iridium_core/tests/merge_statement.rs`, `crates/iridium_core/src/executor/script/dml/merge_helpers.rs` |
| Query features (`JOIN`, `GROUP BY`, `HAVING`, set ops, CTEs, `APPLY`) | compatible subset | Major surface is present, including grouped-join alias lowering and set-op subqueries; remaining compatibility corners are documented in B003 / B005 | `README.md`, `crates/iridium_core/src/parser/lower/dml.rs`, `docs/compatibility-backlog.md`, `crates/iridium_core/tests/cross_join_apply.rs` |
| `PIVOT` and `UNPIVOT` | compatible subset | Fully implemented including statistical aggregates (STDEV, STDEVP, VAR, VARP); NULL handling in UNPIVOT matches SQL Server (skips NULLs); tests added in Phase 1 | `crates/iridium_core/src/parser/parse/statements/query.rs`, `crates/iridium_core/src/executor/query/transformer/pivot.rs`, `crates/iridium_core/src/executor/query/transformer/unpivot.rs`, `crates/iridium_core/tests/phase1_pivot.rs`, `crates/iridium_core/tests/phase1_unpivot.rs` |
| Window functions | compatible subset | Supported and tested, but not yet declared exact | `README.md`, `crates/iridium_core/tests/phase9_window_functions.rs` |
| Procedural T-SQL (`DECLARE`, `SET`, `IF`, `WHILE`, `TRY/CATCH`) | compatible subset | Broad procedural subset is present | `README.md`, `crates/iridium_core/tests/phase4_programmability.rs`, `crates/iridium_core/tests/try_catch_test.rs` |
| Stored procedures / UDF / TVF | compatible subset | Supported as a subset, not full SQL Server programmability | `README.md`, `crates/iridium_core/tests/phase4_programmability_closure.rs`, `crates/iridium_core/tests/p1_16_read_only_udf_regression.rs` |
| Dynamic SQL / `sp_executesql` | compatible subset | Important paths exist, but broader RPC and dynamic execution parity is incomplete | `README.md`, `crates/iridium_core/src/executor/script/procedural/sp_executesql.rs`, `crates/iridium_server/src/tds/rpc/parser.rs` |
| Cursors | compatible subset | Syntax and executor support exist; RPC cursor operations (`sp_cursoropen`, `sp_cursorfetch`, `sp_cursorclose`) now supported via TDS | `crates/iridium_core/tests/cursor_extended_test.rs`, `crates/iridium_core/src/executor/script/procedural/cursor.rs`, `crates/iridium_server/src/tds/rpc/parser.rs` |
| Temporary tables and table variables | compatible subset | `#temp` / `##global_temp` DDL, DML, and DROP supported with session-scoped name mapping and automatic cleanup | `README.md`, `crates/iridium_core/tests/tvp_and_xact_state.rs`, `crates/iridium_core/tests/phase4_programmability_closure.rs` |
| Full SQL Server syntax surface | unsupported | The engine supports a large subset, not the complete language | aggregate repo state |

## Types and Built-ins

| Area | Status | Notes | Evidence |
|---|---|---|---|
| Common numeric, string, date/time, GUID types | compatible subset | README documents a practical subset, not the full SQL Server type system; Unicode string literals (N'...') and NVarChar/NChar types now supported | `README.md`, `crates/iridium_core/src/parser/lexer.rs`, `crates/iridium_core/tests/new_string_functions.rs` |
| Conversion semantics (`CAST`, `CONVERT`, `TRY_CAST`, `TRY_CONVERT`) | compatible subset | Supported in important paths; DATE/TIME to DATETIME/DATETIME2, BINARY to UNIQUEIDENTIFIER, DECIMAL identity now supported; full parity still needs differential validation | `README.md`, `crates/iridium_core/tests/try_cast_try_convert.rs`, `crates/iridium_core/tests/sqlserver_conversion.rs` |
| String built-ins | compatible subset | Extensive support including `LEN`, `DATALENGTH`, `SUBSTRING`, `UPPER`, `LOWER`, `LTRIM`, `RTRIM`, `TRIM`, `REPLACE`, `LEFT`, `RIGHT`, `CHARINDEX`, `ASCII`, `CHAR`, `NCHAR`, `UNICODE`, `CONCAT`, `CONCAT_WS`, `REPLICATE`, `REVERSE`, `STUFF`, `SPACE`, `STR`, `TRANSLATE`, `FORMAT`, `PATINDEX`, `SOUNDEX`, `DIFFERENCE`, and `STRING_ESCAPE` (JSON, HTML, XML, CSV) | `crates/iridium_core/src/executor/scalar/builtin_registry.rs`, `crates/iridium_core/tests/new_string_functions.rs` |
| Math built-ins | compatible subset | Strong coverage including `ABS`, `CEILING`, `FLOOR`, `ROUND`, `SQRT`, `SQUARE`, `POWER`, `EXP`, `LOG`, `LOG10`, `SIN`, `COS`, `TAN`, `ASIN`, `ACOS`, `ATAN`, `ATN2`, `COT`, `DEGREES`, `RADIANS`, `PI`, `SIGN`, `RAND`, and `CHECKSUM` | `crates/iridium_core/src/executor/scalar/builtin_registry.rs`, `crates/iridium_core/tests/new_math_functions.rs` |
| Date/time built-ins | compatible subset | Common functions present and tested including `GETDATE`, `CURRENT_TIMESTAMP`, `GETUTCDATE`, `SYSDATETIME`, `SYSUTCDATETIME`, `DATEPART`, `DATENAME`, `DATEDIFF`, `DATEDIFF_BIG`, `DATEADD`, `EOMONTH`, `ISDATE`, `YEAR`, `MONTH`, and `DAY` | `crates/iridium_core/src/executor/scalar/builtin_registry.rs`, `crates/iridium_core/tests/sqlserver_dates.rs` |
| Metadata / system built-ins | compatible subset | Extensive support for metadata functions (`OBJECT_ID`, `OBJECT_NAME`, `SCHEMA_NAME`, `TYPE_NAME`, `COLUMNPROPERTY`, `DATABASEPROPERTYEX`, `IDENT_SEED`, `IDENT_INCR`, `IDENT_CURRENT`, `SCOPE_IDENTITY`, `DB_NAME`, `DB_ID`, `PARSENAME`, `QUOTENAME`) and system variables (`@@IDENTITY`, `@@ROWCOUNT`, `@@TRANCOUNT`, `@@ERROR`, `@@SPID`, `@@VERSION`, `@@SERVERNAME`, `@@FETCH_STATUS`, `@@NESTLEVEL`, `@@DATEFIRST`, `@@MICROSOFTVERSION`) | `crates/iridium_core/src/executor/scalar/builtin_registry.rs`, `crates/iridium_core/tests/new_system_functions.rs` |
| Logic and JSON functions | compatible subset | Support for logic functions (`ISNULL`, `COALESCE`, `IIF`, `NULLIF`, `CHOOSE`) and JSON functions (`JSON_VALUE`, `JSON_QUERY`, `JSON_MODIFY`, `ISJSON`, `JSON_ARRAY_LENGTH`, `JSON_KEYS`) | `crates/iridium_core/src/executor/scalar/builtin_registry.rs`, `crates/iridium_core/tests/new_logic_functions.rs`, `crates/iridium_core/tests/r7/r7_json_functions.rs` |
| Extended / Non-standard built-ins | exact | Specialized functions unique to Iridium SQL including RegEx (`REGEXP_LIKE`, `REGEXP_REPLACE`, `REGEXP_SUBSTR`, `REGEXP_INSTR`, `REGEXP_COUNT`) and Fuzzy Matching (`EDIT_DISTANCE`, `EDIT_DISTANCE_SIMILARITY`, `JARO_WINKLER_DISTANCE`, `JARO_WINKLER_SIMILARITY`) | `crates/iridium_core/src/executor/scalar/builtin_registry.rs`, `crates/iridium_core/tests/r7/r7_regexp_functions.rs`, `crates/iridium_core/tests/r7/r7_fuzzy_matching.rs` |
| Full collation and type-fidelity behavior | unsupported | Not declared or evidenced as SQL Server-complete | aggregate repo state |

## Metadata and Catalog

| Area | Status | Notes | Evidence |
|---|---|---|---|
| `INFORMATION_SCHEMA` | compatible subset | Extensive support exists; domain views (`DOMAINS`, `COLUMN_DOMAIN_USAGE`) return table types as domains; `DOMAIN_CONSTRAINTS` is intentionally empty (no domain constraints defined) | `crates/iridium_core/tests/information_schema.rs`, `crates/iridium_core/src/executor/metadata/info_schema_empty.rs` |
| `INFORMATION_SCHEMA.VIEW_TABLE_USAGE` | compatible subset | Backed by catalog view dependencies for supported views | `crates/iridium_core/src/executor/metadata/info_schema_views.rs`, `crates/iridium_core/tests/information_schema.rs` |
| `INFORMATION_SCHEMA.ROUTINE_COLUMNS` | compatible subset | Backed for inline table-valued functions with a single source table | `crates/iridium_core/src/executor/metadata/info_schema_routine_columns.rs`, `crates/iridium_core/tests/information_schema.rs` |
| `INFORMATION_SCHEMA.VIEW_COLUMN_USAGE` | compatible subset | Backed for supported views with direct column lineage across base tables, joins, grouping, predicates, and nested subqueries | `crates/iridium_core/src/executor/metadata/info_schema_views.rs`, `crates/iridium_core/tests/information_schema.rs`, `crates/iridium_server/tests/ssms_object_explorer_contract.rs` |
| `INFORMATION_SCHEMA` domain views (`COLUMN_DOMAIN_USAGE`, `DOMAINS`, `DOMAIN_CONSTRAINTS`) | shim | Still intentionally empty where the engine does not yet have domain metadata | `crates/iridium_core/src/executor/metadata/info_schema_empty.rs`, `crates/iridium_core/tests/information_schema.rs` |
| Core `sys.*` catalog views | compatible subset | Broad coverage exists including `sys.views`, `sys.database_principals`, `sys.database_permissions`, `sys.database_role_members`, `sys.index_columns` (with `key_ordinal`), `sys.procedures` (with `is_ms_shipped`), `sys.filegroups` (with PRIMARY row), `sys.all_objects`, `sys.identity_columns`, `sys.computed_columns`, `sys.sql_expression_dependencies` (empty stub); partition functions/schemes still return empty rows | `README.md`, `crates/iridium_core/src/executor/metadata/sys/*`, `crates/iridium_core/src/executor/metadata/sys/partition.rs` |
| SSMS Object Explorer bootstrap / table enumeration / property probes | compatible subset | Contract replay now covers bootstrap, tables, indexes, index columns, constraints (key, check, default), foreign keys, foreign key columns, routines (procedures, functions), routine parameters, routine definitions, triggers, views, schemas, view column usage, database/server property probes, partitions, filegroups, data spaces, stats, extended properties, database principals, database permissions, database role members, and table types | `crates/iridium_server/tests/ssms_object_explorer_contract.rs`, `crates/iridium_server/tests/fixtures/ssms_object_explorer_cases.json` |
| HADR / availability metadata | shim | Stub views exist and intentionally return empty results; documented as deliberate shim for single-node embedded engine (no Always On, no mirroring) | `crates/iridium_core/src/executor/metadata/sys/hadr.rs` |
| Partition functions / schemes / destination data spaces | compatible subset | View shapes match SQL Server but rows are empty; partition DDL is not yet supported, so these views correctly return no data | `crates/iridium_core/src/executor/metadata/sys/partition.rs` |
| `sys.sql_expression_dependencies` | shim | View shape matches SQL Server; rows intentionally empty until cross-object dependency tracking is implemented | `crates/iridium_core/src/executor/metadata/sys/tables/objects_misc.rs` |
| Full server-level metadata surface | unsupported | Not present as a complete implementation | aggregate repo state |

## TDS and Client Protocol

| Area | Status | Notes | Evidence |
|---|---|---|---|
| TDS login / prelogin / basic batch execution | compatible subset | Core flows exist and are tested | `crates/iridium_server/src/session/mod.rs`, `crates/iridium_server/tests/basic.rs` |
| TLS support | compatible subset | TLS is implemented, but parity depends on negotiation and client behavior details | `crates/iridium_server/src/tls.rs`, `crates/iridium_server/src/tds_tls_io.rs`, `crates/iridium_server/tests/security.rs` |
| RPC support | compatible subset | `sp_executesql`, `sp_prepexec`, and cursor RPCs (`sp_cursoropen`, `sp_cursorfetch`, `sp_cursorclose`) are supported; other RPC packets return explicit unsupported error | `crates/iridium_server/src/tds/rpc/parser.rs`, `crates/iridium_server/src/session/mod.rs`, `docs/compatibility-backlog.md` |
| Full SQL Server RPC surface | unsupported | Explicit unsupported RPC requests now produce an error response instead of being silently ignored | `crates/iridium_server/src/session/mod.rs` |
| SSMS / ADS connectivity | compatible subset | Supported in meaningful paths, but still dependent on compatibility-focused shims and partial protocol coverage | `README.md`, `crates/iridium_server/tests/ssms_object_explorer_contract.rs` |
| ADO.NET / ODBC / JDBC parity | unsupported | Not defined as complete today | aggregate repo state |

## Transactions, Locking, and Recovery

| Area | Status | Notes | Evidence |
|---|---|---|---|
| Multi-session transactions | compatible subset | There is meaningful concurrency and transaction coverage | `README.md`, `crates/iridium_core/tests/phase5_transactions.rs` |
| Locking and deadlocks | compatible subset | Locking and deadlock behavior are tested, but not yet declared SQL Server-equivalent | `crates/iridium_core/tests/concurrency_deadlock.rs`, `crates/iridium_core/tests/phase5_row_locking.rs` |
| Savepoints / nested transaction state / `XACT_STATE` | compatible subset | Targeted support exists | `crates/iridium_core/tests/nested_transactions_state.rs`, `crates/iridium_core/tests/tvp_and_xact_state.rs` |
| Checkpoint export / import recovery | compatible subset | Implemented and tested for embedded workflows | `README.md`, `crates/iridium_core/tests/phase5_locking_recovery.rs`, `crates/iridium_core/tests/phase8_persistence.rs` |
| WAL / page persistence / crash recovery | compatible subset | WAL replay integrated on startup; recovers rolled-back transactions | `crates/iridium_core/src/executor/database/persistence/mod.rs`, `crates/iridium_core/tests/wal_crash_recovery.rs` |

## Physical Engine

| Area | Status | Notes | Evidence |
|---|---|---|---|
| Logical index catalog | compatible subset | Index metadata exists and persists | `README.md`, `crates/iridium_core/tests/phase8_persistence.rs` |
| Planner index usage | compatible subset | BTreeIndex storage implemented; supports index seeks and range scans for equality, greater than, less than operations | `crates/iridium_core/src/storage/btree_index.rs`, `crates/iridium_core/src/executor/query/scan/executor.rs` |
| Cost-based optimization / statistics | unsupported | No evidence of SQL Server-class optimizer behavior | aggregate repo state |

## Security and Visibility

| Area | Status | Notes | Evidence |
|---|---|---|---|
| SQL authentication for server login | compatible subset | Basic username/password path exists in `iridium_server` | `crates/iridium_server/src/session/mod.rs`, `crates/iridium_server/src/lib.rs` |
| TLS-backed client login flow | compatible subset | Implemented for server connectivity | `crates/iridium_server/src/session/mod.rs`, `crates/iridium_server/tests/security.rs` |
| Principals, roles, grants, deny, revoke | unsupported | No full SQL Server security model is present | aggregate repo state |
| Metadata visibility by permission | unsupported | Not present as a full access-control model | aggregate repo state |
| Integrated / Windows authentication | unsupported | Not implemented in the current server | aggregate repo state |

## Admin and Server Features

| Area | Status | Notes | Evidence |
|---|---|---|---|
| Playground server for local tooling | shim | Useful for compatibility testing, not a SQL Server-equivalent admin surface | `README.md`, `crates/iridium_server/src/playground/*`, `scripts/start-playground-sa.ps1` |
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


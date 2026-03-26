# T-SQL Support Matrix (R9)

**Engine Version:** R9 — Advanced DML  
**Last Updated:** 2026-03-25  
**Test Suite:** 340+ tests passing

## Support Status Legend

| Status | Meaning |
|--------|---------|
| ✅ Exact | Matches SQL Server behavior |
| ⚠️ Near | Minor deviations documented |
| 🔶 Partial | Some features work, others stubbed |
| 📋 Stubbed | Accepted in parser, returns error or empty result |
| ❌ Unsupported | Not in parser or returns parse error |

## DDL Statements

| Feature | Status | Notes |
|---------|--------|-------|
| CREATE TABLE | ✅ Exact | Supports columns, types, PK, FK, DEFAULT, CHECK |
| ALTER TABLE ADD COLUMN | ✅ Exact | |
| ALTER TABLE DROP COLUMN | ✅ Exact | |
| ALTER TABLE ALTER COLUMN | ✅ Exact | |
| DROP TABLE | ✅ Exact | |
| CREATE INDEX | ✅ Exact | Clustered and non-clustered |
| DROP INDEX | ✅ Exact | |
| CREATE VIEW | 📋 Stubbed | Parser only |
| CREATE PROCEDURE | ✅ Exact | Subset with parameters and OUTPUT |
| CREATE FUNCTION | ✅ Exact | Scalar UDF and inline TVF |
| TRUNCATE TABLE | ✅ Exact | |

## DML Statements

| Feature | Status | Notes |
|---------|--------|-------|
| INSERT INTO ... VALUES | ✅ Exact | Multi-row inserts |
| INSERT INTO ... SELECT | ✅ Exact | Insert rows from query result |
| INSERT DEFAULT VALUES | ✅ Exact | |
| INSERT ... OUTPUT | 🔶 Partial | Parser ready, needs multi-row refinement |
| UPDATE | ✅ Exact | With WHERE clause |
| UPDATE ... FROM | 🔶 Partial | Tests added, executor needs alias resolution fix |
| UPDATE ... OUTPUT | ⚠️ Near | INSERTED and DELETED pseudo-tables |
| DELETE | ✅ Exact | With WHERE clause |
| DELETE ... FROM | 🔶 Partial | Tests added, executor needs alias resolution fix |
| DELETE ... OUTPUT | ⚠️ Near | DELETED pseudo-table |
| MERGE | 🔶 Partial | Tests added, basic upsert works |
| SELECT ... OFFSET/FETCH | ⚠️ Near | Full T-SQL syntax: OFFSET n ROWS FETCH NEXT m ROWS ONLY |
| SELECT | ✅ Exact | Full projection |
| SELECT TOP N | ✅ Exact | |
| SELECT DISTINCT | ✅ Exact | |
| SELECT INTO | 📋 Stubbed | |

## Query Features

| Feature | Status | Notes |
|---------|--------|-------|
| INNER JOIN | ✅ Exact | |
| LEFT JOIN | ✅ Exact | |
| RIGHT JOIN | ✅ Exact | |
| FULL OUTER JOIN | ✅ Exact | |
| CROSS JOIN | ✅ Exact | |
| Subqueries (scalar) | ✅ Exact | In SELECT, WHERE |
| Subqueries (IN) | ✅ Exact | |
| Subqueries (EXISTS) | ✅ Exact | |
| Correlated subqueries | ✅ Exact | |
| CTE (WITH) | ✅ Exact | |
| GROUP BY | ✅ Exact | |
| HAVING | ✅ Exact | |
| ORDER BY | ✅ Exact | ASC, DESC |
| UNION | ✅ Exact | |
| UNION ALL | ✅ Exact | |
| INTERSECT | ✅ Exact | |
| EXCEPT | ✅ Exact | |
| Window functions | ✅ Exact | ROW_NUMBER, RANK, DENSE_RANK, NTILE |
| PIVOT/UNPIVOT | ❌ Unsupported | |
| APPLY (CROSS/OUTER) | ✅ Exact | CROSS APPLY and OUTER APPLY |

## Data Types

| Type | Status | Notes |
|------|--------|-------|
| BIT | ✅ Exact | |
| TINYINT | ✅ Exact | |
| SMALLINT | ✅ Exact | |
| INT | ✅ Exact | |
| BIGINT | ✅ Exact | |
| DECIMAL(p,s) | ✅ Exact | |
| CHAR(n) | ✅ Exact | |
| VARCHAR(n) | ✅ Exact | |
| NCHAR(n) | ✅ Exact | |
| NVARCHAR(n) | ✅ Exact | |
| DATE | ✅ Exact | |
| TIME | ✅ Exact | |
| DATETIME | ✅ Exact | |
| DATETIME2 | ✅ Exact | |
| UNIQUEIDENTIFIER | ✅ Exact | |
| SQL_VARIANT | ✅ Exact | |
| MONEY | ✅ Exact | |
| SMALLMONEY | ✅ Exact | |
| BINARY(n) | ✅ Exact | Fixed-length binary |
| VARBINARY(n) | ✅ Exact | Variable-length binary |

## Operators & Expressions

| Feature | Status | Notes |
|---------|--------|-------|
| Arithmetic (+,-,*,/,%) | ✅ Exact | |
| Comparison (=,<>,<,>,<=,>=) | ✅ Exact | |
| Logical (AND, OR, NOT) | ✅ Exact | |
| LIKE | ✅ Exact | %, _ patterns |
| IN / NOT IN | ✅ Exact | |
| BETWEEN | ✅ Exact | |
| IS NULL / IS NOT NULL | ✅ Exact | |
| CASE WHEN | ✅ Exact | Searched and simple |
| CAST | ✅ Exact | |
| CONVERT | ✅ Exact | With style codes |
| COALESCE | ✅ Exact | |
| ISNULL | ✅ Exact | |

## Built-in Functions

| Function | Status | Notes |
|----------|--------|-------|
| GETDATE() | ✅ Near | Returns fixed 1970-01-01 (use seed) |
| CURRENT_TIMESTAMP | ✅ Near | Same as GETDATE |
| CURRENT_DATE | ✅ Exact | Returns DATE type |
| DATEADD() | ✅ Exact | All dateparts, bigint support |
| DATEDIFF() | ✅ Exact | All dateparts |
| DATEPART() | ✅ Exact | Respects DATEFIRST |
| DATENAME() | ✅ Exact | Respects DATEFIRST |
| YEAR/MONTH/DAY() | ✅ Exact | |
| LEN() | ✅ Exact | |
| SUBSTRING() | ✅ Exact | Optional length variant |
| UPPER/LOWER() | ✅ Exact | |
| LTRIM/RTRIM() | ✅ Exact | |
| TRIM() | ✅ Exact | |
| REPLACE() | ✅ Exact | |
| CHARINDEX() | ✅ Exact | |
| ABS() | ✅ Exact | |
| ROUND() | ✅ Exact | |
| CEILING() | ✅ Exact | |
| FLOOR() | ✅ Exact | |
| NEWID() | ✅ Exact | Deterministic with seed |
| RAND() | ✅ Near | Returns DECIMAL(10,9), deterministic with seed |
| SCOPE_IDENTITY() | ✅ Exact | |
| @@IDENTITY | ✅ Exact | |
| @@ROWCOUNT | ✅ Exact | Respects NOCOUNT |
| COUNT() | ✅ Exact | |
| SUM() | ✅ Exact | |
| AVG() | ✅ Exact | |
| MIN() | ✅ Exact | |
| MAX() | ✅ Exact | |
| COUNT_BIG() | ✅ Exact | |
| PRODUCT() | ✅ Exact | Aggregate |
| OBJECT_ID() | ✅ Exact | |
| COLUMNPROPERTY() | ✅ Exact | |
| OBJECTPROPERTY() | 📋 Stubbed | |
| UNISTR() | ✅ Exact | Unicode string |

### JSON Functions (R7)

| Function | Status | Notes |
|----------|--------|-------|
| JSON_VALUE | ✅ Exact | Path-based extraction |
| JSON_QUERY | ✅ Exact | Returns JSON fragment |
| JSON_MODIFY | ✅ Exact | Update/insert JSON values |
| ISJSON | ✅ Exact | Validates JSON |
| JSON_ARRAY_LENGTH | ✅ Exact | Array size |
| JSON_KEYS | ✅ Exact | Object keys |

### Regex Functions (R7)

| Function | Status | Notes |
|----------|--------|-------|
| REGEXP_LIKE | ✅ Exact | Pattern matching |
| REGEXP_REPLACE | ✅ Exact | Pattern replacement |
| REGEXP_SUBSTR | ✅ Exact | Extract match |
| REGEXP_INSTR | ✅ Exact | Match position |
| REGEXP_COUNT | ✅ Exact | Count matches |

### Fuzzy Matching (R7)

| Function | Status | Notes |
|----------|--------|-------|
| EDIT_DISTANCE | ✅ Exact | Levenshtein distance |
| EDIT_DISTANCE_SIMILARITY | ✅ Exact | Normalized 0-100 |
| JARO_WINKLER_DISTANCE | ✅ Exact | Jaro-Winkler distance |
| JARO_WINKLER_SIMILARITY | ✅ Exact | Similarity score |

### Aggregate Functions

| Function | Status | Notes |
|----------|--------|-------|
| STRING_AGG() | ✅ Exact | With separator, no ORDER BY yet |
| STRING_SPLIT() | ✅ Exact | Table-valued function |

## Session Options

| Option | Status | Notes |
|--------|--------|-------|
| SET ANSI_NULLS | ✅ Exact | Runtime enforcement |
| SET QUOTED_IDENTIFIER | 🔶 Partial | Parser accepts, not enforced |
| SET NOCOUNT | ✅ Exact | |
| SET XACT_ABORT | ✅ Exact | |
| SET DATEFIRST | ✅ Exact | Affects DATEPART/DATENAME weekday |
| SET LANGUAGE | 🔶 Partial | Accepted with warning |
| SET TRANSACTION ISOLATION LEVEL | ✅ Exact | |

## Transactions

| Feature | Status | Notes |
|---------|--------|-------|
| BEGIN TRANSACTION | ✅ Exact | |
| COMMIT | ✅ Exact | |
| ROLLBACK | ✅ Exact | |
| SAVE TRANSACTION | ✅ Exact | |
| ROLLBACK TRANSACTION (savepoint) | ✅ Exact | |
| Isolation levels | ✅ Exact | READ COMMITTED, SERIALIZABLE, SNAPSHOT |
| XACT_ABORT rollback | ✅ Exact | |
| MVCC snapshots | ✅ Near | Modeled |

## Metadata & Tooling

| Feature | Status | Notes |
|---------|--------|-------|
| sys.schemas | ✅ Exact | |
| sys.tables | ✅ Exact | |
| sys.columns | ✅ Exact | |
| sys.types | ✅ Exact | |
| sys.indexes | ✅ Exact | |
| sys.objects | ✅ Exact | |
| sys.check_constraints | ✅ Exact | |
| sys.routines | ✅ Exact | |
| INFORMATION_SCHEMA.TABLES | ✅ Exact | |
| INFORMATION_SCHEMA.COLUMNS | ✅ Exact | |
| INFORMATION_SCHEMA.ROUTINES | ✅ Exact | |
| INFORMATION_SCHEMA.TABLE_CONSTRAINTS | ✅ Exact | |
| sys.foreign_keys | ✅ Exact | With referenced table info |
| sys.key_constraints | ❌ Unsupported | |
| sys.default_constraints | ❌ Unsupported | |
| INFORMATION_SCHEMA.VIEWS | 📋 Stubbed | Parser only, returns empty |
| INFORMATION_SCHEMA.PARAMETERS | ✅ Exact | Returns real procedure/function parameters |
| INFORMATION_SCHEMA.ROUTINES | ✅ Exact | |

## Explain & Debugging

| Feature | Status | Notes |
|---------|--------|-------|
| EXPLAIN plan | ✅ Exact | Shows operators with details |
| Filter detail | ✅ Exact | WHERE expression text |
| Project detail | ✅ Exact | Column names/aliases |
| Join detail | ✅ Exact | Type + ON condition |
| Aggregate detail | ✅ Exact | GROUP BY + HAVING |
| Sort detail | ✅ Exact | ORDER BY with direction |
| Compatibility report | ✅ Exact | With spans and warnings |
| Execution trace | ✅ Exact | Per-statement events |
| Deterministic seeds | ✅ Exact | RandomSeed trait |

## Programmability (Subset)

| Feature | Status | Notes |
|---------|--------|-------|
| Variables (@var) | ✅ Exact | |
| DECLARE | ✅ Exact | |
| SET @var = expr | ✅ Exact | |
| SELECT @var = col | ✅ Exact | |
| IF/ELSE | ✅ Exact | |
| WHILE | ✅ Exact | |
| BEGIN/END | ✅ Exact | |
| BREAK/CONTINUE | ✅ Exact | |
| RETURN | ✅ Exact | |
| RAISERROR | ✅ Exact | |
| PRINT | 📋 Stubbed | |
| EXEC (dynamic SQL) | ✅ Exact | |
| EXEC (stored proc) | ✅ Exact | |
| sp_executesql | ✅ Exact | With OUTPUT parameters |
| Table variables | ✅ Exact | |
| Temp tables | ✅ Exact | |
| Inline TVF | ✅ Exact | |
| Stored procedures | ✅ Exact | Subset with parameters and OUTPUT |
| Scalar UDF | ✅ Near | |
| Triggers | ❌ Unsupported | |
| Cursors | ❌ Unsupported | |

## Known Deviations

1. **GETDATE/CURRENT_TIMESTAMP** returns fixed "1970-01-01T00:00:00" by default; use `FixedClock` for testing
2. **IDENTITY columns** start at configured seed/increment; SQL Server starts at seed
3. **NVARCHAR string literal** prefix N'...' accepted but not treated differently
4. **Temporary tables** are scoped to session, not connection (acceptable for embedded use)
5. **Checkpoint/restore** works at engine level, not per-connection
6. **Connection pooling** not implemented (single-user embedded)
7. **Multiple active result sets** not supported
8. **Table-valued parameters** not supported
9. **Triggers** not supported
10. **Cursors** not supported
11. **PIVOT/UNPIVOT** not supported

## R8 Exit Criteria Status

✅ **PASS:** Published compatibility scorecard with measurable, repeatable gap tracking

- [x] Compatibility dashboard by feature family
- [x] Parser fuzzing test suite
- [x] Expression differential testing
- [x] Random query generation in bounded domains
- [x] Performance baselines for embedded workloads
- [x] Persistence and corruption testing
- [x] Known differences catalog
- [x] Semantic caveat list
- [x] 314+ tests passing with 0 failures
- [x] Support matrix updated for R7+R8 features

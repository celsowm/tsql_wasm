# tsql-wasm

An embeddable, WASM-first T-SQL engine written in Rust — designed for test plans, local-first apps, and deterministic SQL execution without a server.

> Like [PGlite](https://github.com/electric-sql/pglite) for Postgres, but for T-SQL.

---

## What It Is

**tsql-wasm** is a from-scratch T-SQL engine that compiles to WebAssembly. It runs entirely in-memory, needs no server, and speaks enough T-SQL to execute real application scripts, migration batches, and procedural validation logic.

It targets **progressive SQL Server compatibility** — day-to-day T-SQL first, advanced features incrementally.

---

## Features

### DDL

```sql
CREATE TABLE dbo.Users (
  Id INT IDENTITY(1,1) PRIMARY KEY,
  Name NVARCHAR(100) NOT NULL,
  Score DECIMAL(10,2) DEFAULT 0,
  Email VARCHAR(200) UNIQUE,
  IsActive BIT NOT NULL DEFAULT 1
);

ALTER TABLE dbo.Users ADD Age INT NULL;
ALTER TABLE dbo.Users DROP COLUMN Age;
TRUNCATE TABLE dbo.Users;
DROP TABLE dbo.Users;

CREATE SCHEMA app;
DROP SCHEMA app;

CREATE INDEX app.IX_Users_Name ON app.Users (Name);
DROP INDEX app.IX_Users_Name ON app.Users;
```

### DML

```sql
INSERT INTO Users (Name, Score) VALUES ('Alice', 95.5), ('Bob', 87.0);
INSERT INTO Users DEFAULT VALUES;
UPDATE Users SET Score = Score * 1.1 WHERE Name = 'Alice';
DELETE FROM Users WHERE Score < 50;
SELECT * FROM Users;
```

### Queries

```sql
SELECT DISTINCT Name, UPPER(Name) AS NameUpper
FROM Users
WHERE Score BETWEEN 80 AND 100
  AND Name LIKE 'A%'
  AND IsActive IN (1, NULL)
ORDER BY Score DESC;

SELECT TOP 10 Name, Score FROM Users ORDER BY Score DESC;
SELECT TOP(5) * FROM Users;
```

### Joins

```sql
-- All four join types
SELECT u.Name, o.Total
FROM Users u
INNER JOIN Orders o ON u.Id = o.UserId;

SELECT u.Name, o.Total
FROM Users u
LEFT JOIN Orders o ON u.Id = o.UserId;

SELECT u.Name, o.Total
FROM Users u
RIGHT JOIN Orders o ON u.Id = o.UserId;

SELECT u.Name, o.Total
FROM Users u
FULL OUTER JOIN Orders o ON u.Id = o.UserId;
```

### Aggregates & Grouping

```sql
SELECT Category, COUNT(*) AS Cnt, SUM(Amount) AS Total, AVG(Amount) AS AvgAmt
FROM Orders
GROUP BY Category
HAVING COUNT(*) > 5
ORDER BY Total DESC;
```

### Set Operations

```sql
SELECT Name FROM ActiveUsers
UNION
SELECT Name FROM ArchivedUsers;

SELECT Id FROM TableA
INTERSECT
SELECT Id FROM TableB;

SELECT Id FROM TableA
EXCEPT
SELECT Id FROM TableB;
```

### CTEs

```sql
WITH TopUsers AS (
  SELECT Name, SUM(Amount) AS Total
  FROM Orders o JOIN Users u ON o.UserId = u.Id
  GROUP BY Name
  HAVING SUM(Amount) > 1000
)
SELECT * FROM TopUsers ORDER BY Total DESC;

-- Multiple CTEs
WITH A AS (SELECT Id FROM T1), B AS (SELECT Id FROM T2)
SELECT * FROM A INNER JOIN B ON A.Id = B.Id;
```

### Variables & Control Flow

```sql
DECLARE @threshold INT = 50;
DECLARE @count INT;

SET @count = (SELECT COUNT(*) FROM Users WHERE Score > @threshold);

IF @count > 10
BEGIN
    PRINT 'Many high scorers';
END
ELSE
BEGIN
    PRINT 'Few high scorers';
END
```

### While Loops

```sql
DECLARE @i INT = 1;
DECLARE @sum INT = 0;

WHILE @i <= 100
BEGIN
    SET @sum = @sum + @i;
    SET @i = @i + 1;
END

SELECT @sum; -- 5050
```

### Batch Execution

```sql
-- Semicolon-separated statements execute as a batch
DECLARE @x INT = 10;
SET @x = @x * 2 + 5;
SELECT @x AS Result; -- 25
```

### Dynamic SQL

```sql
EXEC 'SELECT * FROM Users WHERE Score > 80';
```

---

## Data Types

| Type | Description |
|------|-------------|
| `BIT` | Boolean (0/1) |
| `TINYINT` | 0–255 |
| `SMALLINT` | 16-bit integer |
| `INT` | 32-bit integer |
| `BIGINT` | 64-bit integer |
| `DECIMAL(p,s)` / `NUMERIC(p,s)` | Fixed-precision decimal |
| `CHAR(n)` | Fixed-length string |
| `VARCHAR(n)` | Variable-length string |
| `NCHAR(n)` | Fixed-length Unicode |
| `NVARCHAR(n)` | Variable-length Unicode |
| `DATE` | Date |
| `TIME` | Time |
| `DATETIME` | Date + time |
| `DATETIME2` | Extended date + time |
| `UNIQUEIDENTIFIER` | UUID/GUID |

---

## Built-in Functions

### String

| Function | Example |
|----------|---------|
| `UPPER(s)` | `UPPER('hello')` → `'HELLO'` |
| `LOWER(s)` | `LOWER('HELLO')` → `'hello'` |
| `LEN(s)` | `LEN('hello  ')` → `5` |
| `SUBSTRING(s, start, len)` | `SUBSTRING('hello', 2, 3)` → `'ell'` |
| `LTRIM(s)` / `RTRIM(s)` / `TRIM(s)` | Trim whitespace |
| `REPLACE(s, from, to)` | `REPLACE('abc', 'b', 'x')` → `'axc'` |
| `CHARINDEX(search, target [, start])` | `CHARINDEX('l', 'hello')` → `3` |

### Date/Time

| Function | Description |
|----------|-------------|
| `GETDATE()` / `CURRENT_TIMESTAMP` | Current datetime |
| `DATEADD(datepart, n, date)` | Add interval |
| `DATEDIFF(datepart, start, end)` | Difference between dates |
| `NEWID()` | Generate unique identifier |

Supported `datepart` values: `year`, `month`, `day`, `hour`, `minute`, `second` (and abbreviations).

### Math

| Function | Description |
|----------|-------------|
| `ABS(x)` | Absolute value |
| `ROUND(x [, precision])` | Round to precision |
| `CEILING(x)` | Round up |
| `FLOOR(x)` | Round down |

### Null Handling

| Function | Description |
|----------|-------------|
| `ISNULL(check, replacement)` | Replace NULL |
| `COALESCE(a, b, ...)` | First non-NULL |

### Type Conversion

| Function | Example |
|----------|---------|
| `CAST(expr AS type)` | `CAST(42 AS NVARCHAR)` → `'42'` |
| `CONVERT(type, expr)` | `CONVERT(NVARCHAR, 42)` → `'42'` |

---

## Constraints

| Constraint | Description |
|------------|-------------|
| `PRIMARY KEY` | Implies NOT NULL + UNIQUE |
| `NOT NULL` | Rejects NULL values |
| `UNIQUE` | Enforces uniqueness (NULLs allowed) |
| `DEFAULT expr` | Default value |
| `IDENTITY(seed, increment)` | Auto-increment |

---

## Three-Valued Logic

Proper SQL NULL semantics:

```
NULL AND TRUE   → NULL
NULL AND FALSE  → FALSE
NULL OR TRUE    → TRUE
NULL OR FALSE   → NULL
NOT NULL        → NULL
NULL + 1        → NULL
NULL = NULL     → NULL (not TRUE)
1 / 0           → NULL
```

---

## Architecture

```
┌──────────────┐     ┌─────────┐     ┌──────────┐     ┌──────────┐
│  WASM / TS   │────▶│ Parser  │────▶│  Binder  │────▶│ Executor │
│  Boundary    │     └─────────┘     └──────────┘     └──────────┘
└──────────────┘          │                              │
                          ▼                              ▼
                    ┌──────────┐                  ┌─────────────┐
                    │   AST    │                  │   Storage   │
                    └──────────┘                  └─────────────┘
```

**Modules:**
- `tsql_core` — the engine (parser, AST, executor, catalog, storage)
- `tsql_wasm` — WASM wrapper via `wasm-bindgen`

The engine uses a deterministic clock abstraction, making all time-dependent behavior testable.

---

## Build

### Prerequisites

```bash
rustup target add wasm32-unknown-unknown
cargo install wasm-pack
```

### Engine (Rust tests)

```bash
cargo test
```

### WASM build

```bash
wasm-pack build crates/tsql_wasm --target web --out-dir crates/tsql_wasm/pkg
```

### Client & Playground

```bash
cd packages/client && npm install && npm run build
cd ../playground && npm install && npm run dev
```

---

## Usage (TypeScript)

```ts
const db = await TsqlDatabase.create();

await db.exec(`
  CREATE TABLE dbo.Users (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    Name NVARCHAR(100) NOT NULL,
    IsActive BIT NOT NULL DEFAULT 1
  )
`);

await db.exec(`
  INSERT INTO dbo.Users (Name, IsActive)
  VALUES (N'Alice', 1), (N'Bob', 0), (N'Charlie', 1)
`);

const result = await db.query(`
  SELECT TOP 2 Name,
         CASE WHEN IsActive = 1 THEN 'Active' ELSE 'Inactive' END AS Status
  FROM dbo.Users
  WHERE Name LIKE 'A%'
  ORDER BY Name ASC
`);

// result.columns = ["Name", "Status"]
// result.rows    = [["Alice", "Active"]]

const checkpoint = await db.exportCheckpoint();
const restored = await TsqlDatabase.fromCheckpoint(checkpoint);
const restoredRows = await restored.query(`SELECT COUNT(*) FROM dbo.Users`);
```

---

## Test Coverage

Core and integration tests covering:

- DDL (CREATE/DROP/ALTER TABLE, schemas, constraints)
- Metadata (`sys.*`, `INFORMATION_SCHEMA`)
- Index catalog operations (`CREATE INDEX` / `DROP INDEX`)
- DML (INSERT/UPDATE/DELETE with all expression types)
- Joins (INNER, LEFT, RIGHT, FULL OUTER)
- Aggregates (COUNT, SUM, AVG, MIN, MAX with GROUP BY/HAVING)
- Set operations (UNION, UNION ALL, INTERSECT, EXCEPT)
- CTEs (single and multiple)
- Variables, IF/ELSE, WHILE loops, BREAK/CONTINUE/RETURN
- `SELECT @var = ...` assignments
- Temporary tables (`#temp`) and table variables (`DECLARE @t TABLE (...)`)
- Stored procedures (subset), scalar UDF (subset), inline TVF (subset)
- `EXEC` and `sp_executesql` subset with OUTPUT parameters
- Identity scope functions (`SCOPE_IDENTITY()`, `@@IDENTITY`, `IDENT_CURRENT`)
- Multi-session transactions and isolation anomaly simulations
- Checkpoint export/import recovery surface (`tsql_core`, WASM, TS client)
- MVCC-style deterministic commit conflict matrix scenarios
- All built-in functions
- Type coercion and NULL semantics
- Arithmetic, CASE, IN, BETWEEN, LIKE expressions

---

## Current Limitations

- No on-disk WAL/page persistence (current recovery model is checkpoint export/import for embedded runtimes)
- Indexes are catalog-only in this phase (planner still uses table scans)
- Transaction fidelity is modeled and still partial vs SQL Server edge cases (see roadmap matrix)
- Catalog coverage is still a subset of SQL Server metadata

See [`docs/roadmap.md`](docs/roadmap.md) for the full compatibility roadmap.

---

## License

MIT

# Frequently Asked Questions (FAQ)

## Table of Contents
1. [General](#general)
2. [Why Rust?](#why-rust)
3. [Compatibility & T-SQL](#compatibility--t-sql)
4. [Implementation Details](#implementation-details)
5. [Persistence & Storage](#persistence--storage)
6. [Transactions & Concurrency](#transactions--concurrency)
7. [WASM & Embedding](#wasm--embedding)
8. [Tools & Connectivity](#tools--connectivity)
9. [Performance](#performance)
10. [Legal & Licensing](#legal--licensing)
11. [Roadmap & Contributing](#roadmap--contributing)

---

## General

### 1. What is Iridium SQL?
Iridium SQL is an independent, open-source SQL Server-compatible database engine written in [Rust](https://www.rust-lang.org/). It provides a T-SQL engine with a native [TDS (Tabular Data Stream)](https://learn.microsoft.com/en-us/sql/relational-databases/native-client-ole-db-tabular-data-stream/tabular-data-stream-protocol) server, allowing it to work with existing SQL Server tools and drivers while offering a lightweight, embeddable, and [WASM](https://webassembly.org/)-compatible alternative.

### 2. Why was Iridium SQL created?
The project aims to provide an open alternative to SQL Server-style workflows. It is designed for application-facing compatibility, local-first persistence, and predictable behavior across native, server, and WASM runtimes.

### 3. How does Iridium SQL differ from SQLite or PostgreSQL?
While [SQLite](https://www.sqlite.org/) and [PostgreSQL](https://www.postgresql.org/) are excellent databases, they use different SQL dialects (Standard SQL / PostgreSQL dialect). Iridium SQL is specifically built for users and applications that depend on **T-SQL** (Transact-SQL) and the **TDS protocol**, providing a drop-in experience for those already using SQL Server tools and libraries.

### 4. Can I use Iridium SQL for production workloads?
Iridium SQL is currently in a phase-based development cycle. While it is highly compatible with many application-facing T-SQL patterns, it is recommended to evaluate its current [Compatibility Matrix](compatibility-matrix.md) and [Roadmap](roadmap.md) to ensure it meets your specific requirements.

### 5. What are the primary use cases?
- **Local-first applications** that want to use T-SQL without a heavy SQL Server installation.
- **Unit and Integration Testing** for T-SQL code without needing a containerized SQL Server.
- **Edge computing and WASM** where you need a real database engine in the browser or at the edge.
- **Migration scenarios** where you need a lightweight shim for SQL Server-dependent apps.

### 6. Is it a fork of any existing project?
No. Iridium SQL is built from the ground up in Rust. It does not use code from SQL Server, Postgres, or any other existing engine.

### 7. What is the "Iridium" name about?
Iridium is a dense, corrosion-resistant metal. The name reflects the project's goal of being a solid, reliable, and "hard" implementation of a complex specification.

---

## Why Rust?

### 8. Why was Rust chosen for this project?
Rust provides the perfect balance of **memory safety**, **performance**, and **modern ergonomics**. For a database engine, preventing memory-related bugs (like buffer overflows or null pointer dereferences) at compile-time is a massive advantage for stability and security.
- Read more: [Rust Language Philosophy](https://doc.rust-lang.org/book/ch00-00-introduction.html)

### 9. Does Rust's borrow checker make database implementation harder?
It makes it more *disciplined*. Managing complex state like a buffer pool, transaction logs, and concurrent lock tables requires careful [ownership](https://doc.rust-lang.org/book/ch04-00-understanding-ownership.html) design. Rust forces you to handle these patterns safely, which pays off in fewer production crashes.

### 10. How does Rust benefit the WASM implementation?
Rust has some of the best toolchains for WebAssembly (`wasm-pack`, `wasm-bindgen`). This allows us to compile the *exact same* engine code used in the native server for use in the browser, ensuring behavioral parity.
- Read more: [Rust and WebAssembly](https://rustwasm.github.io/docs/book/)

### 11. Are you using `unsafe` Rust?
We strive to minimize `unsafe` usage. It is occasionally used in low-level storage optimizations or FFI boundaries (like TLS), but the core logic of the parser, binder, and executor is 100% safe Rust.

---

## Compatibility & T-SQL

### 12. How compatible is Iridium SQL with Microsoft SQL Server?
Iridium SQL targets a "useful SQL Server-compatible subset." It is not a 1:1 clone of every internal SQL Server feature, but it aims for user-visible parity in T-SQL syntax, result sets, metadata (sys.* views), and TDS protocol behavior.

### 13. Which version of SQL Server is the target?
The compatibility target is generally aligned with modern SQL Server versions (2016 and later), focusing on features like window functions, CTEs, and JSON support.

### 14. Is T-SQL the only supported language?
Yes, Iridium SQL is specifically designed as a T-SQL engine. It implements the [T-SQL dialect](https://learn.microsoft.com/en-us/sql/t-sql/language-reference), including its unique procedural extensions (IF/ELSE, WHILE, TRY/CATCH) and system functions.

### 15. Does it support modern T-SQL features like PIVOT or MERGE?
Yes. Iridium SQL includes support for complex T-SQL statements including `PIVOT`, `UNPIVOT`, `MERGE` (including `WHEN NOT MATCHED BY SOURCE`), `STRING_AGG`, and recursive CTEs.

### 16. Does it support window functions?
Yes, it supports a broad range of window functions including `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE`, `NTILE`, and windowed aggregates like `SUM(x) OVER(...)`.

### 17. Are common system functions like `GETDATE()` or `SCOPE_IDENTITY()` supported?
Yes. We implement a large number of system functions (math, string, date, metadata) to ensure that existing scripts run without modification.

### 18. Does it support Table-Valued Parameters (TVPs)?
Yes, [TVPs](https://learn.microsoft.com/en-us/sql/relational-databases/native-client/features/table-valued-parameters-sql-server-native-client) are supported both in T-SQL batches and via the TDS protocol, including multi-column TVPs and `READONLY` enforcement.

### 19. How are errors handled? Does it match SQL Server error codes?
Iridium SQL aims for high fidelity in error reporting. When a query fails, the engine returns TDS error tokens that include SQL Server-compatible error numbers (e.g., 2627 for primary key violations, 1205 for deadlocks), states, and message patterns.

### 20. Does it support temporary tables?
Yes, both local (`#temp`) and global (`##global_temp`) temporary tables are supported with proper session scoping and automatic cleanup.

### 21. What about Common Table Expressions (CTEs)?
Both simple and [recursive CTEs](https://learn.microsoft.com/en-us/sql/t-sql/queries/with-common-table-expression-transact-sql) are supported. The engine handles the recursive anchor and recursive members according to T-SQL semantics.

---

## Implementation Details

### 22. How is the parser implemented?
The parser is a custom-built [recursive descent parser](https://en.wikipedia.org/wiki/Recursive_descent_parser) located in `crates/iridium_core/src/parser/`. It handles T-SQL lexing, statement parsing, and expression evaluation. It is designed to be highly compatible with T-SQL's specific grammar quirks, such as bracketed identifiers and semicolon-less batches.

### 23. How are tables represented in Rust?
Internally, tables are managed by the `Catalog`. Data is stored in rows (represented as `Vec<Value>`) within the `Storage` layer. The executor uses an [AST (Abstract Syntax Tree)](https://en.wikipedia.org/wiki/Abstract_syntax_tree) to navigate and manipulate this data during query execution.

### 24. What is the execution engine architecture?
The engine follows a phase-based execution model:
1. **Parsing:** SQL string to AST.
2. **Lowering/Binding:** Parser AST to Executor AST, resolving names against the catalog and checking types.
3. **Execution:** The `StatementExecutor` dispatches to specific executors (Query, Mutation, Script) to process the data.

### 25. How are system views like `sys.objects` implemented?
System views are implemented as "metadata shims" in the engine. When a user queries `sys.objects` or `INFORMATION_SCHEMA.TABLES`, the engine intercepts these requests and dynamically generates the result sets from the internal `Catalog` state.

### 26. What data types are supported?
Iridium SQL supports a wide range of T-SQL data types, including `INT`, `BIGINT`, `DECIMAL`, `NVARCHAR`, `VARCHAR`, `VARBINARY`, `BIT`, `DATETIME`, `DATETIME2`, `UNIQUEIDENTIFIER`, and `TABLE`.

### 27. How does the engine handle type coercion?
The engine implements T-SQL's [implicit conversion](https://learn.microsoft.com/en-us/sql/t-sql/data-types/data-type-conversion-database-engine) rules. For example, it can automatically coerce `DATE` to `DATETIME2` or `BINARY(16)` to `UNIQUEIDENTIFIER`.

### 28. Is there a query optimizer?
The engine currently uses a rule-based approach for most queries. However, it does support index seeks and range scans when appropriate indexes are available. A more advanced [cost-based optimizer](https://en.wikipedia.org/wiki/Query_optimization) is on the long-term roadmap.

---

## Persistence & Storage

### 29. Where and how is the database persisted?
In native/server mode, Iridium SQL defaults to durable storage on disk.
- **Default Path (Windows):** `%ProgramData%\Iridium SQL\iridium_sql_data`
- **Mechanism:** It uses [redb](https://github.com/crawshaw/redb), a high-performance, embedded, [ACID](https://en.wikipedia.org/wiki/ACID)-compliant key-value store written in Rust, as its primary storage backend. Tables and indexes are mapped to redb tables.

### 30. Can I run it entirely in memory?
Yes. You can use the `--memory` flag to run in ephemeral mode, which is ideal for testing or scenarios where data does not need to persist across restarts.

### 31. Does it support Write-Ahead Logging (WAL)?
The engine includes a [WAL (Write-Ahead Logging)](https://en.wikipedia.org/wiki/Write-ahead_logging) implementation for crash recovery. It records transaction changes before they are applied to the main data files, ensuring durability even after a power failure.

### 32. How are indexes stored?
Indexes are implemented using [B-Trees](https://en.wikipedia.org/wiki/B-tree) (provided by `redb` or our own BTree implementation depending on the configuration). They store key-to-RID (Row ID) mappings to allow for fast lookups.

### 33. Can I change the storage location?
Yes, you can use the `--data-dir <PATH>` flag when starting the server to specify a custom directory for data files.

---

## Transactions & Concurrency

### 34. Does it support transactions?
Yes. Iridium SQL supports ACID transactions with `BEGIN TRANSACTION`, `COMMIT`, and `ROLLBACK`.

### 35. What isolation levels are supported?
We support `READ COMMITTED`, `READ UNCOMMITTED`, and `SNAPSHOT` isolation. The implementation uses a mix of row-level locks and [MVCC (Multi-Version Concurrency Control)](https://en.wikipedia.org/wiki/Multiversion_concurrency_control) depending on the requested level.

### 36. How does locking work?
The engine uses a fine-grained locking system. It can acquire Shared (S), Exclusive (X), and Intent locks at the row and table level.
- Read more: [SQL Server Locking Architecture](https://learn.microsoft.com/en-us/sql/relational-databases/sql-server-transaction-locking-and-row-versioning-guide)

### 37. Does it handle deadlocks?
Yes. The engine includes a [deadlock detector](https://en.wikipedia.org/wiki/Deadlock) that builds a "wait-for" graph. If a cycle is detected, one of the sessions is chosen as a "victim," its transaction is rolled back, and a 1205 error is returned.

### 38. What is `XACT_ABORT`?
Like SQL Server, Iridium SQL supports `SET XACT_ABORT ON/OFF`. When `ON`, any run-time error causes the current transaction to be automatically rolled back.

---

## WASM & Embedding

### 39. How do I use Iridium SQL in a web application?
You can use the `@celsowm/iridium-sql-client` TypeScript package. It provides a high-level API to create a database instance, execute queries, and manage checkpoints within the browser using WebAssembly.

### 40. Is the entire engine running in the browser?
Yes. The complete `iridium_core` engine, including the parser and executor, is compiled to WASM. There is no backend server required for basic query execution in the browser.

### 41. How does persistence work in WASM?
In the browser, persistence is "memory-first" by default. However, Iridium SQL provides `exportCheckpoint()` and `fromCheckpoint()` methods. You can save the exported byte array to [IndexedDB](https://developer.mozilla.org/en-US/docs/Web/API/IndexedDB_API) or `localStorage` and restore it when the user returns.

### 42. What are the limitations of the WASM version?
The WASM version is single-threaded (due to standard browser constraints) and does not support the native TDS network server. It communicates directly via a JS/Rust bridge.

---

## Tools & Connectivity

### 43. Does it support SQL Server Management Studio (SSMS)?
Yes. Iridium SQL implements the "SSMS Object Explorer contract," allowing SSMS and Azure Data Studio to connect, browse objects, and run queries.

### 44. Which drivers are supported?
Iridium SQL aims for compatibility with standard TDS drivers, including:
- [sqlcmd](https://learn.microsoft.com/en-us/sql/tools/sqlcmd-utility)
- [tedious](https://tediousjs.github.io/tedious/) (Node.js)
- [tiberius](https://github.com/stevee/tiberius) (Rust)
- [ADO.NET](https://learn.microsoft.com/en-us/dotnet/framework/data/adonet/sql-server-data-operations) (C#)
- Python (`pyodbc`, `pymssql`)

### 45. Does it support TLS/SSL?
Yes, the server supports [TLS (Transport Layer Security)](https://en.wikipedia.org/wiki/Transport_Layer_Security) for secure connections. You can provide your own certificate or use a self-signed one for development.

### 46. Is there a playground?
Yes! The repository includes a browser-based T-SQL playground in `packages/iridium-playground` where you can try out queries instantly.

---

## Performance

### 47. How fast is Iridium SQL?
In many scenarios, Iridium SQL can be faster than a full SQL Server instance for small-to-medium datasets because it has much lower overhead and is optimized for embedded use. However, for massive multi-terabyte datasets, the lack of a mature cost-based optimizer and distributed features may be a factor.

### 48. Does it support parallel query execution?
Not yet. Current query execution is single-threaded per session to ensure simplicity and correctness, though different sessions run on different threads.

### 49. How does `redb` performance compare?
`redb` is a very fast, modern storage engine. It provides excellent read performance and solid write performance with strong durability guarantees.

---

## Legal & Licensing

### 50. Is Iridium SQL associated with Microsoft?
No. Iridium SQL is an independent implementation and does not use any Microsoft proprietary code. "Microsoft" and "SQL Server" are trademarks of Microsoft Corporation, used here for compatibility description only.

### 51. What is the license?
Iridium SQL is released under the **[MIT License](https://opensource.org/licenses/MIT)**, making it free for both open-source and commercial use.

### 52. Do I need a license key?
No. Iridium SQL is 100% free and open-source. There are no "Enterprise" editions or hidden costs.

---

## Roadmap & Contributing

### 53. What's next on the roadmap?
We are currently focusing on:
- Improving full-text search compatibility.
- Expanding `sys.*` view coverage for better tool support.
- Enhancing the query optimizer.
- Adding more SQL Server-specific data types (like `GEOMETRY`).

### 54. Can I contribute?
Yes! We love contributions. Whether it's fixing a bug, adding a new T-SQL function, or improving documentation, please check our `CONTRIBUTING.md` (or `AGENTS.md`) and open a Pull Request.

### 55. How do I report a bug?
Please open an issue on our [GitHub repository](https://github.com/celsowm/iridium-sql) with a reproducible T-SQL script and a description of the expected vs. actual behavior.

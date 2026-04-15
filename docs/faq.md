# Frequently Asked Questions (FAQ)

## General

### What is Iridium SQL?
Iridium SQL is an independent, open-source SQL Server-compatible database engine written in Rust. It provides a T-SQL engine with a native TDS (Tabular Data Stream) server, allowing it to work with existing SQL Server tools and drivers while offering a lightweight, embeddable, and WASM-compatible alternative.

### Why was Iridium SQL created?
The project aims to provide an open alternative to SQL Server-style workflows. It is designed for application-facing compatibility, local-first persistence, and predictable behavior across native, server, and WASM runtimes.

### Why is it written in Rust?
Rust was chosen for several reasons:
- **Memory Safety:** Rust's ownership model prevents common memory bugs like null pointer dereferences and buffer overflows without needing a garbage collector.
- **Performance:** Rust provides low-level control and performance comparable to C/C++, which is critical for a database engine.
- **Concurrency:** Rust's "fearless concurrency" makes it easier to implement complex multi-threaded features like row-level locking and MVCC safely.
- **WASM Support:** Rust has excellent first-class support for compiling to WebAssembly, enabling Iridium SQL to run in the browser.

### How does Iridium SQL differ from SQLite or PostgreSQL?
While SQLite and PostgreSQL are excellent databases, they use different SQL dialects (Standard SQL / PostgreSQL dialect). Iridium SQL is specifically built for users and applications that depend on **T-SQL** (Transact-SQL) and the **TDS protocol**, providing a drop-in experience for those already using SQL Server tools and libraries.

### Can I use Iridium SQL for production workloads?
Iridium SQL is currently in a phase-based development cycle. While it is highly compatible with many application-facing T-SQL patterns, it is recommended to evaluate its current [Compatibility Matrix](compatibility-matrix.md) and [Roadmap](roadmap.md) to ensure it meets your specific requirements.

## Compatibility

### How compatible is Iridium SQL with Microsoft SQL Server?
Iridium SQL targets a "useful SQL Server-compatible subset." It is not a 1:1 clone of every internal SQL Server feature, but it aims for user-visible parity in T-SQL syntax, result sets, metadata (sys.* views), and TDS protocol behavior.

### Does it support SQL Server Management Studio (SSMS)?
Yes. Iridium SQL implements the "SSMS Object Explorer contract," allowing tools like SSMS and Azure Data Studio to connect, browse tables, views, and stored procedures, and execute queries.

### Which version of SQL Server is the target?
The compatibility target is generally aligned with modern SQL Server versions (2016 and later), focusing on features like window functions, CTEs, and JSON support.

### Is T-SQL the only supported language?
Yes, Iridium SQL is specifically designed as a T-SQL engine. It implements the T-SQL dialect, including its unique procedural extensions (IF/ELSE, WHILE, TRY/CATCH) and system functions.

### Does it support modern T-SQL features like PIVOT or MERGE?
Yes. Iridium SQL includes support for complex T-SQL statements including `PIVOT`, `UNPIVOT`, `MERGE` (including `WHEN NOT MATCHED BY SOURCE`), `STRING_AGG`, and recursive CTEs.

### How are errors handled? Does it match SQL Server error codes?
Iridium SQL aims for high fidelity in error reporting. When a query fails, the engine returns TDS error tokens that include SQL Server-compatible error numbers, states, and message patterns to ensure client drivers handle exceptions correctly.

## Implementation Details

### How is the parser implemented?
The parser is a custom-built recursive descent parser located in `crates/iridium_core/src/parser/`. It handles T-SQL lexing, statement parsing, and expression evaluation. It is designed to be highly compatible with T-SQL's specific grammar quirks, such as bracketed identifiers and semicolon-less batches.

### How are tables represented in Rust?
Internally, tables are managed by the `Catalog`. Data is stored in rows (represented as `Vec<Value>`) within the `Storage` layer. The executor uses an AST (Abstract Syntax Tree) to navigate and manipulate this data during query execution.

### What is the execution engine architecture?
The engine follows a phase-based execution model:
1. **Parsing:** SQL string to AST.
2. **Lowering/Binding:** Parser AST to Executor AST, resolving names against the catalog and checking types.
3. **Execution:** The `StatementExecutor` dispatches to specific executors (Query, Mutation, Script) to process the data.

### Does it support transactions and locking?
Yes. Iridium SQL supports ACID transactions with row-level locking, MVCC (Multi-Version Concurrency Control), savepoints, and deadlock detection. It also supports various isolation levels (READ COMMITTED, SNAPSHOT, etc.) as they are implemented in the roadmap.

### How are system views like `sys.objects` implemented?
System views are implemented as "metadata shims" in the engine. When a user queries `sys.objects` or `INFORMATION_SCHEMA.TABLES`, the engine intercepts these requests and dynamically generates the result sets from the internal `Catalog` state.

### What data types are supported?
Iridium SQL supports a wide range of T-SQL data types, including `INT`, `BIGINT`, `DECIMAL`, `NVARCHAR`, `VARCHAR`, `VARBINARY`, `BIT`, `DATETIME`, `DATETIME2`, `UNIQUEIDENTIFIER`, and `TABLE` (for Table-Valued Parameters).

## Persistence

### Where and how is the database persisted?
In native/server mode, Iridium SQL defaults to durable storage on disk.
- **Default Path (Windows):** `%ProgramData%\Iridium SQL\iridium_sql_data`
- **Mechanism:** It uses `redb`, a high-performance, embedded, ACID-compliant key-value store written in Rust, as its primary storage backend. Tables and indexes are mapped to redb tables.

### Can I run it entirely in memory?
Yes. You can use the `--memory` flag to run in ephemeral mode, which is ideal for testing or scenarios where data does not need to persist across restarts.

### How does persistence work in WASM?
In the browser, persistence is "memory-first." However, Iridium SQL provides explicit checkpoint/export/import flows. You can export a database checkpoint to a byte array and restore it later, allowing for persistence via IndexedDB or local file storage.

### Does it support Write-Ahead Logging (WAL)?
The engine's persistence model depends on the underlying storage implementation. With `redb`, ACID guarantees are provided through a copy-on-write (CoW) mechanism, ensuring data integrity even after unexpected shutdowns.

## WASM & Embedding

### How do I use Iridium SQL in a web application?
You can use the `@celsowm/iridium-sql-client` TypeScript package. It provides a high-level API to create a database instance, execute queries, and manage checkpoints within the browser using WebAssembly.

### Is the entire engine running in the browser?
Yes. The complete `iridium_core` engine, including the parser and executor, is compiled to WASM. There is no backend server required for basic query execution in the browser.

### What are the limitations of the WASM version?
The WASM version is single-threaded by nature (due to browser constraints) and relies on manual checkpointing for persistence. It also does not support the native TDS network server, as it communicates directly via the TypeScript/JavaScript bridge.

## Tools and Connectivity

### Which drivers are supported?
Iridium SQL aims for compatibility with standard TDS drivers, including `sqlcmd`, `tedious` (Node.js), `tiberius` (Rust), and ADO.NET (C#).

### Can I use Azure Data Studio?
Yes. Azure Data Studio can connect to the Iridium SQL server using the standard SQL Server connection provider.

### How do I run the playground?
The repository includes a `packages/iridium-playground` which is a browser-based T-SQL playground. It uses the WASM package to provide an interactive SQL experience without a backend.

## Legal & Licensing

### Is Iridium SQL associated with Microsoft?
No. Iridium SQL is an independent implementation and does not use any Microsoft proprietary code. "Microsoft" and "SQL Server" are trademarks of Microsoft Corporation, used here for compatibility description only.

### What is the license?
Iridium SQL is released under the **MIT License**, making it free for both open-source and commercial use.

### Can I contribute to Iridium SQL?
Yes! Contributions are welcome. Please see the `AGENTS.md` and `docs/code-review.md` for guidelines on how to contribute code, report bugs, or suggest features.

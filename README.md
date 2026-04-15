# Iridium SQL

An open SQL Server-compatible database engine with native persistence by default and WASM support for embedding.

Iridium SQL is an independent Rust implementation built for application-facing compatibility, local-first persistence, and predictable behavior across native, server, and WASM runtimes.

## What It Is

- A T-SQL engine with a native TDS server.
- A persistent database engine by default in native/server mode.
- A WASM runtime for embedding, browser work, and lightweight local use.
- A compatibility-driven project with behavior tracked in docs, not implied by the name.

## Why It Exists

The goal is to provide an open alternative to SQL Server-style workflows without tying the project to a single runtime shape.

That means:

- native/server usage defaults to durable storage on disk
- `--memory` stays available for ephemeral and test-only scenarios
- WASM stays memory-first, with explicit checkpoint/export/import flows when persistence is needed

## Quick Start

Run the server with persistent storage:

```bash
cargo run --package iridium_server --bin iridium-server
```

Run in ephemeral mode:

```bash
cargo run --package iridium_server --bin iridium-server -- --memory
```

On Windows, persistent data defaults to `%ProgramData%\Iridium SQL\iridium_sql_data`.
Use `--data-dir <PATH>` to override it, or the portable ZIP's
`start-iridium-server-portable.cmd` to keep data next to the extracted bundle.

Build the WASM package:

```bash
wasm-pack build crates/iridium_wasm --target web --out-dir crates/iridium_wasm/pkg
```

## Minimal Example

```ts
import { IridiumDatabase } from "@celsowm/iridium-sql-client";

const db = await IridiumDatabase.create();

await db.exec(`
  CREATE TABLE dbo.Users (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    Name NVARCHAR(100) NOT NULL
  )
`);

const result = await db.query(`SELECT TOP 1 Name FROM dbo.Users`);
```

For checkpoint-based persistence in WASM or client flows:

```ts
const checkpoint = await db.exportCheckpoint();
const restored = await IridiumDatabase.fromCheckpoint(checkpoint);
```

## Compatibility

Compatibility is measured, documented, and updated continuously.

- [Frequently Asked Questions (FAQ)](docs/faq.md)
- [Compatibility Roadmap](docs/roadmap.md)
- [Compatibility Matrix](docs/compatibility-matrix.md)
- [Compatibility Backlog](docs/compatibility-backlog.md)

Current posture:

- SQL Server compatibility is the target, not a claim of total parity.
- Native/server persistence is the default.
- WASM support is first-class, but intentionally memory-first.

## Project Surface

- `crates/iridium_core` - parser, binder, executor, storage, and compatibility logic
- `crates/iridium_server` - TDS server and playground runtime
- `crates/iridium_wasm` - WASM bindings
- `packages/iridium-client` - TypeScript client API
- `packages/iridium-playground` - browser playground

## Disclaimer

Iridium SQL is an independent implementation and does not use Microsoft proprietary SQL Server code.

Microsoft and SQL Server are trademarks of Microsoft Corporation. Any mention of SQL Server in this repository is for compatibility and interoperability description only.

## License

MIT

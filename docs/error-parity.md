# Error Parity and Custom Errors

## Purpose

This document records the current state of error handling in Iridium SQL and how it compares to SQL Server.

The short version:

- custom errors are supported
- some common SQL Server error numbers are matched
- total SQL Server error parity is not implemented

## Custom Errors

Iridium SQL supports explicit custom errors through [`DbError::Custom`](../crates/iridium_core/src/error.rs).

That variant carries:

- `class`
- `number`
- `message`

This is what backs supported `THROW` and `RAISERROR` flows when the engine raises a user-defined error.

## Current Parity Matrix

| Area | Current behavior | Parity level | Notes |
|---|---|---|---|
| Parse errors | Mapped to SQL Server-style syntax error codes | Partial | `DbError::Parse` currently maps to `102`, but not all parser wording and states match SQL Server exactly. |
| Missing object / schema / trigger / view | Mapped to `208` | Good | Common "invalid object name" cases are covered. |
| Missing column | Mapped to `207` | Good | Both qualified and unqualified column misses use the SQL Server-style code. |
| Database missing | Mapped to `911` | Good | Useful for common connection / catalog checks. |
| Duplicate table / trigger / view / type / schema | Mapped to `2714` | Good | SQL Server-style duplicate object handling. |
| Duplicate column | Mapped to `2705` | Good | Common table definition error. |
| Deadlock victim | Mapped to `1205` | Good | Matches the SQL Server deadlock victim code. |
| Divide by zero | Mapped to `8134` | Good | Common arithmetic error parity. |
| Conversion failure | Mapped to `245` | Good | Covers many coercion failures, but not every conversion edge case. |
| Cursor not declared | Mapped to `16916` | Good | Cursor-specific error parity exists for the supported path. |
| Generic semantic errors | Collapsed to `50000` | Partial | Many distinct SQL Server semantic errors are not modeled individually. |
| Generic execution errors | Collapsed to `50000` | Partial | Broad execution failures share one bucket. |
| Generic storage errors | Collapsed to `50001` | Partial | Useful for the engine, but not SQL Server-complete. |
| `THROW` | Implemented and tested | Partial | Works for the supported subset, not a full SQL Server error system. |
| `RAISERROR` | Implemented and tested | Partial | Severity and message handling exist, but not the full formatting surface. |

## What Is Still Missing

- a complete SQL Server error catalog
- exact mapping for every runtime, semantic, and metadata failure
- full message text parity for each error number
- exact severity and state behavior across all error sources
- broader parity for `RAISERROR` formatting and edge cases

## Practical Guidance

When adding new failures, choose the narrowest existing SQL Server-style number that fits the behavior.

If a failure does not have a good SQL Server match yet:

1. add a specific `DbError` variant if the engine needs to distinguish it
2. map it to the closest SQL Server number
3. add a parity test if the case is important

## Related Files

- [`crates/iridium_core/src/error.rs`](../crates/iridium_core/src/error.rs)
- [`crates/iridium_core/tests/try_catch_test.rs`](../crates/iridium_core/tests/try_catch_test.rs)
- [`crates/iridium_core/tests/procedural_edge_cases.rs`](../crates/iridium_core/tests/procedural_edge_cases.rs)
- [`crates/iridium_core/tests/sqlserver_comparison.rs`](../crates/iridium_core/tests/sqlserver_comparison.rs)

## Status

Current state is intentionally pragmatic:

- enough parity to support common clients and tests
- not enough parity to claim full SQL Server error compatibility


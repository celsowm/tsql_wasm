# T-SQL Parser Rewrite Plan (Reviewed and Updated)

## Review Outcome (April 1, 2026)

The previous plan had the right direction, but some details were inaccurate for the current codebase and a few migration steps were too risky.

### What was corrected

1. `split_statements()` is not semicolon-only today. It already uses keyword and `BEGIN`/`END` heuristics, but it is still string-scanning and fragile.
2. The current parser API and AST are fully owned (`String`) and heavily used across executor, server, WASM, and tests. A generic borrowed AST is a larger compatibility change than the previous plan implied.
3. Rewriting `parser/tokenizer.rs` in place on day 1 would break the existing expression parser path because it currently depends on `ExprToken` from that file.
4. The old timeline underestimated compatibility work (especially serde + WASM + existing tests).

This updated plan keeps the same end goal, but introduces lower-risk phases and concrete acceptance gates.

## Problem Statement

The parser still has a fundamental architecture problem: statement boundary detection is done with ad-hoc string scanning (`split_statements`, `find_keyword_top_level`, `find_if_blocks`, `find_top_level_begin`, `split_csv_top_level`, `tokenize_preserving_parens`, `find_set_operation`).

That means:

1. Boundary detection is heuristic, not token-aware.
2. Parser behavior depends on repeated `to_uppercase()` and string slicing.
3. Error reporting has no spans and no line/column context.
4. `parse_sql_with_quoted_ident` is a long ordered chain of prefix checks.
5. Utility scanners duplicate quote/paren/block-depth logic in many places.

The practical symptom remains: batches without explicit semicolons can still fail depending on shape and nesting, even when each individual statement is valid.

## Current Codebase Baseline

Measured from the current tree:

- Parser: 26 files, ~5,201 LOC (`crates/tsql_core/src/parser`)
- AST: 9 files, ~833 LOC (`crates/tsql_core/src/ast`)
- Tests: 134 files, ~12,365 LOC (`crates/tsql_core/tests`)

Compatibility constraints:

- Keep public entry points stable: `parse_batch`, `parse_batch_with_quoted_ident`, `parse_sql`, `parse_expr`.
- Keep `Statement` and `Expr` serializable (WASM and external consumers rely on this).
- Avoid executor-wide AST churn during parser rewrite.

## Goals and Non-Goals

### Goals

1. Make batch parsing token-aware and deterministic.
2. Remove string-scanning statement splitting.
3. Add span-based parse errors (line/column).
4. Replace giant dispatch chain with token-based dispatch.
5. Preserve current behavior for supported syntax before adding new syntax.

### Non-Goals (for this rewrite)

1. Full SQL Server grammar parity.
2. Large executor refactor.
3. Mandatory AST generics in the first ship.
4. Treating `GO` as a server-side SQL statement (it is a client batch separator).

## Target Architecture

```
Source: &str
  -> Tokenizer (single pass, spans)
  -> Parser cursor over tokens
  -> Existing owned AST (Statement / Expr)
  -> Executor
```

Important migration choice:

- First ship keeps owned AST (`String`) to minimize blast radius.
- Borrowed/generic AST becomes an optional follow-up after parser stability.

## Token Model

Create a new tokenizer module without breaking the current expression tokenizer immediately:

- New files:
  - `crates/tsql_core/src/parser/token/keyword.rs`
  - `crates/tsql_core/src/parser/token/token.rs`
  - `crates/tsql_core/src/parser/token/tokenizer.rs`
  - `crates/tsql_core/src/parser/token/span.rs`
- Keep existing `parser/tokenizer.rs` temporarily as compatibility code for expression parsing.

Core types:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub lo: u32,
    pub hi: u32,
    pub line: u32,
    pub col: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TokenKind<'a> {
    Ident(&'a str),
    QuotedIdent(&'a str),
    StringLiteral(&'a str),
    IntLiteral(i64),
    FloatLiteral(&'a str),
    BinaryLiteral(&'a str),
    Keyword(Keyword),
    Comma,
    Dot,
    Semicolon,
    LParen,
    RParen,
    Eq,
    NotEq,
    Lt,
    Lte,
    Gt,
    Gte,
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    Eof,
}

#[derive(Debug, Clone)]
pub struct Token<'a> {
    pub kind: TokenKind<'a>,
    pub span: Span,
}
```

## Phased Migration Plan

### Phase 0: Characterization and Safety Net (2 days)

1. Add tests that lock current supported behavior before rewrites.
2. Add explicit regression tests for problematic batch boundaries.
3. Keep old parser path as the default.

Exit criteria:

- `cargo test -p tsql_core` green.
- Baseline parser behavior captured in tests.

### Phase 1: Token-Aware Batch Boundary Engine (2-3 days)

1. Implement tokenizer and a boundary scanner that outputs statement slices by token span.
2. Replace `split_statements` inside `parse_batch_with_quoted_ident`.
3. Continue parsing each statement with existing `parse_sql_with_quoted_ident`.

This is the fastest low-risk fix for batch boundary bugs.

Exit criteria:

- Existing tests stay green.
- Regression case without semicolons parses into correct statement count.
- `BEGIN...END` and `TRY...CATCH` nested batches still parse correctly.

### Phase 2: Shared Token Stream for Expressions (3-4 days)

1. Migrate expression parser from legacy `ExprToken` tokenizer to the new tokenizer.
2. Remove duplicate expression tokenization code.
3. Keep `parse_expr` API unchanged.

Exit criteria:

- All expression tests pass.
- Old expression tokenizer no longer required.

### Phase 3: Statement Dispatch Refactor (3-5 days)

1. Introduce token-cursor parser skeleton (`Parser<'a> { tokens, pos }`).
2. Move dispatch from prefix-string checks to first-token keyword dispatch.
3. Implement recovery helpers (`skip_semicolons`, `synchronize`).

Exit criteria:

- `parse_sql_with_quoted_ident` routing no longer depends on giant ordered `starts_with` chain.
- No behavior regressions in DML/DDL/procedural smoke tests.

### Phase 4: Incremental Statement Parser Migration (7-10 days)

Migrate statement families in this order:

1. `SELECT` + set operations
2. DML (`INSERT/UPDATE/DELETE/MERGE`)
3. Procedural (`IF/WHILE/BEGIN...END/TRY...CATCH/DECLARE/SET/EXEC`)
4. DDL (`CREATE/DROP/ALTER` families)

For each family:

- Implement token-based parser.
- Keep AST shape unchanged.
- Delete corresponding string scanner helper usage.

Exit criteria:

- Legacy helper usage in migrated family removed.
- Tests for that family pass before moving next family.

### Phase 5: Span-Aware Errors (2-3 days)

1. Add parse error structure with span and expected/found context.
2. Bridge to existing `DbError::Parse(String)` for external compatibility.
3. Format errors with `line:col`.

Exit criteria:

- Parse errors include concrete location.
- Existing callers still receive `DbError`.

### Phase 6: Cleanup and Deletion (2 days)

1. Remove dead utilities:
   - `split_statements`
   - `find_keyword_top_level`
   - `find_if_blocks`
   - `find_top_level_begin`
   - `split_csv_top_level`
   - `tokenize_preserving_parens`
   - legacy `find_set_operation`
2. Remove old expression tokenizer module.
3. Final pass on docs and comments.

Exit criteria:

- No remaining parser call sites for removed helpers.
- `cargo test -p tsql_core` and `cargo test -p tsql_wasm` green.

## Optional Follow-Up: Borrowed/Generic AST

Treat generic `AstText` / borrowed AST as a separate RFC after parser rewrite is stable.

Why split it out:

1. It changes AST type signatures across many crates.
2. It impacts serde derives and WASM boundaries.
3. It is not required to fix boundary parsing correctness.

If pursued later:

- Start with dual representation (`BorrowedStatement` internal + existing `Statement` public).
- Add explicit conversion layer and benchmarks.

## Key Edge Cases to Keep as Required Tests

1. Batch boundaries without semicolons:
   - `SELECT 1\nSELECT 2`
   - `SELECT ... FROM ...\nIF ... SELECT ... ELSE EXEC ...`
2. Nested `BEGIN...END` and `TRY...CATCH`.
3. Bracketed identifiers with dots: `[schema.with.dots].[table.with.dots]`.
4. Escaped string quotes: `'it''s ok'`.
5. `@@` globals: `@@ROWCOUNT`, `@@MICROSOFTVERSION`.
6. `WITH` ambiguity:
   - CTE (`WITH cte AS (...) SELECT ...`)
   - table hint (`FROM t WITH (NOLOCK)`)
7. `SET` disambiguation:
   - variable assignment
   - session options
   - `SET IDENTITY_INSERT`
8. `OUTPUT` clause in `INSERT/UPDATE/DELETE/MERGE`.

## Critical Regression Test (Keep)

```rust
#[test]
fn test_ssms_batch_without_semicolons() {
    let sql = "
        DECLARE @edition sysname;
        SET @edition = cast(SERVERPROPERTY(N'EDITION') as sysname);
        SELECT case when @edition = N'SQL Azure' then 2 else 1 end as 'DatabaseEngineType',
        SERVERPROPERTY('EngineEdition') AS DatabaseEngineEdition,
        SERVERPROPERTY('ProductVersion') AS ProductVersion,
        @@MICROSOFTVERSION AS MicrosoftVersion;
        select host_platform from sys.dm_os_host_info
        if @edition = N'SQL Azure'
          select 'TCP' as ConnectionProtocol
        else
          exec ('select CONVERT(nvarchar(40),CONNECTIONPROPERTY(''net_transport'')) as ConnectionProtocol')
    ";

    let stmts = parse_batch(sql).unwrap();
    assert_eq!(stmts.len(), 5);
}
```

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Behavior regressions during migration | High | Migrate by statement family with tests and gated deletion |
| AST compatibility break in WASM/executor | High | Keep existing owned AST for first ship |
| Scope expansion to full grammar rewrite | Medium | Treat unsupported syntax as explicit out-of-scope per phase |
| Performance regression | Medium | Benchmark parse throughput before and after each major phase |
| Error message churn breaks tests | Low | Keep stable message prefixes, add location suffixes |

## Estimated Effort

| Phase | Duration |
|-------|----------|
| 0 | 2 days |
| 1 | 2-3 days |
| 2 | 3-4 days |
| 3 | 3-5 days |
| 4 | 7-10 days |
| 5 | 2-3 days |
| 6 | 2 days |
| Total | ~3-5 weeks |

## Acceptance Criteria for Completion

1. `parse_batch` no longer uses string-based statement splitting.
2. Old scanner helpers are removed.
3. Parser dispatch is token-based.
4. Error messages include line/column.
5. `cargo test -p tsql_core` and `cargo test -p tsql_wasm` pass.
6. Targeted regression tests for non-semicolon batches pass.

## Decision Log (Updated)

| Decision | Rationale |
|----------|-----------|
| Keep owned AST in rewrite scope | Minimizes blast radius and keeps executor/WASM stable |
| Split tokenizer into new module first | Avoids breaking current expression parser during transition |
| Deliver boundary fix before full parser migration | Fast path to user-visible bug fix |
| Make generic borrowed AST optional follow-up | Correctness first, optimization second |
| Keep `DbError` as external contract | Prevents downstream API breakage |

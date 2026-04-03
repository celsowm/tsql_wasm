# T-SQL Parser & AST Refactoring Plan

> **Goal**: Fix all SOLID violations, eliminate anti-patterns, and reorganize the directory tree for maintainability and extensibility.

---

## Table of Contents

1. [Current Problems Summary](#current-problems-summary)
2. [Current vs Proposed Directory Tree](#current-vs-proposed-directory-tree)
3. [Phase 1 — Strongly-Typed Keywords (Primitive Obsession)](#phase-1--strongly-typed-keywords)
4. [Phase 2 — Parser Infrastructure (Error Context & State)](#phase-2--parser-infrastructure)
5. [Phase 3 — Eliminate Code Duplication](#phase-3--eliminate-code-duplication)
6. [Phase 4 — Statement Enum Hierarchy (Open/Closed)](#phase-4--statement-enum-hierarchy)
7. [Phase 5 — God Function Decomposition (SRP)](#phase-5--god-function-decomposition)
8. [Phase 6 — AST Semantic Fixes (Stringly-Typed Hacks)](#phase-6--ast-semantic-fixes)
9. [Phase 7 — Module Re-exports Cleanup](#phase-7--module-re-exports-cleanup)
10. [Verification Strategy](#verification-strategy)

---

## Current Problems Summary

| # | Issue | SOLID Principle | Anti-Pattern | Severity |
|---|-------|----------------|--------------|----------|
| 1 | `Token::Keyword(Cow<str>)` — keywords are strings | — | Primitive Obsession | Medium |
| 2 | All errors are `ContextError::new()` with zero context | — | Silent Failures | **High** |
| 3 | Constraint parsing duplicated in `mod.rs` and `ddl.rs` | SRP | Code Duplication | Medium |
| 4 | `parse_begin_end` / `parse_try_catch` duplicated in `other.rs` and `control_flow.rs` | SRP | Copy-Paste | Medium |
| 5 | `parse_create` is a God Function (TABLE/VIEW/PROC/FUNC/TRIGGER) | SRP | God Function | Low |
| 6 | `parse_statement` is a 570-line mega match | SRP | God Function | Medium |
| 7 | `Statement` enum has 50+ flat variants | OCP | Shotgun Surgery | Medium |
| 8 | `TableName::name()` returns `"subquery"` magic string | — | Stringly-Typed | Medium |
| 9 | `peek_token`/`next_token`/`expect_keyword`/`parse_expr` re-exported in 4+ files | — | Wrapper Bloat | Low |
| 10 | Two parallel AST layers (`parser::ast` → `lower` → `ast`) with 870-line lowering | — | Unnecessary Indirection | Medium |

---

## Current vs Proposed Directory Tree

### CURRENT Structure
```
crates/tsql_core/src/
├── parser/
│   ├── ast/                          # "v2" intermediate AST (parser-specific)
│   │   ├── statements/
│   │   │   ├── mod.rs
│   │   │   ├── other.rs              # ← 50+ Statement variants, all DML/DDL/procedural types
│   │   │   └── query.rs
│   │   ├── common.rs
│   │   ├── expressions.rs
│   │   ├── mod.rs
│   │   └── tokens.rs                 # Token enum with Keyword(Cow<str>)
│   ├── parser/
│   │   ├── statements/
│   │   │   ├── mod.rs                # only re-exports 4 modules (missing 4 others!)
│   │   │   ├── control_flow.rs       # ← DUPLICATED: parse_begin_end, parse_try_catch
│   │   │   ├── cursor.rs             # ← NOT REGISTERED in mod.rs
│   │   │   ├── ddl.rs                # ← God Function: parse_create
│   │   │   ├── dml.rs
│   │   │   ├── other.rs              # ← DUPLICATED: parse_begin_end, parse_try_catch
│   │   │   ├── query.rs              # ← re-exports helpers already in expressions.rs
│   │   │   └── transaction.rs        # ← NOT REGISTERED in mod.rs
│   │   ├── expressions.rs            # canonical helpers (peek_token, next_token, etc.)
│   │   └── mod.rs                    # 694-line God Function: parse_statement
│   ├── lexer.rs
│   ├── lower.rs                      # 870-line v2→v1 AST lowering
│   └── mod.rs                        # public API
├── ast/                              # "v1" final AST (executor-facing)
│   ├── statements/
│   │   ├── ddl.rs
│   │   ├── dml.rs
│   │   ├── procedural.rs
│   │   └── query.rs
│   ├── common.rs                     # TableName with "subquery" hack
│   ├── data_types.rs
│   ├── expressions.rs
│   └── mod.rs
├── executor/
├── catalog/
├── storage/
├── types/
├── error.rs
└── lib.rs
```

### PROPOSED Structure
```
crates/tsql_core/src/
├── parser/
│   ├── token/
│   │   ├── mod.rs                    # Token enum (uses Keyword enum)
│   │   └── keyword.rs               # NEW: Keyword enum (200+ variants, from_str)
│   │
│   ├── error.rs                      # NEW: ParseError with Span, expected/found context
│   │
│   ├── state.rs                      # NEW: Parser<'a> struct (tokens, position, span tracking)
│   │
│   ├── lexer.rs                      # Lexer (produces Token<Keyword> instead of Token<Cow<str>>)
│   │
│   ├── parse/                        # Renamed from parser/parser/ (avoids confusion)
│   │   ├── mod.rs                    # parse_batch, parse_statement (thin dispatcher only)
│   │   ├── common.rs                 # NEW: shared helpers (parse_identifier, multipart_name,
│   │   │                             #       parse_constraint, parse_referential_action)
│   │   ├── expressions.rs            # Expression parsing (Pratt parser)
│   │   ├── helpers.rs                # peek_token, next_token, expect_keyword, parse_comma_list
│   │   │                             # (SINGLE source of truth, no re-exports)
│   │   └── statements/
│   │       ├── mod.rs                # registers ALL sub-modules
│   │       ├── select.rs             # SELECT / set operations
│   │       ├── insert.rs             # INSERT only
│   │       ├── update.rs             # UPDATE only
│   │       ├── delete.rs             # DELETE only
│   │       ├── merge.rs              # MERGE only
│   │       ├── create_table.rs       # CREATE TABLE only
│   │       ├── create_view.rs        # CREATE VIEW only
│   │       ├── create_procedure.rs   # CREATE PROCEDURE only
│   │       ├── create_function.rs    # CREATE FUNCTION only
│   │       ├── create_trigger.rs     # CREATE TRIGGER only
│   │       ├── create_other.rs       # CREATE INDEX / TYPE / SCHEMA
│   │       ├── alter.rs              # ALTER TABLE (delegates to common.rs for constraints)
│   │       ├── drop.rs               # All DROP variants
│   │       ├── control_flow.rs       # IF / BEGIN..END / WHILE / BREAK / CONTINUE / RETURN
│   │       ├── try_catch.rs          # TRY..CATCH (SINGLE implementation)
│   │       ├── cursor.rs             # DECLARE CURSOR / OPEN / FETCH / CLOSE / DEALLOCATE
│   │       ├── transaction.rs        # BEGIN TRAN / COMMIT / ROLLBACK / SAVE
│   │       ├── declare.rs            # DECLARE / DECLARE TABLE
│   │       ├── set.rs                # SET @var / SET OPTION / SET IDENTITY_INSERT
│   │       └── exec.rs              # EXEC / EXEC dynamic / sp_executesql
│   │
│   ├── lower.rs                      # v2→v1 lowering (simplified after Phase 4)
│   └── mod.rs                        # Public API: parse_sql, parse_batch, parse_expr
│
├── ast/                              # Final AST (executor-facing)
│   ├── mod.rs
│   ├── common.rs                     # TableFactor (renamed from TableName, no magic strings)
│   ├── data_types.rs
│   ├── expressions.rs
│   └── statements/
│       ├── mod.rs                    # Statement enum with grouped sub-enums
│       ├── ddl.rs                    # DdlStatement sub-enum + related structs
│       ├── dml.rs                    # DmlStatement sub-enum + related structs
│       ├── procedural.rs            # ProceduralStatement sub-enum + related structs
│       └── query.rs                 # SelectStmt, JoinClause, etc.
│
├── executor/
├── catalog/
├── storage/
├── types/
├── error.rs
└── lib.rs
```

---

## Phase 1 — Strongly-Typed Keywords

**Problem**: `Token::Keyword(Cow<'a, str>)` forces hundreds of `eq_ignore_ascii_case` calls.

**Files to change**:
- `parser/ast/tokens.rs` → `parser/token/keyword.rs` + `parser/token/mod.rs`
- `parser/lexer.rs`
- Every file in `parser/parser/` that matches on keywords

### Steps

1. **Create `keyword.rs`** with a `Keyword` enum:
   ```rust
   #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
   pub enum Keyword {
       Select, From, Where, Insert, Update, Delete, Create, Drop, Alter,
       Table, View, Procedure, Proc, Function, Trigger, Index, Schema, Type,
       Begin, End, If, Else, While, Break, Continue, Return,
       Declare, Set, Exec, Execute, Print,
       And, Or, Not, In, Is, Like, Between, Exists,
       Case, When, Then, Cast, Convert, TryCast, TryConvert,
       Null, As, Into, Values, Default, Top, Distinct,
       Join, Inner, Left, Right, Full, Outer, Cross, On,
       Union, Intersect, Except, All,
       Group, Order, By, Having, Asc, Desc,
       Primary, Key, Foreign, References, Unique, Check, Constraint,
       Identity, Column, Add, Cascade, Action, No,
       Tran, Transaction, Commit, Rollback, Save,
       Merge, Using, Matched, Source,
       Offset, Rows, Fetch, Next, Only,
       With, Over, Partition, Unbounded, Preceding, Following, Current, Row,
       Pivot, Unpivot, Apply, For, After, Instead, Of,
       Cursor, Open, Close, Deallocate, Prior, Last, First,
       Raiserror, Try, Catch,
       Output, Out, Returns, Isolation, Level, Read,
       Uncommitted, Committed, Repeatable, Serializable, Snapshot,
       Truncate, Go,
       // ... remaining keywords
   }

   impl Keyword {
       pub fn from_str(s: &str) -> Option<Keyword> { /* phf or match */ }
   }
   ```

2. **Update `Token` enum**:
   ```rust
   pub enum Token<'a> {
       Keyword(Keyword),           // was Keyword(Cow<'a, str>)
       Identifier(Cow<'a, str>),   // unchanged
       // ...
   }
   ```

3. **Update lexer** to resolve keywords at lex-time via `Keyword::from_str`.

4. **Update all parser files** to match on `Token::Keyword(Keyword::Select)` instead of string comparisons. This is the largest mechanical change.

### Impact
- ~150 `eq_ignore_ascii_case` calls replaced with enum matches
- Compiler enforces exhaustive matching
- Zero runtime string allocations for keywords
- **Risk**: Large diff, but purely mechanical

---

## Phase 2 — Parser Infrastructure

**Problem**: No position tracking, useless error messages (`ContextError::new()`).

**Files to create**: `parser/error.rs`, `parser/state.rs`  
**Files to change**: `parser/parser/expressions.rs`, all statement parsers

### Winnow Usage Strategy

| Layer | Uses Winnow? | Why |
|-------|-------------|-----|
| **Lexer** (`lexer.rs`) | ✅ Yes — keep | Winnow's `alt`, `repeat`, `take_while` combinators are a natural fit for character-level tokenization |
| **Parser** (`parser/` files) | ❌ Remove | The parser is already hand-written (manual `peek`/`next` loops). It only imports `ModalResult`, `ErrMode`, and `ContextError` as type wrappers — winnow combinators are never used. Replace with a custom `Parser` struct and `ParseError` type. |

> **Summary**: Winnow stays in the lexer. The parser drops its winnow dependency in favor of a purpose-built `Parser<'a>` + `ParseError` that provides real error context.

### Steps

1. **Create `ParseError`** (replaces `ErrMode::Backtrack(ContextError::new())`):
   ```rust
   #[derive(Debug, Clone)]
   pub struct ParseError {
       pub position: usize,          // token index
       pub expected: Vec<Expected>,   // what was expected
       pub found: Option<TokenKind>,  // what was actually found
   }

   #[derive(Debug, Clone)]
   pub enum Expected {
       Keyword(Keyword),
       Token(TokenKind),
       Description(&'static str),    // e.g., "expression", "column name"
   }
   ```

2. **Create `Parser<'a>` struct** (replaces raw `&mut &'a [Token<'a>]` + free functions):
   ```rust
   pub type ParseResult<T> = Result<T, ParseError>;

   pub struct Parser<'a> {
       tokens: &'a [Token<'a>],
       position: usize,
   }

   impl<'a> Parser<'a> {
       pub fn peek(&self) -> Option<&Token<'a>> { ... }
       pub fn next(&mut self) -> Option<&Token<'a>> { ... }
       pub fn expect_keyword(&mut self, kw: Keyword) -> ParseResult<()> { ... }
       pub fn expect_token(&mut self, tok: TokenKind) -> ParseResult<()> { ... }
       pub fn at_keyword(&self, kw: Keyword) -> bool { ... }
       pub fn eat_keyword(&mut self, kw: Keyword) -> bool { ... }
   }
   ```

3. **Migrate all parser functions** from `fn(input: &mut &'a [Token<'a>]) -> ModalResult<T>` to `fn(parser: &mut Parser<'a>) -> ParseResult<T>`.

4. **Replace every `Err(ErrMode::Backtrack(ContextError::new()))`** with descriptive errors.

5. **Remove winnow imports** from all `parser/parser/**` files (`use winnow::prelude::*`, `use winnow::error::*`).

6. **Propagate position** through `lower.rs` so the final `DbError::Parse` message includes location info.

### Impact
- Syntax errors now report position and expected token
- Parser state is encapsulated (no raw slice manipulation scattered everywhere)
- Winnow dependency is scoped only to the lexer where it belongs
- **Risk**: Medium — signature changes touch all parser functions, but behavior is preserved

---

## Phase 3 — Eliminate Code Duplication

**Problem**: Multiple copies of the same parsing logic scattered across files.

### 3A: Remove `other.rs` ↔ `control_flow.rs` Duplication

**Current state**: `parse_begin_end`, `parse_try_catch`, and `parse_if` exist in BOTH:
- `parser/parser/statements/other.rs` (lines 54–202)
- `parser/parser/statements/control_flow.rs` (lines 6–92)

**Steps**:
1. Keep the implementations in `control_flow.rs` (canonical home)
2. Delete the duplicate functions from `other.rs`
3. Update `other.rs` to re-export from `control_flow.rs`
4. Update `mod.rs` to register `control_flow` module
5. Fix all import paths

### 3B: Remove Helper Re-export Wrappers

**Current state**: `other.rs`, `query.rs`, and `ddl.rs` all define local wrappers:
```rust
// other.rs — these are pointless wrappers
pub fn parse_expr(...) { crate::parser::parser::expressions::parse_expr(input) }
pub fn expect_keyword(...) { crate::parser::parser::expressions::expect_keyword(input, kw) }
// ... 6 more wrappers
```

**Steps**:
1. Delete all wrapper functions from `other.rs`, `query.rs`
2. Use `use super::super::expressions::*` or direct imports instead
3. Single canonical location: `parser/parse/helpers.rs`

### 3C: Unify Constraint Parsing

**Current state**: ALTER TABLE ADD CONSTRAINT in `mod.rs` (lines 142–239) duplicates `parse_table_constraint` in `ddl.rs` (lines 285–394).

**Steps**:
1. Move `parse_table_constraint` to `parser/parse/common.rs`
2. Call it from both `alter.rs` and `create_table.rs`
3. Also move `parse_referential_action` to `common.rs` (currently duplicated as `parse_referential_action` in ddl.rs AND `parse_referential_action_v2` in mod.rs)

### 3D: Unify Cursor Parsing

**Current state**: `cursor.rs` is not registered in `statements/mod.rs`. FETCH/OPEN/CLOSE/DEALLOCATE are parsed inline in `parse_statement` (mod.rs lines 503–567), duplicating `cursor.rs`.

**Steps**:
1. Register `cursor.rs` in `statements/mod.rs`
2. Have `parse_statement` call the functions from `cursor.rs`
3. Delete the inline duplicates from `parse_statement`

### 3E: Unify Transaction Parsing

**Current state**: `transaction.rs` is not registered in `statements/mod.rs`. BEGIN/COMMIT/ROLLBACK/SAVE TRAN are parsed inline in `parse_statement` (mod.rs lines 420–501), duplicating `transaction.rs`.

**Steps**:
1. Register `transaction.rs` in `statements/mod.rs`
2. Delegate from `parse_statement` to transaction parser functions
3. Delete inline transaction parsing from `parse_statement`

### Impact
- ~400 lines of duplicated code removed
- Single source of truth for every parsing concern
- **Risk**: Low — delete duplicates, update imports

---

## Phase 4 — Statement Enum Hierarchy (Open/Closed)

**Problem**: The `Statement` enum in `parser/ast/statements/other.rs` has 50+ flat variants. Adding a new statement requires modifying this enum AND every exhaustive `match` in `lower.rs` and the executor.

### Steps

1. **Create sub-enums** in the parser's intermediate AST:
   ```rust
   pub enum Statement<'a> {
       Ddl(DdlStatement<'a>),
       Dml(DmlStatement<'a>),
       Procedural(ProceduralStatement<'a>),
       Transaction(TransactionStatement<'a>),
       Cursor(CursorStatement<'a>),
       Session(SessionStatement<'a>),
   }

   pub enum DdlStatement<'a> {
       CreateTable { name, columns, constraints },
       CreateView { name, query },
       CreateProcedure { name, params, body },
       CreateFunction { name, params, returns, body },
       CreateTrigger { name, table, events, is_instead_of, body },
       CreateIndex { name, table, columns },
       CreateType { name, columns },
       CreateSchema(Cow<'a, str>),
       DropTable(Vec<Cow<'a, str>>),
       DropView(Vec<Cow<'a, str>>),
       DropProcedure(Vec<Cow<'a, str>>),
       DropFunction(Vec<Cow<'a, str>>),
       DropTrigger(Vec<Cow<'a, str>>),
       DropIndex { name, table },
       DropType(Vec<Cow<'a, str>>),
       DropSchema(Cow<'a, str>),
       AlterTable { table, action },
       TruncateTable(Vec<Cow<'a, str>>),
   }

   pub enum DmlStatement<'a> {
       Select(Box<SelectStmt<'a>>),
       Insert(Box<InsertStmt<'a>>),
       Update(Box<UpdateStmt<'a>>),
       Delete(Box<DeleteStmt<'a>>),
       Merge(Box<MergeStmt<'a>>),
       SelectAssign { assignments, from, selection },
       WithCte { ctes, body },
   }

   pub enum ProceduralStatement<'a> {
       Declare(Vec<DeclareVar<'a>>),
       DeclareTableVar { name, columns },
       Set { variable, expr },
       If { condition, then_stmt, else_stmt },
       BeginEnd(Vec<Statement<'a>>),
       While { condition, stmt },
       Break,
       Continue,
       Return(Option<Expr<'a>>),
       Print(Expr<'a>),
       ExecDynamic { sql_expr },
       ExecProcedure { name, args },
       SpExecuteSql { sql_expr, params_def, args },
       Raiserror { msg, severity, state },
       TryCatch { try_body, catch_body },
   }

   pub enum TransactionStatement<'a> {
       Begin(Option<Cow<'a, str>>),
       Commit(Option<Cow<'a, str>>),
       Rollback(Option<Cow<'a, str>>),
       Save(Cow<'a, str>),
       SetIsolationLevel(IsolationLevel),
   }

   pub enum CursorStatement<'a> {
       Declare { name, query },
       Open(Cow<'a, str>),
       Fetch { name, direction, into_vars },
       Close(Cow<'a, str>),
       Deallocate(Cow<'a, str>),
   }
   ```

2. **Apply the same grouping** to the final AST (`ast/statements/mod.rs`):
   ```rust
   pub enum Statement {
       Ddl(DdlStatement),
       Dml(DmlStatement),
       Procedural(ProceduralStatement),
       Transaction(TransactionStatement),
       Cursor(CursorStatement),
       Session(SessionStatement),
   }
   ```

3. **Update `lower.rs`** to match on sub-enums instead of 50+ flat arms.

4. **Update all executor match sites** — use nested matches or add helper methods on the sub-enums.

### Impact
- Adding a new DDL statement only requires touching `DdlStatement` and its corresponding executor arm
- Related statement types are grouped logically
- `lower.rs` becomes organized into per-category lowering functions
- **Risk**: High — this is a large refactor touching executor code. Do this incrementally (one category at a time)

### Recommended Incremental Order
1. `TransactionStatement` (smallest, fewest executor matches)
2. `CursorStatement` (small, self-contained)
3. `DdlStatement` (large but all similar)
4. `ProceduralStatement` (medium, many executor touches)
5. `DmlStatement` (core queries, most complex)

---

## Phase 5 — God Function Decomposition (SRP)

**Problem**: `parse_statement` (694 lines) and `parse_create` (120 lines) do too much.

### 5A: Decompose `parse_statement`

**Current**: One giant `match k_upper.as_str()` with 30+ arms, many containing inline parsing logic.

**After Phase 3** (duplication removed) many arms will already be delegating. The remaining work:

1. **Extract `parse_drop`** — lines 85-127 (DROP TABLE/VIEW/PROC/INDEX/TYPE/SCHEMA/FUNCTION/TRIGGER) into `statements/drop.rs`
2. **Extract `parse_alter`** — lines 134-290 into `statements/alter.rs`
3. **Reduce `parse_statement` to a thin dispatcher** (~50 lines):
   ```rust
   pub fn parse_statement<'a>(parser: &mut Parser<'a>) -> Result<Statement<'a>> {
       match parser.peek_keyword()? {
           Keyword::Select => parse_select_or_assign(parser),
           Keyword::Insert => parse_insert_stmt(parser),
           Keyword::Update => parse_update_stmt(parser),
           Keyword::Delete => parse_delete_stmt(parser),
           Keyword::Create => parse_create_dispatch(parser),
           Keyword::Drop   => parse_drop(parser),
           Keyword::Alter  => parse_alter(parser),
           Keyword::Merge  => parse_merge_stmt(parser),
           Keyword::Begin  => parse_begin_dispatch(parser),
           // ... one line per keyword, all delegating
       }
   }
   ```

### 5B: Decompose `parse_create`

1. Split into: `parse_create_table`, `parse_create_view`, `parse_create_procedure`, `parse_create_function`, `parse_create_trigger`
2. Keep `parse_create_dispatch` as a thin keyword-match dispatcher

### Impact
- Each file has a single, testable responsibility
- `parse_statement` becomes a readable routing table
- **Risk**: Low if done AFTER Phase 3 (dependencies are clean)

---

## Phase 6 — AST Semantic Fixes (Stringly-Typed Hacks)

**Problem**: `TableName::name()` returns `"subquery"` for derived tables.

### Steps

1. **Rename `TableName` → `TableFactor`** in `ast/common.rs`:
   ```rust
   pub enum TableFactor {
       Named(ObjectName),
       Derived(Box<SelectStmt>),
   }
   ```

2. **Remove `name_string()`** and `name()` methods that return magic strings.

3. **Add `as_object_name(&self) -> Option<&ObjectName>`** for safe access.

4. **Force callers to handle both variants** via pattern matching — the compiler will find all call sites.

5. **Update `schema_or_dbo()`** to return `Option<&str>` instead of a fake `"dbo"` for subqueries.

### Impact
- Compiler catches all code that assumed a table always has a name
- No more silent bugs from magic `"subquery"` strings
- **Risk**: Low — compiler-guided refactor

---

## Phase 7 — Module Re-exports Cleanup

**Problem**: The `parser/` tree has a confusing `parser/parser/` nesting, and modules like `cursor.rs` and `transaction.rs` are orphaned.

### Steps

1. **Rename `parser/parser/`** → **`parser/parse/`** to avoid the `parser::parser` stutter.

2. **Register all modules** in `parse/statements/mod.rs`:
   ```rust
   pub mod select;
   pub mod insert;
   pub mod update;
   pub mod delete;
   pub mod merge;
   pub mod create_table;
   pub mod create_view;
   pub mod create_procedure;
   pub mod create_function;
   pub mod create_trigger;
   pub mod create_other;
   pub mod alter;
   pub mod drop;
   pub mod control_flow;
   pub mod try_catch;
   pub mod cursor;
   pub mod transaction;
   pub mod declare;
   pub mod set;
   pub mod exec;
   ```

3. **Create `parse/helpers.rs`** as the SINGLE source for:
   - `peek_token`, `next_token`
   - `expect_keyword`, `expect_token`
   - `parse_comma_list`
   - `is_stop_keyword`

4. **Delete all wrapper re-exports** from `other.rs`, `query.rs`, etc.

5. **Flatten public API** in `parser/mod.rs` — users only see `parse_sql`, `parse_batch`, `parse_expr`.

---

## Verification Strategy

### After Each Phase

```bash
# 1. Compile check
cargo check -p tsql_core

# 2. Run all core tests
cargo test -p tsql_core

# 3. Run WASM tests
cargo test -p tsql_wasm

# 4. Check for warnings (unused imports, dead code)
cargo clippy -p tsql_core -- -W clippy::all
```

### Integration Test for Error Messages (After Phase 2)

```rust
#[test]
fn test_error_reports_position() {
    let result = parse_sql("SELECT * FORM users");
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("expected keyword FROM"));
    assert!(err.contains("found 'FORM'"));
}
```

### Regression Test for Phase 4

Before starting Phase 4, create a snapshot of all existing test outputs:
```bash
cargo test -p tsql_core -- --nocapture > pre_refactor_baseline.txt 2>&1
```
After Phase 4, compare against baseline to ensure no behavioral changes.

---

## Execution Order & Dependencies

```
Phase 1 (Keywords)
    ↓
Phase 2 (Parser State)        ← depends on Phase 1 (uses Keyword enum)
    ↓
Phase 3 (Dedup)               ← depends on Phase 2 (uses Parser struct)
    ↓
Phase 5 (God Functions)       ← depends on Phase 3 (clean dependencies)
    ↓
Phase 4 (Statement Hierarchy) ← depends on Phase 5 (clean statement files)
    ↓
Phase 6 (TableFactor)         ← independent, can be done anytime after Phase 4
    ↓
Phase 7 (Module Cleanup)      ← final pass, rename directories
```

> **Estimated effort**: ~5-7 working sessions total  
> **Highest ROI first**: Phase 3 (dedup) is the easiest win; Phase 2 (error context) is the most user-visible improvement.

---

## Risks & Mitigations

| Risk | Mitigation |
|------|-----------|
| Large diff breaks tests | Run `cargo test` after every sub-step, not just each phase |
| Executor breaks after Phase 4 enum change | Do one sub-enum at a time, starting with Transaction (smallest) |
| `lower.rs` becomes stale after Phase 4 | Consider eliminating the two-AST design entirely (merge parser AST and final AST) in a future phase |
| Phase 1 keyword enum is tedious | Use a macro `define_keywords!` to auto-generate the enum, `from_str`, and `Display` |

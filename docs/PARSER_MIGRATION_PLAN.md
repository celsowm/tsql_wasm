# Parser Migration Plan: Replace Old Parser with v2 Winnow Parser

## Motivation

The current parser uses a naive `split_statements()` function that splits SQL batches by scanning raw text for semicolons and keyword boundaries. This fundamentally breaks when SSMS sends complex multi-statement batches without semicolons:

```sql
select host_platform from sys.dm_os_host_info
if @edition = N'SQL Azure'
    select 'TCP' as ConnectionProtocol
else
    exec ('select CONVERT(nvarchar(40),CONNECTIONPROPERTY(''net_transport'')) as ConnectionProtocol')
```

The old parser sees `sys.dm_os_host_info if` as a table name because `if` is just raw text after a dot-separated identifier — it never tokenizes first.

The v2 parser (in `parser/v2/`) uses **winnow** with a proper **lexer → token stream → parser** pipeline, which correctly identifies `IF` as a keyword token starting a new statement.

## Architecture

```
SQL string
  → v2 Lexer (winnow)     → Vec<Token>
  → v2 Parser (winnow)    → Vec<v2::ast::Statement>
  → Lowering layer         → Vec<crate::ast::Statement>
  → Executor (UNCHANGED)
```

The executor, tooling, dispatch, and table_util modules stay on `crate::ast::Statement`. We build a **lowering layer** that converts `v2::ast` → `crate::ast`. The old parser is fully deleted.

## Gap Analysis

### v2 Parser: What's Already Done

| Category | Supported |
|---|---|
| **Lexer** | Keywords, identifiers, bracketed identifiers, variables (`@`, `@@`), numbers, strings (`N'...'`), operators, punctuation, comments (`--`, `/* */`), `GO` |
| **Expressions** | Pratt parser with binary ops, unary (`NOT`, `-`), `IS NULL`/`IS NOT NULL`, `LIKE`/`NOT LIKE`, `CAST`, `CONVERT`, `CASE/WHEN`, `EXISTS`, subqueries in parens, function calls, qualified identifiers, `*` wildcard |
| **SELECT** | `DISTINCT`, `TOP`, projection with aliases, `INTO`, `FROM` with joins (INNER/LEFT/RIGHT/FULL/CROSS), subquery tables, `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, `OFFSET/FETCH`, `UNION/INTERSECT/EXCEPT` |
| **DML** | `INSERT INTO ... VALUES/SELECT`, `UPDATE ... SET ... FROM ... WHERE`, `DELETE FROM ... WHERE`, `MERGE ... USING ... ON ... WHEN MATCHED/NOT MATCHED` |
| **DDL** | `CREATE TABLE` (columns with NULL/NOT NULL/IDENTITY/PRIMARY KEY), `CREATE VIEW`, `CREATE PROCEDURE`, `CREATE FUNCTION` (scalar/inline table), `CREATE TRIGGER`, `DROP TABLE/VIEW/PROCEDURE`, `TRUNCATE TABLE` |
| **Procedural** | `DECLARE`, `SET @var = expr`, `IF/ELSE`, `BEGIN/END`, `WHILE`, `EXEC proc`/`EXEC('dynamic')`, `PRINT`, `BREAK`, `CONTINUE`, `RETURN` |
| **Transactions** | `BEGIN/COMMIT/ROLLBACK/SAVE TRANSACTION` |
| **Session** | `SET NOCOUNT ON/OFF`, `SET LOCK_TIMEOUT`, `SET TRANSACTION ISOLATION LEVEL`, `SET IDENTITY_INSERT` |
| **Other** | `WITH CTE`, `MERGE` |

### v2 Parser: What's Missing

#### Expressions (Phase 2)

| Feature | Old AST Type | Notes |
|---|---|---|
| `BETWEEN expr AND expr` | `Expr::Between` | Needs special handling since `AND` is also a binary op |
| `NOT BETWEEN` | `Expr::Between { negated: true }` | |
| `IN (value_list)` | `Expr::InList` | v2 AST has it, parser doesn't handle it |
| `IN (subquery)` | `Expr::InSubquery` | v2 AST has it, parser doesn't handle it |
| `NOT IN` | negated variants | |
| `NOT EXISTS` | `Expr::Exists { negated: true }` | v2 AST has it, parser always sets `negated: false` |
| `TRY_CAST(expr AS type)` | `Expr::TryCast` | Not in v2 AST or parser |
| `TRY_CONVERT(type, expr)` | `Expr::TryConvert` | Not in v2 AST or parser |
| Window functions | `Expr::WindowFunction` | `ROW_NUMBER() OVER(PARTITION BY ... ORDER BY ...)`, `RANK()`, `LAG()`, etc. |
| `FloatLiteral` as string | `Expr::FloatLiteral(String)` | Old AST stores as string, v2 stores as `f64::to_bits()` |
| Binary literal `0xABCD` | `Expr::BinaryLiteral(Vec<u8>)` | v2 AST has it, lexer doesn't parse it |
| `~` bitwise NOT | `UnaryOp::BitwiseNot` (missing in old) | v2 AST has it, lexer missing `~` token |
| Keyword as function name | e.g. `SERVERPROPERTY(...)` | v2 parser only matches `Identifier` for function calls, not `Keyword` |

#### Query (Phase 3)

| Feature | Old AST Type | Notes |
|---|---|---|
| `CROSS APPLY` / `OUTER APPLY` | `ApplyClause`, `ApplyType` | Not in v2 at all |
| `PIVOT` / `UNPIVOT` | `PivotSpec`, `UnpivotSpec` on `TableRef` | Not in v2 at all |
| Table hints `WITH (NOLOCK)` | `TableRef.hints: Vec<String>` | Not in v2 |
| `SELECT @var = expr FROM ...` | `Statement::SelectAssign` | Separate statement type in old AST |
| String alias `'alias'` for select items | Already partially supported | Need to verify edge cases |

#### DML (Phase 4)

| Feature | Old AST Type | Notes |
|---|---|---|
| `OUTPUT INSERTED.*` / `OUTPUT DELETED.*` | `OutputColumn`, `OutputSource` | On INSERT/UPDATE/DELETE/MERGE |
| `OUTPUT INTO @table` | `output_into: Option<ObjectName>` | |
| `INSERT ... DEFAULT VALUES` | `InsertSource::DefaultValues` | |
| `TOP (n)` on UPDATE | `UpdateStmt.top` | |
| `TOP (n)` on DELETE | `DeleteStmt.top` | |
| `FROM` clause with joins on UPDATE/DELETE | `FromClause { tables, joins, applies }` | v2 UPDATE has `from: Option<Vec<TableRef>>` but no joins/applies |
| `INSERT ... EXEC proc` | `InsertSource::Exec(Box<Statement>)` | v2 has `InsertSource::Exec { procedure, args }` |
| Multi-row VALUES | Already supported | `VALUES (1,2), (3,4)` |

#### DDL (Phase 5)

| Feature | Old AST Type | Notes |
|---|---|---|
| `ALTER TABLE ADD COLUMN` | `AlterTableStmt`, `AlterTableAction::AddColumn` | Not in v2 |
| `ALTER TABLE DROP COLUMN` | `AlterTableAction::DropColumn` | |
| `ALTER TABLE ADD CONSTRAINT` | `AlterTableAction::AddConstraint` | |
| `ALTER TABLE DROP CONSTRAINT` | `AlterTableAction::DropConstraint` | |
| `CREATE INDEX` | `CreateIndexStmt` | Not in v2 |
| `DROP INDEX` | `DropIndexStmt` | Not in v2 |
| `CREATE TYPE` | `CreateTypeStmt` | Not in v2 |
| `DROP TYPE` | `DropTypeStmt` | Not in v2 |
| `CREATE SCHEMA` | `CreateSchemaStmt` | Not in v2 |
| `DROP SCHEMA` | `DropSchemaStmt` | Not in v2 |
| `DROP FUNCTION` | `DropFunctionStmt` | Not in v2 |
| `DROP TRIGGER` | `DropTriggerStmt` | Not in v2 |
| Full `ColumnSpec` | default, check, computed, foreign key, constraint names, unique | v2 `ColumnDef` only has name, type, nullable, identity, primary_key |
| `TableConstraintSpec` | PK, Unique, FK, Check, Default (named) | Not in v2 |

#### Procedural (Phase 6)

| Feature | Old AST Type | Notes |
|---|---|---|
| `BEGIN TRY ... END TRY BEGIN CATCH ... END CATCH` | `TryCatchStmt` | Not in v2 |
| `RAISERROR(msg, severity, state)` | `RaiserrorStmt` | Not in v2 |
| `DECLARE @t TABLE (...)` | `DeclareTableVarStmt` | Not in v2 |
| `EXEC sp_executesql N'...', N'@p int', @p=1` | `SpExecuteSqlStmt` | Not in v2 |
| `EXEC ('dynamic sql')` vs `EXEC proc @args` | `ExecDynamic` vs `ExecProcedure` | v2 conflates them into one `Exec` variant |
| Exec arguments with names/OUTPUT | `ExecArgument { name, expr, is_output }` | v2 `Exec` has `args: Vec<Expr>` only |
| `DECLARE cursor_name CURSOR FOR SELECT ...` | `DeclareCursorStmt` | Not in v2 |
| `OPEN cursor_name` | `Statement::OpenCursor(String)` | Not in v2 |
| `FETCH NEXT FROM cursor INTO @vars` | `FetchCursorStmt` | Not in v2 |
| `CLOSE cursor_name` | `Statement::CloseCursor(String)` | Not in v2 |
| `DEALLOCATE cursor_name` | `Statement::DeallocateCursor(String)` | Not in v2 |

#### Data Types (Lowering)

| v2 `DataType` | Old `DataTypeSpec` | Notes |
|---|---|---|
| `Real` | missing in v2 | Add to v2 |
| `Money`, `SmallMoney` | exist in both | v2 has them |
| `Char(Option<u32>)` | `Char(u16)` | Different size types, need mapping |
| `NChar(Option<u32>)` | `NChar(u16)` | Same |
| `Binary(Option<u32>)` | `Binary(u16)` | Same |
| `VarBinary(Option<u32>)` | `VarBinary(u16)` | Same |
| `DateTimeOffset`, `SmallDateTime` | missing in old | v2 has them, old doesn't — keep or drop |
| `Xml`, `Image`, `Text`, `NText` | missing in old | v2 has them, old doesn't |
| `Table` | missing in old | v2 has it for table-valued params |
| `Custom(Cow<str>)` | missing in old | Fallback for unknown types like `sysname` |
| `MAX` size | not handled | `VARCHAR(MAX)` → `VarChar(u16::MAX)` or special value |

#### Lexer

| Feature | Notes |
|---|---|
| `~` operator | Add to `parse_operator_token` |
| `0x` hex literals | Add `parse_binary_literal` |
| `::` scope resolution | For `value::type` syntax (rarely used) |
| Quoted identifiers `"name"` | When `SET QUOTED_IDENTIFIER ON` |

## Implementation Phases

### Phase 1: Complete Lexer/Token Coverage
**Files:** `parser/v2/lexer.rs`, `parser/v2/ast/tokens.rs`

- [ ] Add `~` to operator tokens
- [ ] Add binary literal `0x...` parsing
- [ ] Handle `MAX` keyword in data type contexts
- [ ] Add quoted identifier `"name"` support (when `QUOTED_IDENTIFIER ON`)
- [ ] Add missing keywords to `is_keyword()`: `BETWEEN`, `APPLY`, `PIVOT`, `UNPIVOT`, `OVER`, `PARTITION`, `ROWS`, `RANGE`, `UNBOUNDED`, `PRECEDING`, `FOLLOWING`, `CURRENT`, `ROW`, `TRY_CAST`, `TRY_CONVERT`, `RAISERROR`, `TRY`, `CATCH`, `CURSOR`, `OPEN`, `CLOSE`, `DEALLOCATE`, `ALTER`, `ADD`, `CONSTRAINT`, `REFERENCES`, `DEFAULT`, `CHECK`, `UNIQUE`, `FOREIGN`, `NOLOCK`, `ROWLOCK`, `TABLOCK`, `READUNCOMMITTED`, `READCOMMITTED`, `HOLDLOCK`, `UPDLOCK`, `XLOCK`

### Phase 2: Complete Expression Parser
**Files:** `parser/v2/parser/expressions.rs`, `parser/v2/ast/expressions.rs`

- [ ] `BETWEEN expr AND expr` / `NOT BETWEEN`
- [ ] `IN (list)` / `NOT IN (list)`
- [ ] `IN (subquery)` / `NOT IN (subquery)`
- [ ] `NOT EXISTS` (set `negated: true`)
- [ ] `TRY_CAST(expr AS type)` — add to v2 AST + parser
- [ ] `TRY_CONVERT(type, expr, style)` — add to v2 AST + parser
- [ ] Window functions: `func() OVER (PARTITION BY ... ORDER BY ... ROWS/RANGE ...)` — add to v2 AST + parser
- [ ] Allow `Keyword` tokens as function names (for `SERVERPROPERTY`, `ISNULL`, `COALESCE`, `IIF`, `NULLIF`, etc.)
- [ ] Binary literal parsing in primary expressions
- [ ] Handle negative number literals properly

### Phase 3: Complete Query Parser
**Files:** `parser/v2/parser/statements/query.rs`, `parser/v2/ast/statements/query.rs`

- [ ] `CROSS APPLY (subquery) alias` / `OUTER APPLY` — add `ApplyClause` to v2 AST
- [ ] `PIVOT (agg(col) FOR col IN (values)) alias` — add to v2 AST
- [ ] `UNPIVOT (value_col FOR pivot_col IN (columns)) alias` — add to v2 AST
- [ ] Table hints: `tablename WITH (NOLOCK)` — add `hints: Vec<Cow<str>>` to `TableRef::Table`
- [ ] `SELECT @var = expr, @var2 = expr2 FROM ...` — detect variable assignment in projection, emit `SelectAssign` statement

### Phase 4: Complete DML Parser
**Files:** `parser/v2/parser/statements/dml.rs` (new), `parser/v2/ast/statements/dml.rs` (new or extend `other.rs`)

- [ ] `OUTPUT INSERTED.col, DELETED.col` clause — add to v2 AST
- [ ] `OUTPUT INTO @table_var` — add to v2 AST
- [ ] `INSERT ... DEFAULT VALUES`
- [ ] `TOP (n)` on UPDATE/DELETE
- [ ] FROM clause with joins/applies on UPDATE/DELETE
- [ ] `INSERT ... EXEC proc @arg1, @arg2` with proper argument parsing
- [ ] Exec arguments: `@name = expr OUTPUT`

### Phase 5: Complete DDL Parser
**Files:** `parser/v2/parser/statements/ddl.rs` (new), `parser/v2/ast/statements/ddl.rs` (new or extend)

- [ ] `ALTER TABLE ... ADD column_def`
- [ ] `ALTER TABLE ... DROP COLUMN col`
- [ ] `ALTER TABLE ... ADD CONSTRAINT ...`
- [ ] `ALTER TABLE ... DROP CONSTRAINT name`
- [ ] `CREATE INDEX name ON table (columns)`
- [ ] `DROP INDEX name ON table`
- [ ] `CREATE TYPE name AS TABLE (...)`
- [ ] `DROP TYPE name`
- [ ] `CREATE SCHEMA name`
- [ ] `DROP SCHEMA name`
- [ ] `DROP FUNCTION name`
- [ ] `DROP TRIGGER name`
- [ ] Full `ColumnSpec`: `DEFAULT expr`, `CHECK (expr)`, computed columns `AS expr`, `FOREIGN KEY REFERENCES`, constraint names (`CONSTRAINT name`), `UNIQUE`
- [ ] `TableConstraintSpec`: `PRIMARY KEY`, `UNIQUE`, `FOREIGN KEY ... REFERENCES ... ON DELETE/UPDATE`, `CHECK`, `DEFAULT`

### Phase 6: Complete Procedural Parser
**Files:** `parser/v2/parser/statements/other.rs` (extend), `parser/v2/ast/statements/other.rs` (extend)

- [ ] `BEGIN TRY ... END TRY BEGIN CATCH ... END CATCH`
- [ ] `RAISERROR(msg, severity, state)`
- [ ] `DECLARE @t TABLE (col1 int, col2 varchar(50), ...)` with full column specs
- [ ] `EXEC sp_executesql N'query', N'@p1 int', @p1 = value` — special-case `sp_executesql`
- [ ] Split `Exec` into `ExecDynamic` (exec string expression) vs `ExecProcedure` (exec named proc with args)
- [ ] Exec arguments: named `@param = expr`, positional `expr`, `OUTPUT` flag
- [ ] `DECLARE cursor_name CURSOR FOR SELECT ...`
- [ ] `OPEN cursor_name`
- [ ] `FETCH NEXT FROM cursor_name INTO @var1, @var2`
- [ ] `CLOSE cursor_name`
- [ ] `DEALLOCATE cursor_name`

### Phase 7: Build Lowering Layer
**Files:** `parser/v2/lower.rs` (new)

Functions to implement:
```
lower_batch(Vec<v2::Statement>) → Result<Vec<ast::Statement>, DbError>
lower_statement(v2::Statement) → Result<ast::Statement, DbError>
lower_expr(v2::Expr) → Result<ast::Expr, DbError>
lower_select(v2::SelectStmt) → Result<ast::SelectStmt, DbError>
lower_table_ref(v2::TableRef) → Result<ast::TableRef, DbError>
lower_data_type(v2::DataType) → Result<ast::DataTypeSpec, DbError>
lower_insert(v2::InsertStmt) → Result<ast::InsertStmt, DbError>
lower_update(v2::UpdateStmt) → Result<ast::UpdateStmt, DbError>
lower_delete(v2::DeleteStmt) → Result<ast::DeleteStmt, DbError>
lower_merge(v2::MergeStmt) → Result<ast::MergeStmt, DbError>
lower_create(v2::CreateStmt) → Result<ast::Statement, DbError>
lower_object_name(Vec<Cow<str>>) → ast::ObjectName
```

Key mapping decisions:
- v2 `TableRef::Join` (nested) → old flat `SelectStmt.from + joins: Vec<JoinClause>`
- v2 `SelectStmt.set_op` (chained) → old `Statement::SetOp { left, right }` (top-level)
- v2 `Expr::Float(u64)` → old `Expr::FloatLiteral(String)`
- v2 `Expr::Variable` → old `Expr::Identifier` (variables are identifiers in old AST)
- v2 `Expr::Bool` → old doesn't have it, convert to `Expr::Integer(1/0)`
- v2 `DataType::Custom` → try mapping to `DataTypeSpec` variants or error
- v2 `Vec<Cow<str>>` multipart names → old `ObjectName { schema, name }`

### Phase 8: Wire Up & Delete Old Parser
**Files:** `parser/mod.rs`, `lib.rs`

- [ ] Rewrite `parse_batch` / `parse_batch_with_quoted_ident`:
  ```
  fn parse_batch(sql: &str) -> Result<Vec<Statement>, DbError> {
      let tokens = v2::lexer::lex(&mut sql_ref)?;
      let v2_stmts = v2::parser::parse_batch(&mut tokens.as_slice())?;
      v2::lower::lower_batch(v2_stmts)
  }
  ```
- [ ] Rewrite `parse_sql` / `parse_sql_with_quoted_ident` similarly
- [ ] Rewrite `parse_expr` / `parse_expr_subquery_aware` / `parse_expr_with_quoted_ident`
- [ ] Handle `QUOTED_IDENTIFIER` mode in lexer (double-quoted strings vs identifiers)
- [ ] Delete old parser files:
  - `parser/tokenizer.rs`
  - `parser/utils.rs`
  - `parser/expression/` (entire directory)
  - `parser/statements/` (entire old directory — NOT the v2 one)
- [ ] Move `parser/v2/` contents up to `parser/` (optional cleanup)
- [ ] Update `lib.rs` exports
- [ ] Run full test suite: `cargo test -p tsql_core`
- [ ] Run WASM tests: `cargo test -p tsql_wasm`

## Testing Strategy

### During migration (each phase)
- Run `cargo test -p tsql_core` after each phase
- Any new parser feature should pass existing tests that exercise that SQL construct

### After Phase 8 (final validation)
- All existing `tsql_core` and `tsql_wasm` tests must pass
- Test the SSMS connection query that motivated this migration:
  ```sql
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
  ```
- Test with SSMS connecting to the playground server

## Files to Delete (Phase 8)

```
crates/tsql_core/src/parser/tokenizer.rs
crates/tsql_core/src/parser/utils.rs
crates/tsql_core/src/parser/expression/mod.rs
crates/tsql_core/src/parser/expression/operators.rs
crates/tsql_core/src/parser/expression/primary.rs
crates/tsql_core/src/parser/expression/special.rs
crates/tsql_core/src/parser/expression/window.rs
crates/tsql_core/src/parser/statements/mod.rs
crates/tsql_core/src/parser/statements/select.rs
crates/tsql_core/src/parser/statements/ddl.rs
crates/tsql_core/src/parser/statements/subquery_utils.rs
crates/tsql_core/src/parser/statements/transaction.rs
crates/tsql_core/src/parser/statements/dml/mod.rs
crates/tsql_core/src/parser/statements/dml/insert.rs
crates/tsql_core/src/parser/statements/dml/update.rs
crates/tsql_core/src/parser/statements/dml/delete.rs
crates/tsql_core/src/parser/statements/dml/merge.rs
crates/tsql_core/src/parser/statements/dml/output.rs
crates/tsql_core/src/parser/statements/procedural/mod.rs
crates/tsql_core/src/parser/statements/procedural/variable.rs
crates/tsql_core/src/parser/statements/procedural/routine.rs
crates/tsql_core/src/parser/statements/procedural/print.rs
crates/tsql_core/src/parser/statements/procedural/execute.rs
crates/tsql_core/src/parser/statements/procedural/control_flow.rs
crates/tsql_core/src/parser/statements/procedural/cursor.rs
```

## Risks

1. **Expression parsing edge cases**: The Pratt parser needs careful handling of `BETWEEN ... AND` since `AND` is also a logical operator
2. **SetOp structural mismatch**: v2 chains set ops inside `SelectStmt.set_op`, old AST wraps in top-level `Statement::SetOp` — lowering must restructure
3. **TableRef join flattening**: v2 uses nested `TableRef::Join`, old uses flat `from + joins: Vec<JoinClause>` — lowering must flatten
4. **Quoted identifiers**: Old parser preprocesses SQL before parsing; v2 should handle in lexer
5. **Variable vs Identifier**: Old AST uses `Expr::Identifier("@var")`, v2 has `Expr::Variable("@var")` — lowering must merge

use tsql_core::{parse_batch, parse_sql, types::Value, Engine};

fn exec(engine: &mut Engine, sql: &str) {
    engine.exec(sql).expect(sql);
}

fn query(engine: &mut Engine, sql: &str) -> tsql_core::QueryResult {
    engine.query(sql).expect(sql)
}

#[test]
fn parser_bracket_delimited_identifiers() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE [dbo].[my table] ([my column] INT, [another col] NVARCHAR(50))",
    );
    exec(
        &mut e,
        "INSERT INTO [dbo].[my table] ([my column], [another col]) VALUES (1, N'test')",
    );

    let r = query(
        &mut e,
        "SELECT [my column], [another col] FROM [dbo].[my table]",
    );
    assert_eq!(r.rows[0][0], Value::Int(1));
    assert_eq!(r.rows[0][1].to_string_value(), "test");
}

#[test]
fn parser_semicolon_separated_statements() {
    let stmts = parse_batch("SELECT 1; SELECT 2; SELECT 3").unwrap();
    assert_eq!(stmts.len(), 3);
}

#[test]
fn parser_go_separator() {
    let stmts = parse_batch("SELECT 1 GO SELECT 2 GO").unwrap();
    assert_eq!(stmts.len(), 2);
}

#[test]
fn parser_empty_statements_ignored() {
    let stmts = parse_batch("SELECT 1; ; ; SELECT 2").unwrap();
    assert_eq!(stmts.len(), 2);
}

#[test]
fn parser_line_comment_mid_statement() {
    let mut e = Engine::new();
    let r = query(&mut e, "SELECT 1 -- this is a comment\n + 2");
    assert_eq!(r.rows[0][0], Value::BigInt(3));
}

#[test]
fn parser_block_comment() {
    let mut e = Engine::new();
    let r = query(&mut e, "SELECT /* block comment */ 1 + /* another */ 2");
    assert_eq!(r.rows[0][0], Value::BigInt(3));
}

#[test]
fn parser_trailing_whitespace_and_newlines() {
    let stmt = parse_sql("SELECT 1   \n\n  \t  ");
    assert!(stmt.is_ok());
}

#[test]
fn parser_unicode_string_literal() {
    let mut e = Engine::new();
    let r = query(&mut e, "SELECT N'caf\u{e9}'");
    assert_eq!(r.rows[0][0].to_string_value(), "caf\u{e9}");
}

#[test]
fn parser_nested_parentheses() {
    let mut e = Engine::new();
    let r = query(&mut e, "SELECT ((1 + 2) * (3 + 4))");
    assert_eq!(r.rows[0][0], Value::BigInt(21));
}

#[test]
fn parser_case_insensitive_keywords() {
    let mut e = Engine::new();
    let r = query(&mut e, "sElEcT 1 + 2 aS result");
    assert_eq!(r.rows[0][0], Value::BigInt(3));
    assert_eq!(r.columns[0], "result");
}

#[test]
fn parser_string_with_escaped_quotes() {
    let mut e = Engine::new();
    let r = query(&mut e, "SELECT 'it''s a test'");
    assert_eq!(r.rows[0][0].to_string_value(), "it's a test");
}

#[test]
fn parser_select_with_column_alias() {
    let mut e = Engine::new();
    let r = query(&mut e, "SELECT 42 AS the_answer");
    assert_eq!(r.columns[0], "the_answer");
    assert_eq!(r.rows[0][0], Value::Int(42));
}

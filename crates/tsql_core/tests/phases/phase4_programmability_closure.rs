use tsql_core::{parse_batch, parse_sql, types::Value, DbError, Engine};

fn exec(engine: &mut Engine, sql: &str) {
    let stmt = parse_sql(sql).expect("parse failed");
    engine.execute(stmt).expect("execute failed");
}

fn exec_batch(engine: &mut Engine, sql: &str) {
    let stmts = parse_batch(sql).expect("parse batch failed");
    engine.execute_batch(stmts).expect("execute batch failed");
}

fn query(engine: &mut Engine, sql: &str) -> tsql_core::QueryResult {
    let stmt = parse_sql(sql).expect("parse failed");
    engine
        .execute(stmt)
        .expect("execute failed")
        .expect("expected result")
}

fn query_batch(engine: &mut Engine, sql: &str) -> tsql_core::QueryResult {
    let stmts = parse_batch(sql).expect("parse batch failed");
    let mut result = None;
    for stmt in stmts {
        result = engine.execute(stmt).expect("execute failed");
    }
    result.expect("expected result")
}

#[test]
fn test_select_var_assign_no_from() {
    let mut e = Engine::new();
    let r = query_batch(
        &mut e,
        "DECLARE @x INT = 1; SELECT @x = @x + 41; SELECT @x AS val",
    );
    assert_eq!(r.rows[0][0], Value::Int(42));
}

#[test]
fn test_select_var_assign_from_last_row_wins() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (v INT)");
    exec_batch(
        &mut e,
        "INSERT INTO t VALUES (10); INSERT INTO t VALUES (20); INSERT INTO t VALUES (30)",
    );
    let r = query_batch(
        &mut e,
        "DECLARE @x INT = 0; SELECT @x = v FROM t; SELECT @x AS val",
    );
    assert_eq!(r.rows[0][0], Value::Int(30));
}

#[test]
fn test_temp_table_persists_in_session() {
    let mut e = Engine::new();
    exec_batch(
        &mut e,
        "CREATE TABLE #tmp (v INT); INSERT INTO #tmp VALUES (1);",
    );
    let r = query(&mut e, "SELECT COUNT(*) AS cnt FROM #tmp");
    assert_eq!(r.rows[0][0], Value::BigInt(1));
}

#[test]
fn test_temp_table_isolated_between_engines() {
    let mut e1 = Engine::new();
    let e2 = Engine::new();
    exec_batch(
        &mut e1,
        "CREATE TABLE #tmp (v INT); INSERT INTO #tmp VALUES (1);",
    );
    let stmt = parse_sql("SELECT * FROM #tmp").unwrap();
    let err = e2.execute(stmt).unwrap_err();
    assert!(matches!(err, DbError::Semantic(_)));
}

#[test]
fn test_table_variable_basic() {
    let mut e = Engine::new();
    let r = query_batch(
        &mut e,
        "
        DECLARE @tv TABLE (v INT);
        INSERT INTO @tv VALUES (1);
        INSERT INTO @tv VALUES (2);
        SELECT COUNT(*) AS cnt FROM @tv
    ",
    );
    assert_eq!(r.rows[0][0], Value::BigInt(2));
}

#[test]
fn test_procedure_with_output_param() {
    let mut e = Engine::new();
    exec_batch(
        &mut e,
        "
        CREATE PROCEDURE dbo.bump @in INT, @out INT OUTPUT AS
        BEGIN
            SET @out = @in + 1;
            RETURN;
        END
    ",
    );
    let r = query_batch(
        &mut e,
        "DECLARE @x INT = 0; EXEC dbo.bump @in = 41, @out = @x OUTPUT; SELECT @x AS val",
    );
    assert_eq!(r.rows[0][0], Value::Int(42));
}

#[test]
fn test_scalar_udf_in_select() {
    let mut e = Engine::new();
    exec_batch(
        &mut e,
        "
        CREATE FUNCTION dbo.add1(@x INT) RETURNS INT AS
        BEGIN
            RETURN @x + 1;
        END
    ",
    );
    let r = query(&mut e, "SELECT dbo.add1(5) AS v");
    assert_eq!(r.rows[0][0], Value::BigInt(6));
}

#[test]
fn test_inline_tvf_in_from() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (v INT)");
    exec_batch(
        &mut e,
        "INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); INSERT INTO t VALUES (3)",
    );
    exec_batch(
        &mut e,
        "
        CREATE FUNCTION dbo.gt(@min INT) RETURNS TABLE AS
        RETURN (SELECT v FROM t WHERE v > @min)
    ",
    );
    let r = query(&mut e, "SELECT COUNT(*) AS cnt FROM dbo.gt(1)");
    assert_eq!(r.rows[0][0], Value::BigInt(2));
}

#[test]
fn test_sp_executesql_output() {
    let mut e = Engine::new();
    let r = query_batch(
        &mut e,
        "
        DECLARE @x INT = 5;
        EXEC sp_executesql N'SET @p = @p + 7', N'@p INT OUTPUT', @p = @x OUTPUT;
        SELECT @x AS val
    ",
    );
    assert_eq!(r.rows[0][0], Value::Int(12));
}

#[test]
fn test_identity_functions() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (id INT IDENTITY(1,1), v INT)");
    exec(&mut e, "INSERT INTO t (v) VALUES (10)");
    let r = query(
        &mut e,
        "SELECT SCOPE_IDENTITY() AS s, @@IDENTITY AS a, IDENT_CURRENT('t') AS c",
    );
    assert_eq!(r.rows[0][0], Value::BigInt(1));
    assert_eq!(r.rows[0][1], Value::BigInt(1));
    assert_eq!(r.rows[0][2], Value::BigInt(1));
}

#[test]
fn test_table_variable_physical_cleanup_on_scope_exit() {
    let mut e = Engine::new();
    exec_batch(
        &mut e,
        "
        BEGIN
            DECLARE @tv TABLE (v INT);
            INSERT INTO @tv VALUES (1);
        END
    ",
    );
    let r = query(
        &mut e,
        "SELECT COUNT(*) AS cnt FROM sys.tables WHERE name LIKE '__tablevar_%'",
    );
    assert_eq!(r.rows[0][0], Value::BigInt(0));
}

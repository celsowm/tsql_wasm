use tsql_core::{types::Value, Engine};

fn exec(engine: &mut Engine, sql: &str) {
    engine.exec(sql).expect(sql);
}

fn query(engine: &mut Engine, sql: &str) -> tsql_core::QueryResult {
    engine.query(sql).expect(sql)
}

#[test]
fn procedural_while_loop_sum() {
    let mut e = Engine::new();
    exec(&mut e, "DECLARE @sum INT = 0, @i INT = 1");
    exec(
        &mut e,
        "WHILE @i <= 10 BEGIN SET @sum = @sum + @i; SET @i = @i + 1 END",
    );
    let r = query(&mut e, "SELECT @sum");
    assert_eq!(r.rows[0][0], Value::Int(55));
}

#[test]
fn procedural_while_break() {
    let mut e = Engine::new();
    exec(&mut e, "DECLARE @sum INT = 0, @i INT = 1");
    exec(
        &mut e,
        "WHILE @i <= 100 BEGIN SET @sum = @sum + @i; IF @sum > 20 BREAK; SET @i = @i + 1 END",
    );
    let r = query(&mut e, "SELECT @sum");
    assert!(r.rows[0][0].to_integer_i64().unwrap() > 20);
}

#[test]
fn procedural_while_continue() {
    let mut e = Engine::new();
    exec(&mut e, "DECLARE @evens INT = 0, @i INT = 0");
    exec(
        &mut e,
        "WHILE @i < 10 BEGIN SET @i = @i + 1; IF @i % 2 <> 0 CONTINUE; SET @evens = @evens + 1 END",
    );
    let r = query(&mut e, "SELECT @evens");
    assert_eq!(r.rows[0][0], Value::Int(5));
}

#[test]
fn procedural_nested_begin_end() {
    let mut e = Engine::new();
    exec(&mut e, "DECLARE @x INT = 0");
    exec(
        &mut e,
        "BEGIN SET @x = 1; BEGIN SET @x = @x + 2; BEGIN SET @x = @x + 3 END END END",
    );
    let r = query(&mut e, "SELECT @x");
    assert_eq!(r.rows[0][0], Value::Int(6));
}

#[test]
fn procedural_raiserror_severity_handling() {
    let mut e = Engine::new();
    exec(&mut e, "RAISERROR('info msg', 10, 1)");
    assert_eq!(e.print_output()[0], "info msg");

    let err = e.exec("RAISERROR('error msg', 16, 1)");
    assert!(err.is_err(), "severity 16 should raise error");
}

#[test]
fn procedural_try_catch_with_error_message() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "BEGIN TRY RAISERROR('deliberate error', 16, 1) END TRY BEGIN CATCH PRINT ERROR_MESSAGE() END CATCH",
    );
    let output = e.print_output();
    assert!(!output.is_empty(), "CATCH should print error message");
}

#[test]
fn procedural_proc_output_param() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE PROCEDURE dbo.swap @a INT OUTPUT, @b INT OUTPUT AS BEGIN DECLARE @tmp INT = @a; SET @a = @b; SET @b = @tmp END",
    );
    exec(&mut e, "DECLARE @x INT = 10, @y INT = 20");
    let _ = e.query("EXEC dbo.swap @a = @x OUTPUT, @b = @y OUTPUT");

    let r = query(&mut e, "SELECT @x, @y");
    assert_eq!(r.rows[0][0], Value::Int(20));
    assert_eq!(r.rows[0][1], Value::Int(10));
}

#[test]
fn procedural_if_else_branching() {
    let mut e = Engine::new();
    exec(&mut e, "DECLARE @result NVARCHAR(10)");
    exec(
        &mut e,
        "IF 1 > 2 SET @result = N'false' ELSE SET @result = N'true'",
    );
    let r = query(&mut e, "SELECT @result");
    assert_eq!(r.rows[0][0].to_string_value(), "true");
}

#[test]
fn procedural_declare_multiple_variables() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "DECLARE @a INT = 1, @b INT = 2, @c NVARCHAR(10) = N'hello'",
    );
    let r = query(&mut e, "SELECT @a + @b, @c");
    assert_eq!(r.rows[0][0], Value::BigInt(3));
    assert_eq!(r.rows[0][1].to_string_value(), "hello");
}

#[test]
fn procedural_select_into_variable() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE dbo.tmp (val INT)");
    exec(&mut e, "INSERT INTO dbo.tmp VALUES (42)");

    exec(&mut e, "DECLARE @v INT");
    exec(&mut e, "SELECT @v = val FROM dbo.tmp");

    let r = query(&mut e, "SELECT @v");
    assert_eq!(r.rows[0][0], Value::Int(42));
}

#[test]
fn procedural_while_with_insert_accumulation() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE dbo.accumulator (n INT)");
    exec(&mut e, "DECLARE @i INT = 1");
    exec(
        &mut e,
        "WHILE @i <= 5 BEGIN INSERT INTO dbo.accumulator VALUES (@i); SET @i = @i + 1 END",
    );

    let r = query(&mut e, "SELECT SUM(n) FROM dbo.accumulator");
    assert_eq!(r.rows[0][0], Value::BigInt(15));
}

#[test]
fn procedural_nested_while() {
    let mut e = Engine::new();
    exec(&mut e, "DECLARE @total INT = 0, @i INT = 1");
    exec(
        &mut e,
        "WHILE @i <= 3 BEGIN DECLARE @j INT = 1; WHILE @j <= 3 BEGIN SET @total = @total + 1; SET @j = @j + 1 END; SET @i = @i + 1 END",
    );
    let r = query(&mut e, "SELECT @total");
    assert_eq!(r.rows[0][0], Value::Int(9));
}

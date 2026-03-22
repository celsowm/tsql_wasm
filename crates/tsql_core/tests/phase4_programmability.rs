use tsql_core::{parse_batch, parse_sql, types::Value, Engine};

fn exec(engine: &mut Engine, sql: &str) {
    let stmt = parse_sql(sql).expect("parse failed");
    engine.execute(stmt).expect("execute failed");
}

fn exec_batch(engine: &mut Engine, sql: &str) {
    let stmts = parse_batch(sql).expect("parse batch failed");
    engine.execute_batch(stmts).ok();
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

// ─── DECLARE and SET ───────────────────────────────────────────────────

#[test]
fn test_declare_and_set() {
    let mut e = Engine::new();
    exec_batch(&mut e, "DECLARE @x INT; SET @x = 42");
    let r = query_batch(&mut e, "DECLARE @x INT; SET @x = 42; SELECT @x AS val");
    assert_eq!(r.rows[0][0], Value::Int(42));
}

#[test]
fn test_declare_with_default() {
    let mut e = Engine::new();
    let r = query_batch(&mut e, "DECLARE @x INT = 10; SELECT @x AS val");
    assert_eq!(r.rows[0][0], Value::Int(10));
}

#[test]
fn test_set_arithmetic() {
    let mut e = Engine::new();
    let r = query_batch(
        &mut e,
        "DECLARE @x INT = 10; SET @x = @x * 2 + 5; SELECT @x AS val",
    );
    assert_eq!(r.rows[0][0], Value::Int(25));
}

#[test]
fn test_declare_string() {
    let mut e = Engine::new();
    let r = query_batch(
        &mut e,
        "DECLARE @name VARCHAR(50) = 'Alice'; SELECT @name AS val",
    );
    assert_eq!(r.rows[0][0], Value::VarChar("Alice".to_string()));
}

#[test]
fn test_multiple_variables() {
    let mut e = Engine::new();
    let r = query_batch(
        &mut e,
        "DECLARE @a INT = 3; DECLARE @b INT = 4; SELECT @a + @b AS val",
    );
    assert_eq!(r.rows[0][0], Value::BigInt(7));
}

// ─── IF / ELSE ─────────────────────────────────────────────────────────

#[test]
fn test_if_true() {
    let mut e = Engine::new();
    let r = query_batch(
        &mut e,
        "DECLARE @x INT = 10; IF @x > 5 BEGIN SELECT 'big' AS val END",
    );
    assert_eq!(r.rows[0][0], Value::VarChar("big".to_string()));
}

#[test]
fn test_while_loop() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (v INT)");
    let r = query_batch(
        &mut e,
        "
        DECLARE @i INT = 1;
        WHILE @i <= 5
        BEGIN
            INSERT INTO t (v) VALUES (@i);
            SET @i = @i + 1;
        END;
        SELECT COUNT(*) AS cnt FROM t
    ",
    );
    assert_eq!(r.rows[0][0], Value::BigInt(5));
}

#[test]
fn test_if_else_chain() {
    let mut e = Engine::new();
    let r = query_batch(&mut e, "DECLARE @result VARCHAR(10) = 'none'; IF 1 = 1 BEGIN SET @result = 'yes' END ELSE BEGIN SET @result = 'no' END; SELECT @result AS val");
    assert_eq!(r.rows[0][0], Value::VarChar("yes".to_string()));
}

// ─── WHILE ─────────────────────────────────────────────────────────────

#[test]
fn test_while_sum() {
    let mut e = Engine::new();
    let r = query_batch(
        &mut e,
        "
        DECLARE @sum INT = 0;
        DECLARE @i INT = 1;
        WHILE @i <= 10
        BEGIN
            SET @sum = @sum + @i;
            SET @i = @i + 1;
        END;
        SELECT @sum AS total
    ",
    );
    assert_eq!(r.rows[0][0], Value::Int(55));
}

// ─── Batch execution ───────────────────────────────────────────────────

#[test]
fn test_semicolon_separated() {
    let mut e = Engine::new();
    exec_batch(&mut e, "CREATE TABLE t (id INT); INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); INSERT INTO t VALUES (3)");
    let r = query(&mut e, "SELECT COUNT(*) AS cnt FROM t");
    assert_eq!(r.rows[0][0], Value::BigInt(3));
}

#[test]
fn test_batch_with_select() {
    let mut e = Engine::new();
    let r = query_batch(
        &mut e,
        "CREATE TABLE t (name VARCHAR(50)); INSERT INTO t VALUES ('hello'); SELECT name FROM t",
    );
    assert_eq!(r.rows[0][0], Value::VarChar("hello".to_string()));
}

// ─── Variable in query ─────────────────────────────────────────────────

#[test]
fn test_variable_in_where() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (v INT)");
    exec(&mut e, "INSERT INTO t VALUES (1)");
    exec(&mut e, "INSERT INTO t VALUES (2)");
    exec(&mut e, "INSERT INTO t VALUES (3)");
    let r = query_batch(
        &mut e,
        "DECLARE @threshold INT = 2; SELECT v FROM t WHERE v > @threshold",
    );
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::Int(3));
}

#[test]
fn test_variable_concat() {
    let mut e = Engine::new();
    let r = query_batch(&mut e, "DECLARE @first VARCHAR(50) = 'Hello'; DECLARE @second VARCHAR(50) = ' World'; SELECT @first + @second AS greeting");
    assert_eq!(r.rows[0][0], Value::VarChar("Hello World".to_string()));
}

// ─── BREAK / CONTINUE ───────────────────────────────────────────────────

#[test]
fn test_while_with_break() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (v INT)");
    exec_batch(
        &mut e,
        "
        DECLARE @i INT = 1;
        WHILE @i <= 100
        BEGIN
            IF @i > 5
            BEGIN
                BREAK;
            END
            INSERT INTO t VALUES (@i);
            SET @i = @i + 1;
        END
    ",
    );
    let r = query(&mut e, "SELECT COUNT(*) AS cnt FROM t");
    assert_eq!(r.rows[0][0], Value::BigInt(5));
}

#[test]
fn test_while_with_continue() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (v INT)");
    exec_batch(
        &mut e,
        "
        DECLARE @i INT = 1;
        WHILE @i <= 5
        BEGIN
            SET @i = @i + 1;
            IF @i = 3
            BEGIN
                CONTINUE;
            END
            INSERT INTO t VALUES (@i);
        END
    ",
    );
    let r = query(&mut e, "SELECT COUNT(*) AS cnt FROM t");
    assert_eq!(r.rows[0][0], Value::BigInt(4));
}

#[test]
fn test_nested_while_break_inner() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (v INT)");
    exec_batch(
        &mut e,
        "
        DECLARE @i INT = 1;
        WHILE @i <= 3
        BEGIN
            DECLARE @j INT = 1;
            WHILE @j <= 10
            BEGIN
                IF @j > 2
                BEGIN
                    BREAK;
                END
                INSERT INTO t VALUES (@i * 100 + @j);
                SET @j = @j + 1;
            END;
            SET @i = @i + 1;
        END
    ",
    );
    let r = query(&mut e, "SELECT COUNT(*) AS cnt FROM t");
    assert_eq!(r.rows[0][0], Value::BigInt(6));
}

#[test]
fn test_return_value() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (v INT)");
    exec_batch(
        &mut e,
        "
        DECLARE @i INT = 1;
        WHILE @i <= 5
        BEGIN
            IF @i = 3
            BEGIN
                RETURN;
            END
            INSERT INTO t VALUES (@i);
            SET @i = @i + 1;
        END
    ",
    );
    let cnt = query(&mut e, "SELECT COUNT(*) AS cnt FROM t");
    assert_eq!(cnt.rows[0][0], Value::BigInt(2));
}

#[test]
fn test_return_with_value() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (v INT)");
    exec_batch(
        &mut e,
        "
        DECLARE @val INT = 42;
        IF @val > 0
        BEGIN
            RETURN @val;
        END
        INSERT INTO t VALUES (@val);
    ",
    );
    let cnt = query(&mut e, "SELECT COUNT(*) AS cnt FROM t");
    assert_eq!(cnt.rows[0][0], Value::BigInt(0));
}

#[test]
fn test_return_early() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (v INT)");
    exec_batch(
        &mut e,
        "
        DECLARE @x INT = 100;
        RETURN;
        SET @x = 200;
        INSERT INTO t VALUES (@x);
    ",
    );
    let cnt = query(&mut e, "SELECT COUNT(*) AS cnt FROM t");
    assert_eq!(cnt.rows[0][0], Value::BigInt(0));
}

#[test]
fn test_continue_skip_insert() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (v INT)");
    exec_batch(
        &mut e,
        "
        DECLARE @i INT = 1;
        WHILE @i <= 4
        BEGIN
            SET @i = @i + 1;
            CONTINUE;
            INSERT INTO t VALUES (@i);
        END
    ",
    );
    let cnt = query(&mut e, "SELECT COUNT(*) AS cnt FROM t");
    assert_eq!(cnt.rows[0][0], Value::BigInt(0));
}

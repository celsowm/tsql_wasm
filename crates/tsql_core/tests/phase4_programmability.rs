use tsql_core::{parse_batch, parse_sql, Engine};

fn exec(engine: &mut Engine, sql: &str) {
    let stmt = parse_sql(sql).expect("parse failed");
    engine.execute(stmt).expect("execute failed");
}

fn exec_batch(engine: &mut Engine, sql: &str) {
    let stmts = parse_batch(sql).expect("parse batch failed");
    for stmt in stmts {
        engine.execute(stmt).expect("execute failed");
    }
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
    assert_eq!(r.rows[0][0], serde_json::json!(42));
}

#[test]
fn test_declare_with_default() {
    let mut e = Engine::new();
    let r = query_batch(&mut e, "DECLARE @x INT = 10; SELECT @x AS val");
    assert_eq!(r.rows[0][0], serde_json::json!(10));
}

#[test]
fn test_set_arithmetic() {
    let mut e = Engine::new();
    let r = query_batch(
        &mut e,
        "DECLARE @x INT = 10; SET @x = @x * 2 + 5; SELECT @x AS val",
    );
    assert_eq!(r.rows[0][0], serde_json::json!(25));
}

#[test]
fn test_declare_string() {
    let mut e = Engine::new();
    let r = query_batch(
        &mut e,
        "DECLARE @name VARCHAR(50) = 'Alice'; SELECT @name AS val",
    );
    assert_eq!(r.rows[0][0], serde_json::json!("Alice"));
}

#[test]
fn test_multiple_variables() {
    let mut e = Engine::new();
    let r = query_batch(
        &mut e,
        "DECLARE @a INT = 3; DECLARE @b INT = 4; SELECT @a + @b AS val",
    );
    assert_eq!(r.rows[0][0], serde_json::json!(7));
}

// ─── IF / ELSE ─────────────────────────────────────────────────────────

#[test]
fn test_if_true() {
    let mut e = Engine::new();
    let r = query_batch(
        &mut e,
        "DECLARE @x INT = 10; IF @x > 5 BEGIN SELECT 'big' AS val END",
    );
    assert_eq!(r.rows[0][0], serde_json::json!("big"));
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
    assert_eq!(r.rows[0][0], serde_json::json!(5));
}

#[test]
fn test_if_else_chain() {
    let mut e = Engine::new();
    let r = query_batch(&mut e, "DECLARE @result VARCHAR(10) = 'none'; IF 1 = 1 BEGIN SET @result = 'yes' END ELSE BEGIN SET @result = 'no' END; SELECT @result AS val");
    assert_eq!(r.rows[0][0], serde_json::json!("yes"));
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
    assert_eq!(r.rows[0][0], serde_json::json!(55));
}

// ─── Batch execution ───────────────────────────────────────────────────

#[test]
fn test_semicolon_separated() {
    let mut e = Engine::new();
    exec_batch(&mut e, "CREATE TABLE t (id INT); INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); INSERT INTO t VALUES (3)");
    let r = query(&mut e, "SELECT COUNT(*) AS cnt FROM t");
    assert_eq!(r.rows[0][0], serde_json::json!(3));
}

#[test]
fn test_batch_with_select() {
    let mut e = Engine::new();
    let r = query_batch(
        &mut e,
        "CREATE TABLE t (name VARCHAR(50)); INSERT INTO t VALUES ('hello'); SELECT name FROM t",
    );
    assert_eq!(r.rows[0][0], serde_json::json!("hello"));
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
    assert_eq!(r.rows[0][0], serde_json::json!(3));
}

#[test]
fn test_variable_concat() {
    let mut e = Engine::new();
    let r = query_batch(&mut e, "DECLARE @first VARCHAR(50) = 'Hello'; DECLARE @second VARCHAR(50) = ' World'; SELECT @first + @second AS greeting");
    assert_eq!(r.rows[0][0], serde_json::json!("Hello World"));
}

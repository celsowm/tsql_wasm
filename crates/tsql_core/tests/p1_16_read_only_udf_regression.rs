use tsql_core::{parse_batch, types::Value, Engine};

fn exec_batch(engine: &mut Engine, sql: &str) {
    let stmts = parse_batch(sql).expect("parse batch failed");
    engine.execute_batch(stmts).expect("execute batch failed");
}

fn query_batch(engine: &mut Engine, sql: &str) -> tsql_core::QueryResult {
    let stmts = parse_batch(sql).expect("parse batch failed");
    let mut result = None;
    for stmt in stmts {
        result = engine.execute(stmt).expect("execute failed");
    }
    result.expect("expected result")
}

fn int_val(v: i64) -> Value {
    if v >= i32::MIN as i64 && v <= i32::MAX as i64 {
        Value::Int(v as i32)
    } else {
        Value::BigInt(v)
    }
}

fn count_val(v: i64) -> Value {
    Value::BigInt(v)
}

// ─── P1 #16 Regression Tests: Read-only UDF classification ──────────────
//
// These tests verify that is_read_only_statement() correctly classifies
// statements so that read-only UDFs execute via UnsafeCell (no cloning)
// while write UDFs clone catalog/storage for safety.
//
// If a write statement is misclassified as read-only, it would mutate
// shared catalog/storage through raw pointers — a data corruption bug.

#[test]
fn test_read_only_udf_select_returns_correct_result() {
    let mut e = Engine::new();
    exec_batch(
        &mut e,
        "
        CREATE FUNCTION dbo.read_only_fn() RETURNS INT AS
        BEGIN
            DECLARE @x INT = 42;
            RETURN @x;
        END
    ",
    );
    let r = query_batch(&mut e, "SELECT dbo.read_only_fn() AS val");
    assert_eq!(r.rows[0][0], int_val(42));
}

#[test]
fn test_udf_with_if_containing_select_is_read_only() {
    let mut e = Engine::new();
    exec_batch(
        &mut e,
        "
        CREATE FUNCTION dbo.if_select_fn(@flag INT) RETURNS INT AS
        BEGIN
            DECLARE @r INT = 200;
            IF @flag = 1
                SET @r = 100;
            RETURN @r;
        END
    ",
    );
    let r = query_batch(&mut e, "SELECT dbo.if_select_fn(1) AS val");
    assert_eq!(r.rows[0][0], int_val(100));
    let r = query_batch(&mut e, "SELECT dbo.if_select_fn(0) AS val");
    assert_eq!(r.rows[0][0], int_val(200));
}

#[test]
fn test_udf_with_while_containing_select_is_read_only() {
    let mut e = Engine::new();
    exec_batch(
        &mut e,
        "
        CREATE FUNCTION dbo.while_loop_fn(@n INT) RETURNS INT AS
        BEGIN
            DECLARE @sum INT = 0;
            DECLARE @i INT = 1;
            WHILE @i <= @n
            BEGIN
                SET @sum = @sum + @i;
                SET @i = @i + 1;
            END
            RETURN @sum;
        END
    ",
    );
    let r = query_batch(&mut e, "SELECT dbo.while_loop_fn(5) AS val");
    assert_eq!(r.rows[0][0], int_val(15));
}

#[test]
fn test_udf_with_try_catch_read_only_returns_correct_result() {
    let mut e = Engine::new();
    exec_batch(
        &mut e,
        "
        CREATE FUNCTION dbo.try_catch_fn() RETURNS INT AS
        BEGIN
            DECLARE @r INT = 99;
            BEGIN TRY
                SET @r = 42;
            END TRY
            BEGIN CATCH
                SET @r = -1;
            END CATCH
            RETURN @r;
        END
    ",
    );
    let r = query_batch(&mut e, "SELECT dbo.try_catch_fn() AS val");
    assert_eq!(r.rows[0][0], int_val(42));
}

// ─── Write UDFs must clone — verify isolation ───────────────────────────

#[test]
fn test_udf_with_insert_clones_catalog() {
    let mut e = Engine::new();
    exec_batch(
        &mut e,
        "
        CREATE TABLE dbo.data_table (id INT, val INT);
        INSERT INTO dbo.data_table VALUES (1, 10);

        CREATE FUNCTION dbo.write_udf() RETURNS INT AS
        BEGIN
            INSERT INTO dbo.data_table VALUES (99, 999);
            RETURN 1;
        END
    ",
    );
    let r = query_batch(&mut e, "SELECT dbo.write_udf() AS val");
    assert_eq!(r.rows[0][0], int_val(1));

    // The insert inside the UDF should NOT have affected the shared catalog
    // because write UDFs clone. If is_read_only_statement() is broken and
    // classifies this as read-only, the row (99, 999) would leak.
    let r = query_batch(&mut e, "SELECT COUNT(*) AS cnt FROM dbo.data_table");
    assert_eq!(r.rows[0][0], count_val(1));
}

#[test]
fn test_udf_with_update_clones_catalog() {
    let mut e = Engine::new();
    exec_batch(
        &mut e,
        "
        CREATE TABLE dbo.counter (id INT, val INT);
        INSERT INTO dbo.counter VALUES (1, 100);

        CREATE FUNCTION dbo.update_udf() RETURNS INT AS
        BEGIN
            UPDATE dbo.counter SET val = 999 WHERE id = 1;
            RETURN 1;
        END
    ",
    );
    let r = query_batch(&mut e, "SELECT dbo.update_udf() AS val");
    assert_eq!(r.rows[0][0], int_val(1));

    // The update inside the UDF should NOT have affected the shared catalog
    let r = query_batch(&mut e, "SELECT val FROM dbo.counter WHERE id = 1");
    assert_eq!(r.rows[0][0], Value::Int(100));
}

#[test]
fn test_udf_with_delete_clones_catalog() {
    let mut e = Engine::new();
    exec_batch(
        &mut e,
        "
        CREATE TABLE dbo.items (id INT);
        INSERT INTO dbo.items VALUES (1), (2), (3);

        CREATE FUNCTION dbo.delete_udf() RETURNS INT AS
        BEGIN
            DELETE FROM dbo.items WHERE id = 2;
            RETURN 1;
        END
    ",
    );
    let r = query_batch(&mut e, "SELECT dbo.delete_udf() AS val");
    assert_eq!(r.rows[0][0], int_val(1));

    // The delete inside the UDF should NOT have affected the shared catalog
    let r = query_batch(&mut e, "SELECT COUNT(*) AS cnt FROM dbo.items");
    assert_eq!(r.rows[0][0], count_val(3));
}

// ─── P1 #16 Regression: DeclareTableVar must NOT be classified as read-only ──

#[test]
fn test_udf_with_declare_table_var_clones_catalog() {
    let mut e = Engine::new();
    exec_batch(
        &mut e,
        "
        CREATE FUNCTION dbo.table_var_udf() RETURNS INT AS
        BEGIN
            DECLARE @tv TABLE (x INT);
            INSERT INTO @tv VALUES (1);
            RETURN (SELECT COUNT(*) FROM @tv);
        END
    ",
    );
    let r = query_batch(&mut e, "SELECT dbo.table_var_udf() AS val");
    assert_eq!(r.rows[0][0], count_val(1));

    // The table variable should not leak into the shared catalog
    let r = query_batch(
        &mut e,
        "SELECT COUNT(*) AS cnt FROM sys.tables WHERE name LIKE '__tablevar_%'",
    );
    assert_eq!(r.rows[0][0], count_val(0));
}

// ─── P1 #16 Regression: IF with write inside must NOT be read-only ──

#[test]
fn test_udf_with_if_containing_insert_clones_catalog() {
    let mut e = Engine::new();
    exec_batch(
        &mut e,
        "
        CREATE TABLE dbo.log_table (msg VARCHAR(100));

        CREATE FUNCTION dbo.if_write_udf(@flag INT) RETURNS INT AS
        BEGIN
            IF @flag = 1
                INSERT INTO dbo.log_table VALUES ('written');
            RETURN @flag;
        END
    ",
    );
    let r = query_batch(&mut e, "SELECT dbo.if_write_udf(1) AS val");
    assert_eq!(r.rows[0][0], int_val(1));

    // The insert inside IF should NOT have affected the shared catalog
    let r = query_batch(&mut e, "SELECT COUNT(*) AS cnt FROM dbo.log_table");
    assert_eq!(r.rows[0][0], count_val(0));
}

// ─── P1 #16 Regression: WHILE with write inside must NOT be read-only ──

#[test]
fn test_udf_with_while_containing_insert_clones_catalog() {
    let mut e = Engine::new();
    exec_batch(
        &mut e,
        "
        CREATE TABLE dbo.while_log (msg VARCHAR(100));

        CREATE FUNCTION dbo.while_write_udf() RETURNS INT AS
        BEGIN
            DECLARE @i INT = 0;
            WHILE @i < 3
            BEGIN
                INSERT INTO dbo.while_log VALUES ('loop');
                SET @i = @i + 1;
            END
            RETURN @i;
        END
    ",
    );
    let r = query_batch(&mut e, "SELECT dbo.while_write_udf() AS val");
    assert_eq!(r.rows[0][0], int_val(3));

    // The inserts inside WHILE should NOT have affected the shared catalog
    let r = query_batch(&mut e, "SELECT COUNT(*) AS cnt FROM dbo.while_log");
    assert_eq!(r.rows[0][0], count_val(0));
}

// ─── P1 #16 Regression: TRY/CATCH with write inside must NOT be read-only ──

#[test]
fn test_udf_with_try_catch_containing_insert_clones_catalog() {
    let mut e = Engine::new();
    exec_batch(
        &mut e,
        "
        CREATE TABLE dbo.try_log (msg VARCHAR(100));

        CREATE FUNCTION dbo.try_write_udf() RETURNS INT AS
        BEGIN
            BEGIN TRY
                INSERT INTO dbo.try_log VALUES ('try written');
            END TRY
            BEGIN CATCH
                INSERT INTO dbo.try_log VALUES ('catch written');
            END CATCH
            RETURN 1;
        END
    ",
    );
    let r = query_batch(&mut e, "SELECT dbo.try_write_udf() AS val");
    assert_eq!(r.rows[0][0], int_val(1));

    // The inserts inside TRY/CATCH should NOT have affected the shared catalog
    let r = query_batch(&mut e, "SELECT COUNT(*) AS cnt FROM dbo.try_log");
    assert_eq!(r.rows[0][0], count_val(0));
}

// ─── P1 #16 Regression: BEGIN/END with write inside must NOT be read-only ──

#[test]
fn test_udf_with_begin_end_containing_insert_clones_catalog() {
    let mut e = Engine::new();
    exec_batch(
        &mut e,
        "
        CREATE TABLE dbo.begin_log (msg VARCHAR(100));

        CREATE FUNCTION dbo.begin_write_udf() RETURNS INT AS
        BEGIN
            BEGIN
                INSERT INTO dbo.begin_log VALUES ('begin written');
            END
            RETURN 1;
        END
    ",
    );
    let r = query_batch(&mut e, "SELECT dbo.begin_write_udf() AS val");
    assert_eq!(r.rows[0][0], int_val(1));

    // The insert inside BEGIN/END should NOT have affected the shared catalog
    let r = query_batch(&mut e, "SELECT COUNT(*) AS cnt FROM dbo.begin_log");
    assert_eq!(r.rows[0][0], count_val(0));
}

// ─── P1 #16 Regression: Nested control flow with writes ──

#[test]
fn test_udf_with_nested_if_inside_while_clones_catalog() {
    let mut e = Engine::new();
    exec_batch(
        &mut e,
        "
        CREATE TABLE dbo.nested_log (msg VARCHAR(100));

        CREATE FUNCTION dbo.nested_write_udf() RETURNS INT AS
        BEGIN
            DECLARE @i INT = 0;
            WHILE @i < 2
            BEGIN
                IF @i = 0
                    INSERT INTO dbo.nested_log VALUES ('nested');
                SET @i = @i + 1;
            END
            RETURN @i;
        END
    ",
    );
    let r = query_batch(&mut e, "SELECT dbo.nested_write_udf() AS val");
    assert_eq!(r.rows[0][0], int_val(2));

    // The insert inside nested IF inside WHILE should NOT leak
    let r = query_batch(&mut e, "SELECT COUNT(*) AS cnt FROM dbo.nested_log");
    assert_eq!(r.rows[0][0], count_val(0));
}

#[test]
fn test_udf_with_nested_try_catch_inside_begin_end_clones_catalog() {
    let mut e = Engine::new();
    exec_batch(
        &mut e,
        "
        CREATE TABLE dbo.deep_log (msg VARCHAR(100));

        CREATE FUNCTION dbo.deep_write_udf() RETURNS INT AS
        BEGIN
            BEGIN
                BEGIN TRY
                    INSERT INTO dbo.deep_log VALUES ('deep');
                END TRY
                BEGIN CATCH
                    SELECT 1;
                END CATCH
            END
            RETURN 1;
        END
    ",
    );
    let r = query_batch(&mut e, "SELECT dbo.deep_write_udf() AS val");
    assert_eq!(r.rows[0][0], int_val(1));

    // The insert inside deeply nested TRY should NOT leak
    let r = query_batch(&mut e, "SELECT COUNT(*) AS cnt FROM dbo.deep_log");
    assert_eq!(r.rows[0][0], count_val(0));
}

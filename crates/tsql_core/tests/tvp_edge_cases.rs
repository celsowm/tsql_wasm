use tsql_core::{types::Value, Engine};

fn exec(engine: &mut Engine, sql: &str) {
    engine.exec(sql).expect(sql);
}

fn query(engine: &mut Engine, sql: &str) -> tsql_core::QueryResult {
    engine.query(sql).expect(sql)
}

#[test]
fn tvp_multi_column_procedure() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TYPE dbo.OrderItem AS TABLE (product_id INT, qty INT, price DECIMAL(10,2))",
    );
    exec(
        &mut e,
        "CREATE PROCEDURE dbo.calc_total @items dbo.OrderItem READONLY AS BEGIN SELECT SUM(qty * price) AS total FROM @items END",
    );
    exec(
        &mut e,
        "DECLARE @src TABLE (product_id INT, qty INT, price DECIMAL(10,2))",
    );
    exec(&mut e, "INSERT INTO @src VALUES (1, 2, 10.50)");
    exec(&mut e, "INSERT INTO @src VALUES (2, 1, 25.00)");

    let r = query(&mut e, "EXEC dbo.calc_total @items = @src");
    assert_eq!(r.rows.len(), 1);
}

#[test]
fn tvp_type_mismatch_wrong_column_count() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TYPE dbo.IntPair AS TABLE (a INT, b INT)");
    exec(
        &mut e,
        "CREATE PROCEDURE dbo.use_pair @p dbo.IntPair READONLY AS BEGIN SELECT COUNT(*) FROM @p END",
    );
    exec(&mut e, "DECLARE @src TABLE (x INT)");
    exec(&mut e, "INSERT INTO @src VALUES (1)");

    let err = e.exec("EXEC dbo.use_pair @p = @src");
    assert!(err.is_err(), "should fail on column count mismatch");
    let msg = err.unwrap_err().to_string();
    assert!(
        msg.to_uppercase().contains("TVP"),
        "error should mention TVP: {}",
        msg
    );
}

#[test]
fn tvp_type_mismatch_wrong_column_type() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TYPE dbo.IntList AS TABLE (id INT)");
    exec(
        &mut e,
        "CREATE PROCEDURE dbo.use_list @p dbo.IntList READONLY AS BEGIN SELECT COUNT(*) FROM @p END",
    );
    exec(&mut e, "DECLARE @src TABLE (id VARCHAR(50))");
    exec(&mut e, "INSERT INTO @src VALUES ('hello')");

    let err = e.exec("EXEC dbo.use_list @p = @src");
    assert!(err.is_err(), "should fail on column type mismatch");
    let msg = err.unwrap_err().to_string();
    assert!(
        msg.to_uppercase().contains("MISMATCH"),
        "error should mention mismatch: {}",
        msg
    );
}

#[test]
fn tvp_with_null_rows() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TYPE dbo.NullableList AS TABLE (val INT)");
    exec(
        &mut e,
        "CREATE PROCEDURE dbo.count_nulls @p dbo.NullableList READONLY AS BEGIN SELECT COUNT(*) AS total, COUNT(val) AS non_null FROM @p END",
    );
    exec(&mut e, "DECLARE @src TABLE (val INT)");
    exec(&mut e, "INSERT INTO @src VALUES (1)");
    exec(&mut e, "INSERT INTO @src VALUES (NULL)");
    exec(&mut e, "INSERT INTO @src VALUES (3)");

    let r = query(&mut e, "EXEC dbo.count_nulls @p = @src");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::BigInt(3));
    assert_eq!(r.rows[0][1], Value::BigInt(2));
}

#[test]
fn tvp_mixed_scalar_and_table_params() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TYPE dbo.IdList AS TABLE (id INT)");
    exec(
        &mut e,
        "CREATE PROCEDURE dbo.filter_items @threshold INT, @ids dbo.IdList READONLY AS BEGIN SELECT id FROM @ids WHERE id > @threshold END",
    );
    exec(&mut e, "DECLARE @src TABLE (id INT)");
    exec(&mut e, "INSERT INTO @src VALUES (1), (2), (3), (4)");

    let r = query(&mut e, "EXEC dbo.filter_items @threshold = 2, @ids = @src");
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Value::Int(3));
    assert_eq!(r.rows[1][0], Value::Int(4));
}

#[test]
fn tvp_with_varchar_columns() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TYPE dbo.NameList AS TABLE (first_name NVARCHAR(50), last_name NVARCHAR(50))",
    );
    exec(
        &mut e,
        "CREATE PROCEDURE dbo.concat_names @names dbo.NameList READONLY AS BEGIN SELECT first_name + ' ' + last_name AS full_name FROM @names END",
    );
    exec(
        &mut e,
        "DECLARE @src TABLE (first_name NVARCHAR(50), last_name NVARCHAR(50))",
    );
    exec(&mut e, "INSERT INTO @src VALUES (N'John', N'Doe')");
    exec(&mut e, "INSERT INTO @src VALUES (N'Jane', N'Smith')");

    let r = query(&mut e, "EXEC dbo.concat_names @names = @src");
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0].to_string_value(), "John Doe");
    assert_eq!(r.rows[1][0].to_string_value(), "Jane Smith");
}

#[test]
fn tvp_readonly_enforced_on_insert() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TYPE dbo.IntList AS TABLE (id INT)");
    exec(
        &mut e,
        "CREATE PROCEDURE dbo.modify_tvp @p dbo.IntList READONLY AS BEGIN INSERT INTO @p VALUES (99) END",
    );
    exec(&mut e, "DECLARE @src TABLE (id INT)");

    let err = e.exec("EXEC dbo.modify_tvp @p = @src");
    assert!(err.is_err(), "READONLY should prevent INSERT into TVP");
    let msg = err.unwrap_err().to_string();
    assert!(
        msg.to_uppercase().contains("READONLY"),
        "error should mention READONLY: {}",
        msg
    );
}

#[test]
fn tvp_readonly_enforced_on_delete() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TYPE dbo.IntList AS TABLE (id INT)");
    exec(
        &mut e,
        "CREATE PROCEDURE dbo.delete_tvp @p dbo.IntList READONLY AS BEGIN DELETE FROM @p WHERE id = 1 END",
    );
    exec(&mut e, "DECLARE @src TABLE (id INT)");
    exec(&mut e, "INSERT INTO @src VALUES (1)");

    let err = e.exec("EXEC dbo.delete_tvp @p = @src");
    assert!(err.is_err(), "READONLY should prevent DELETE from TVP");
    let msg = err.unwrap_err().to_string();
    assert!(
        msg.to_uppercase().contains("READONLY"),
        "error should mention READONLY: {}",
        msg
    );
}

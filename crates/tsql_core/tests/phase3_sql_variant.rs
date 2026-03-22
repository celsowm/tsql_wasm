use tsql_core::{parse_batch, parse_sql, Engine};

fn exec(engine: &mut Engine, sql: &str) {
    let stmt = parse_sql(sql).expect("parse failed");
    engine.execute(stmt).expect("execute failed");
}

fn query(engine: &mut Engine, sql: &str) -> tsql_core::QueryResult {
    let stmt = parse_sql(sql).expect("parse failed");
    engine
        .execute(stmt)
        .expect("execute failed")
        .expect("expected result")
}

#[test]
fn test_sql_variant_variable_and_cast() {
    let mut e = Engine::new();
    let batch = parse_batch("DECLARE @v SQL_VARIANT = 42; SELECT CAST(@v AS INT) AS v;")
        .expect("parse batch failed");
    let r = e
        .execute_batch(batch)
        .expect("execute batch failed")
        .expect("expected rowset");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0].to_string_value(), "42");
}

#[test]
fn test_sql_variant_column_roundtrip() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE dbo.t (v SQL_VARIANT)");
    exec(&mut e, "INSERT INTO dbo.t VALUES ('10')");
    exec(&mut e, "INSERT INTO dbo.t VALUES (20)");

    let r = query(
        &mut e,
        "SELECT CAST(v AS INT) AS iv FROM dbo.t ORDER BY CAST(v AS INT)",
    );
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0].to_string_value(), "10");
    assert_eq!(r.rows[1][0].to_string_value(), "20");
}

#[test]
fn test_sql_variant_in_sys_types() {
    let mut e = Engine::new();
    let r = query(
        &mut e,
        "SELECT COUNT(*) AS cnt FROM sys.types WHERE name = 'sql_variant'",
    );
    assert_eq!(r.rows[0][0].to_string_value(), "1");
}

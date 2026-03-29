use tsql_core::{parse_sql, Engine};

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
fn test_create_and_drop_index_catalog_only() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE dbo.t (id INT, name VARCHAR(50))");
    exec(&mut e, "CREATE INDEX dbo.ix_t_name ON dbo.t (name)");

    let r = query(
        &mut e,
        "SELECT COUNT(*) AS cnt FROM sys.indexes WHERE name = 'ix_t_name'",
    );
    assert_eq!(r.rows[0][0].to_string_value(), "1");

    exec(&mut e, "DROP INDEX dbo.ix_t_name ON dbo.t");
    let r2 = query(
        &mut e,
        "SELECT COUNT(*) AS cnt FROM sys.indexes WHERE name = 'ix_t_name'",
    );
    assert_eq!(r2.rows[0][0].to_string_value(), "0");
}

#[test]
fn test_named_default_constraint_table_level() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE dbo.t (id INT, val INT, CONSTRAINT DF_t_val DEFAULT 9 FOR val)",
    );
    exec(&mut e, "INSERT INTO dbo.t (id) VALUES (1)");
    let r = query(&mut e, "SELECT val FROM dbo.t");
    assert_eq!(r.rows[0][0].to_string_value(), "9");
}

#[test]
fn test_named_check_constraint_column_level() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE dbo.t (v INT CONSTRAINT CK_t_v CHECK (v > 0))",
    );
    let err = e
        .execute(parse_sql("INSERT INTO dbo.t VALUES (-1)").unwrap())
        .unwrap_err();
    assert!(err.to_string().contains("CK_t_v"));
}

#[test]
fn test_named_check_constraint_table_level() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE dbo.pairs (a INT, b INT, CONSTRAINT CK_pairs CHECK (a < b))",
    );
    let err = e
        .execute(parse_sql("INSERT INTO dbo.pairs VALUES (2, 1)").unwrap())
        .unwrap_err();
    assert!(err.to_string().contains("CK_pairs"));
}

#[test]
fn test_drop_table_removes_index_metadata() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE dbo.t (id INT, name VARCHAR(50))");
    exec(&mut e, "CREATE INDEX dbo.ix_t_name ON dbo.t (name)");
    exec(&mut e, "DROP TABLE dbo.t");
    let r = query(
        &mut e,
        "SELECT COUNT(*) AS cnt FROM sys.indexes WHERE name = 'ix_t_name'",
    );
    assert_eq!(r.rows[0][0].to_string_value(), "0");
}

#[test]
fn test_indexed_predicate_and_order_semantics() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE dbo.t (id INT, name VARCHAR(50))");
    exec(&mut e, "CREATE INDEX dbo.ix_t_id ON dbo.t (id)");
    exec(
        &mut e,
        "INSERT INTO dbo.t VALUES (3, 'c'), (1, 'a'), (2, 'b'), (4, 'd')",
    );
    let r = query(&mut e, "SELECT id FROM dbo.t WHERE id >= 2 ORDER BY id");
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][0].to_string_value(), "2");
    assert_eq!(r.rows[1][0].to_string_value(), "3");
    assert_eq!(r.rows[2][0].to_string_value(), "4");
}

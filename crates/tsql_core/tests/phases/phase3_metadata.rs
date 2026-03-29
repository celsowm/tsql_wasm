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
fn test_sys_schemas_and_tables() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE SCHEMA app");
    exec(&mut e, "CREATE TABLE app.users (id INT, name VARCHAR(50))");

    let schemas = query(&mut e, "SELECT name FROM sys.schemas WHERE name = 'app'");
    assert_eq!(schemas.rows.len(), 1);

    let tables = query(&mut e, "SELECT name FROM sys.tables WHERE name = 'users'");
    assert_eq!(tables.rows.len(), 1);
}

#[test]
fn test_sys_columns_and_information_schema() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE SCHEMA app");
    exec(
        &mut e,
        "CREATE TABLE app.users (id INT, name VARCHAR(50) NULL)",
    );

    let cols = query(
        &mut e,
        "SELECT name FROM sys.columns WHERE object_id = OBJECT_ID('app.users') ORDER BY name",
    );
    assert_eq!(cols.rows.len(), 2);

    let info_tables = query(
        &mut e,
        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'app' AND TABLE_NAME = 'users'",
    );
    assert_eq!(info_tables.rows.len(), 1);

    let info_cols = query(
        &mut e,
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'app' AND TABLE_NAME = 'users' ORDER BY COLUMN_NAME",
    );
    assert_eq!(info_cols.rows.len(), 2);
}

#[test]
fn test_object_id_and_sys_indexes() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE SCHEMA app");
    exec(&mut e, "CREATE TABLE app.users (id INT, name VARCHAR(50))");
    exec(&mut e, "CREATE INDEX app.ix_users_name ON app.users (name)");

    let oid = query(&mut e, "SELECT OBJECT_ID('app.users') AS oid");
    assert!(!oid.rows[0][0].is_null());

    let missing = query(&mut e, "SELECT OBJECT_ID('app.missing') AS oid");
    assert!(missing.rows[0][0].is_null());

    let indexes = query(
        &mut e,
        "SELECT name FROM sys.indexes WHERE object_id = OBJECT_ID('app.users')",
    );
    assert_eq!(indexes.rows.len(), 1);
    assert_eq!(indexes.rows[0][0].to_string_value(), "ix_users_name");
}

#[test]
fn test_columnproperty_basic() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE dbo.metrics (a INT NOT NULL, b INT NULL, total AS (a + b))",
    );
    let r = query(
        &mut e,
        "SELECT COLUMNPROPERTY(OBJECT_ID('dbo.metrics'), 'a', 'AllowsNull') AS a_null, COLUMNPROPERTY(OBJECT_ID('dbo.metrics'), 'total', 'IsComputed') AS total_comp, COLUMNPROPERTY(OBJECT_ID('dbo.metrics'), 'b', 'ColumnId') AS b_colid",
    );
    assert_eq!(r.rows[0][0].to_string_value(), "0");
    assert_eq!(r.rows[0][1].to_string_value(), "1");
    assert_eq!(r.rows[0][2].to_string_value(), "2");
}

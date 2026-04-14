use iridium_core::{types::Value, Engine};

fn exec(engine: &mut Engine, sql: &str) {
    engine.exec(sql).expect(sql);
}

fn query(engine: &mut Engine, sql: &str) -> iridium_core::QueryResult {
    engine.query(sql).expect(sql)
}

#[test]
fn metadata_sys_tables_count() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE dbo.test1 (id INT)");
    exec(&mut e, "CREATE TABLE dbo.test2 (id INT PRIMARY KEY)");
    exec(&mut e, "CREATE TABLE dbo.test3 (id INT, name NVARCHAR(50))");
    exec(&mut e, "CREATE TABLE dbo.test4 (id INT)");
    exec(&mut e, "CREATE TABLE dbo.test5 (id INT)");

    let r = query(
        &mut e,
        "SELECT COUNT(*) FROM sys.tables WHERE is_ms_shipped = 0",
    );
    assert_eq!(r.rows[0][0], Value::BigInt(5));
}

#[test]
fn metadata_sys_indexes_basic() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE dbo.idx_test (id INT PRIMARY KEY, name NVARCHAR(50))",
    );
    exec(&mut e, "CREATE INDEX idx_name ON dbo.idx_test (name)");

    let r = query(
        &mut e,
        "SELECT name, type FROM sys.indexes WHERE object_id = OBJECT_ID('dbo.idx_test')",
    );
    assert!(r.rows.len() >= 2);
}

#[test]
fn metadata_sys_foreign_keys() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE dbo.parent (id INT PRIMARY KEY)");
    exec(
        &mut e,
        "CREATE TABLE dbo.child (id INT, parent_id INT REFERENCES dbo.parent(id))",
    );

    let r = query(
        &mut e,
        "SELECT name FROM sys.foreign_keys WHERE parent_object_id = OBJECT_ID('dbo.child')",
    );
    assert_eq!(r.rows.len(), 1);
}

#[test]
fn metadata_information_schema_tables() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE dbo.is_test (id INT PRIMARY KEY, name NVARCHAR(100))",
    );

    let r = query(&mut e, "SELECT TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' ORDER BY TABLE_NAME");
    assert!(r
        .rows
        .iter()
        .any(|row| row[0].to_string_value() == "is_test"));
}

#[test]
fn metadata_information_schema_columns() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE dbo.isc_test (id INT PRIMARY KEY, name NVARCHAR(50) NOT NULL, age INT)",
    );

    let r = query(&mut e, "SELECT COLUMN_NAME, IS_NULLABLE, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'isc_test' ORDER BY ORDINAL_POSITION");
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][0].to_string_value(), "id");
    assert_eq!(r.rows[2][1].to_string_value(), "YES");
}

#[test]
fn metadata_information_schema_routines() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE PROCEDURE dbo.test_proc AS BEGIN SELECT 1 END",
    );

    let r = query(&mut e, "SELECT ROUTINE_NAME, ROUTINE_TYPE FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA = 'dbo'");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0].to_string_value(), "test_proc");
}

#[test]
fn metadata_sys_procedures() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE PROCEDURE dbo.meta_test AS SELECT 1");

    let r = query(
        &mut e,
        "SELECT name FROM sys.procedures WHERE name = 'meta_test'",
    );
    assert_eq!(r.rows.len(), 1);
}

#[test]
fn metadata_schema_counts() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE dbo.sc1 (id INT)");
    exec(&mut e, "CREATE TABLE dbo.sc2 (id INT)");
    exec(&mut e, "CREATE SCHEMA alt");
    exec(&mut e, "CREATE TABLE alt.sc3 (id INT)");

    let r = query(
        &mut e,
        "SELECT TABLE_SCHEMA, COUNT(*) as cnt FROM INFORMATION_SCHEMA.TABLES GROUP BY TABLE_SCHEMA",
    );
    assert!(r.rows.len() >= 2);
}

#[test]
fn metadata_sys_routines_fn() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE FUNCTION dbo.fn_test() RETURNS INT AS BEGIN RETURN 1 END",
    );

    let r = query(
        &mut e,
        "SELECT name FROM sys.routines WHERE name = 'fn_test'",
    );
    assert_eq!(r.rows.len(), 1);
}

#[test]
fn metadata_schema_id() {
    let mut e = Engine::new();
    let r = query(&mut e, "SELECT SCHEMA_ID('dbo')");
    assert!(!r.rows[0][0].is_null());
}

#[test]
fn metadata_type_id() {
    let mut e = Engine::new();
    let r = query(&mut e, "SELECT TYPE_ID('int')");
    assert!(!r.rows[0][0].is_null());
    assert_eq!(r.rows[0][0].to_integer_i64().unwrap(), 56);
}

#[test]
fn metadata_sys_all_objects() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE dbo.allobj (id INT)");

    let r = query(
        &mut e,
        "SELECT name FROM sys.all_objects WHERE name = 'allobj'",
    );
    assert_eq!(r.rows.len(), 1);
}

#[test]
fn metadata_database_principals() {
    let mut e = Engine::new();
    let r = query(&mut e, "SELECT name, type FROM sys.database_principals");
    assert!(r.rows.len() > 0);
}

#[test]
fn metadata_object_id() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE dbo.objtest (id INT)");

    let r = query(&mut e, "SELECT OBJECT_ID('dbo.objtest')");
    assert!(!r.rows[0][0].is_null());
}

#[test]
fn metadata_key_constraints() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE dbo.pktest (id INT PRIMARY KEY)");

    let r = query(
        &mut e,
        "SELECT name FROM sys.key_constraints WHERE parent_object_id = OBJECT_ID('dbo.pktest')",
    );
    assert_eq!(r.rows.len(), 1);
}

#[test]
fn metadata_index_columns_count() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE dbo.idxc (id INT PRIMARY KEY, name NVARCHAR(50))",
    );
    exec(&mut e, "CREATE INDEX idx_name ON dbo.idxc (name)");

    let r = query(
        &mut e,
        "SELECT COUNT(*) FROM sys.index_columns WHERE object_id = OBJECT_ID('dbo.idxc')",
    );
    assert!(r.rows[0][0].to_integer_i64().unwrap() >= 2);
}


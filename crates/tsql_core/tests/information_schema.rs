use tsql_core::{parse_sql, Engine, QueryResult};

fn exec(engine: &mut Engine, sql: &str) {
    let stmt = parse_sql(sql).expect("parse failed");
    engine.execute(stmt).expect("execute failed");
}

fn query(engine: &mut Engine, sql: &str) -> QueryResult {
    let stmt = parse_sql(sql).expect("parse failed");
    engine
        .execute(stmt)
        .expect("execute failed")
        .expect("expected result")
}

fn val(r: &QueryResult, row: usize, col: usize) -> String {
    r.rows[row][col].to_string_value()
}

fn is_null(r: &QueryResult, row: usize, col: usize) -> bool {
    r.rows[row][col].is_null()
}

// ─── SCHEMATA ──────────────────────────────────────────────────────────

#[test]
fn test_schemata_default() {
    let mut e = Engine::new();
    let r = query(
        &mut e,
        "SELECT CATALOG_NAME, SCHEMA_NAME, SCHEMA_OWNER FROM INFORMATION_SCHEMA.SCHEMATA",
    );
    assert!(r.rows.len() >= 1);
    assert_eq!(val(&r, 0, 0), "tsql_wasm");
    assert_eq!(val(&r, 0, 1), "dbo");
    assert_eq!(val(&r, 0, 2), "dbo");
}

#[test]
fn test_schemata_with_custom_schema() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE SCHEMA sales");
    let r = query(
        &mut e,
        "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = 'sales'",
    );
    assert_eq!(r.rows.len(), 1);
    assert_eq!(val(&r, 0, 0), "sales");
}

// ─── TABLES ────────────────────────────────────────────────────────────

#[test]
fn test_tables_has_catalog() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t1 (id INT)");
    let r = query(&mut e, "SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 't1'");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(val(&r, 0, 0), "tsql_wasm");
    assert_eq!(val(&r, 0, 1), "dbo");
    assert_eq!(val(&r, 0, 2), "t1");
    assert_eq!(val(&r, 0, 3), "BASE TABLE");
}

#[test]
fn test_tables_includes_views() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t1 (id INT)");
    exec(&mut e, "CREATE VIEW v1 AS SELECT id FROM t1");
    let r = query(
        &mut e,
        "SELECT TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES ORDER BY TABLE_NAME",
    );
    assert_eq!(r.rows.len(), 2);
    assert_eq!(val(&r, 1, 0), "v1");
    assert_eq!(val(&r, 1, 1), "VIEW");
}

// ─── COLUMNS ───────────────────────────────────────────────────────────

#[test]
fn test_columns_full_columns() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE t1 (id INT NOT NULL, name VARCHAR(50) NULL, amount DECIMAL(10,2))",
    );
    let r = query(&mut e, "SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 't1' ORDER BY ORDINAL_POSITION");
    assert_eq!(r.rows.len(), 3);
    // id
    assert_eq!(val(&r, 0, 0), "tsql_wasm");
    assert_eq!(val(&r, 0, 3), "id");
    assert_eq!(val(&r, 0, 4), "1");
    assert_eq!(val(&r, 0, 5), "NO");
    assert_eq!(val(&r, 0, 6), "int");
    // name
    assert_eq!(val(&r, 1, 3), "name");
    assert_eq!(val(&r, 1, 5), "YES");
    assert_eq!(val(&r, 1, 6), "varchar");
    // amount
    assert_eq!(val(&r, 2, 3), "amount");
    assert_eq!(val(&r, 2, 6), "decimal");
}

#[test]
fn test_columns_char_max_length() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE t1 (a VARCHAR(100), b NVARCHAR(50), c INT)",
    );
    let r = query(&mut e, "SELECT COLUMN_NAME, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 't1' ORDER BY ORDINAL_POSITION");
    assert_eq!(val(&r, 0, 1), "100"); // VARCHAR(100) char max len
    assert_eq!(val(&r, 0, 2), "100"); // VARCHAR(100) octet len
    assert_eq!(val(&r, 1, 1), "50"); // NVARCHAR(50) char max len
    assert_eq!(val(&r, 1, 2), "100"); // NVARCHAR(50) octet len = 50*2
    assert!(is_null(&r, 2, 1)); // INT has no char max length
}

#[test]
fn test_columns_numeric_precision() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE t1 (a INT, b DECIMAL(8,3), c FLOAT, d VARCHAR(10))",
    );
    let r = query(&mut e, "SELECT COLUMN_NAME, NUMERIC_PRECISION, NUMERIC_PRECISION_RADIX, NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 't1' ORDER BY ORDINAL_POSITION");
    // INT
    assert_eq!(val(&r, 0, 1), "10"); // precision
    assert_eq!(val(&r, 0, 2), "10"); // radix
    assert_eq!(val(&r, 0, 3), "0"); // scale
                                    // DECIMAL(8,3)
    assert_eq!(val(&r, 1, 1), "8");
    assert_eq!(val(&r, 1, 2), "10");
    assert_eq!(val(&r, 1, 3), "3");
    // FLOAT
    assert_eq!(val(&r, 2, 1), "53");
    assert_eq!(val(&r, 2, 2), "2"); // binary radix
    assert!(is_null(&r, 2, 3)); // Float has no scale
                                // VARCHAR
    assert!(is_null(&r, 3, 1));
}

#[test]
fn test_columns_datetime_precision() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE t1 (a DATE, b DATETIME, c DATETIME2, d TIME)",
    );
    let r = query(&mut e, "SELECT COLUMN_NAME, DATETIME_PRECISION FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 't1' ORDER BY ORDINAL_POSITION");
    assert_eq!(val(&r, 0, 1), "0"); // DATE
    assert_eq!(val(&r, 1, 1), "3"); // DATETIME
    assert_eq!(val(&r, 2, 1), "7"); // DATETIME2
    assert_eq!(val(&r, 3, 1), "7"); // TIME
}

#[test]
fn test_columns_collation() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t1 (a VARCHAR(10), b INT)");
    let r = query(&mut e, "SELECT COLUMN_NAME, COLLATION_NAME, CHARACTER_SET_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 't1' ORDER BY ORDINAL_POSITION");
    assert_eq!(val(&r, 0, 1), "SQL_Latin1_General_CP1_CI_AS");
    assert_eq!(val(&r, 0, 2), "iso_1");
    assert!(is_null(&r, 1, 1)); // INT has no collation
    assert!(is_null(&r, 1, 2));
}

// ─── VIEWS ─────────────────────────────────────────────────────────────

#[test]
fn test_views() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t1 (id INT)");
    exec(&mut e, "CREATE VIEW v1 AS SELECT id FROM t1");
    let r = query(&mut e, "SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, CHECK_OPTION, IS_UPDATABLE FROM INFORMATION_SCHEMA.VIEWS");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(val(&r, 0, 0), "tsql_wasm");
    assert_eq!(val(&r, 0, 2), "v1");
    assert_eq!(val(&r, 0, 3), "NONE");
    assert_eq!(val(&r, 0, 4), "NO");
}

// ─── ROUTINES ──────────────────────────────────────────────────────────

#[test]
fn test_routines_procedure() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE PROCEDURE dbo.sp_test AS SELECT 1");
    let r = query(&mut e, "SELECT SPECIFIC_CATALOG, ROUTINE_SCHEMA, ROUTINE_NAME, ROUTINE_TYPE, ROUTINE_BODY, IS_DETERMINISTIC, SQL_DATA_ACCESS, SCHEMA_LEVEL_ROUTINE FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = 'sp_test'");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(val(&r, 0, 0), "tsql_wasm");
    assert_eq!(val(&r, 0, 1), "dbo");
    assert_eq!(val(&r, 0, 2), "sp_test");
    assert_eq!(val(&r, 0, 3), "PROCEDURE");
    assert_eq!(val(&r, 0, 4), "SQL");
    assert_eq!(val(&r, 0, 5), "NO");
    assert_eq!(val(&r, 0, 6), "MODIFIES");
    assert_eq!(val(&r, 0, 7), "YES");
}

#[test]
fn test_routines_function() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE FUNCTION dbo.fn_double(@x INT) RETURNS INT AS BEGIN RETURN @x * 2 END",
    );
    let r = query(&mut e, "SELECT ROUTINE_NAME, ROUTINE_TYPE, DATA_TYPE, SQL_DATA_ACCESS FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = 'fn_double'");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(val(&r, 0, 1), "FUNCTION");
    assert_eq!(val(&r, 0, 2), "int");
    assert_eq!(val(&r, 0, 3), "READS");
}

// ─── PARAMETERS ────────────────────────────────────────────────────────

#[test]
fn test_parameters() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE PROCEDURE dbo.sp_add @a INT, @b INT AS SELECT @a",
    );
    let r = query(&mut e, "SELECT SPECIFIC_NAME, ORDINAL_POSITION, PARAMETER_MODE, PARAMETER_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.PARAMETERS WHERE SPECIFIC_NAME = 'sp_add' ORDER BY ORDINAL_POSITION");
    assert_eq!(r.rows.len(), 2);
    assert_eq!(val(&r, 0, 0), "sp_add");
    assert_eq!(val(&r, 0, 1), "1");
    assert_eq!(val(&r, 0, 2), "IN");
    assert_eq!(val(&r, 0, 3), "@a");
    assert_eq!(val(&r, 0, 4), "int");
    assert_eq!(val(&r, 1, 3), "@b");
    assert_eq!(val(&r, 1, 4), "int");
}

// ─── TABLE_CONSTRAINTS ────────────────────────────────────────────────

#[test]
fn test_table_constraints_pk() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(50))",
    );
    let r = query(&mut e, "SELECT CONSTRAINT_CATALOG, CONSTRAINT_NAME, TABLE_NAME, CONSTRAINT_TYPE, IS_DEFERRABLE FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE TABLE_NAME = 't1' AND CONSTRAINT_TYPE = 'PRIMARY KEY'");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(val(&r, 0, 0), "tsql_wasm");
    assert_eq!(val(&r, 0, 1), "PK_t1");
    assert_eq!(val(&r, 0, 3), "PRIMARY KEY");
    assert_eq!(val(&r, 0, 4), "NO");
}

#[test]
fn test_table_constraints_check() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE t1 (id INT, age INT, CONSTRAINT CK_age CHECK (age > 0))",
    );
    let r = query(&mut e, "SELECT CONSTRAINT_NAME, CONSTRAINT_TYPE FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE TABLE_NAME = 't1' AND CONSTRAINT_TYPE = 'CHECK'");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(val(&r, 0, 0), "CK_age");
}

#[test]
fn test_table_constraints_fk() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE parent (id INT PRIMARY KEY)");
    exec(&mut e, "CREATE TABLE child (id INT, parent_id INT, CONSTRAINT FK_child_parent FOREIGN KEY (parent_id) REFERENCES parent(id))");
    let r = query(&mut e, "SELECT CONSTRAINT_NAME, CONSTRAINT_TYPE FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE TABLE_NAME = 'child' AND CONSTRAINT_TYPE = 'FOREIGN KEY'");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(val(&r, 0, 0), "FK_child_parent");
}

// ─── CHECK_CONSTRAINTS ────────────────────────────────────────────────

#[test]
fn test_check_constraints() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE t1 (id INT, age INT, CONSTRAINT CK_age CHECK (age > 0))",
    );
    let r = query(&mut e, "SELECT CONSTRAINT_CATALOG, CONSTRAINT_NAME FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS WHERE CONSTRAINT_NAME = 'CK_age'");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(val(&r, 0, 0), "tsql_wasm");
    assert_eq!(val(&r, 0, 1), "CK_age");
}

// ─── REFERENTIAL_CONSTRAINTS ──────────────────────────────────────────

#[test]
fn test_referential_constraints() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE parent (id INT PRIMARY KEY)");
    exec(&mut e, "CREATE TABLE child (id INT, pid INT, CONSTRAINT FK_cp FOREIGN KEY (pid) REFERENCES parent(id))");
    let r = query(&mut e, "SELECT CONSTRAINT_NAME, MATCH_OPTION, UPDATE_RULE, DELETE_RULE FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_NAME = 'FK_cp'");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(val(&r, 0, 1), "SIMPLE");
    assert_eq!(val(&r, 0, 2), "NO ACTION");
    assert_eq!(val(&r, 0, 3), "NO ACTION");
}

// ─── KEY_COLUMN_USAGE ─────────────────────────────────────────────────

#[test]
fn test_key_column_usage_pk() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(50))",
    );
    let r = query(&mut e, "SELECT CONSTRAINT_NAME, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME = 't1'");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(val(&r, 0, 0), "PK_t1");
    assert_eq!(val(&r, 0, 2), "id");
    assert_eq!(val(&r, 0, 3), "1");
}

// ─── CONSTRAINT_TABLE_USAGE ───────────────────────────────────────────

#[test]
fn test_constraint_table_usage() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE t1 (id INT PRIMARY KEY, age INT, CONSTRAINT CK_age CHECK (age > 0))",
    );
    let r = query(&mut e, "SELECT TABLE_NAME, CONSTRAINT_NAME FROM INFORMATION_SCHEMA.CONSTRAINT_TABLE_USAGE WHERE TABLE_NAME = 't1' ORDER BY CONSTRAINT_NAME");
    assert!(r.rows.len() >= 2); // PK + CHECK
}

// ─── CONSTRAINT_COLUMN_USAGE ──────────────────────────────────────────

#[test]
fn test_constraint_column_usage() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t1 (id INT PRIMARY KEY)");
    let r = query(&mut e, "SELECT TABLE_NAME, COLUMN_NAME, CONSTRAINT_NAME FROM INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE WHERE TABLE_NAME = 't1'");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(val(&r, 0, 1), "id");
    assert_eq!(val(&r, 0, 2), "PK_t1");
}

// ─── Empty views (should return 0 rows but not error) ─────────────────

#[test]
fn test_empty_views_queryable() {
    let mut e = Engine::new();
    for view in &[
        "COLUMN_DOMAIN_USAGE",
        "DOMAINS",
        "DOMAIN_CONSTRAINTS",
        "TABLE_PRIVILEGES",
        "COLUMN_PRIVILEGES",
        "VIEW_COLUMN_USAGE",
        "VIEW_TABLE_USAGE",
        "ROUTINE_COLUMNS",
    ] {
        let sql = format!("SELECT * FROM INFORMATION_SCHEMA.{}", view);
        let r = query(&mut e, &sql);
        assert_eq!(r.rows.len(), 0, "{} should return 0 rows", view);
    }
}

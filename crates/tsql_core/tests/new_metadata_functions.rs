include!("new_functions/helpers.rs");

#[test]
fn test_schema_object_and_column_metadata() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE SCHEMA app");
    exec(&mut engine, "CREATE TABLE app.users (id INT, name NVARCHAR(10), note VARCHAR(10))");

    let r = query(
        &mut engine,
        "SELECT \
            OBJECT_NAME(OBJECT_ID('app.users')) AS obj_name, \
            OBJECT_SCHEMA_NAME(OBJECT_ID('app.users')) AS schema_name, \
            SCHEMA_ID('app') AS schema_id, \
            SCHEMA_NAME(SCHEMA_ID('app')) AS schema_name_roundtrip, \
            COL_NAME(OBJECT_ID('app.users'), 2) AS col_name, \
            COL_LENGTH('app.users', 'id') AS id_len, \
            COL_LENGTH('app.users', 'name') AS name_len",
    );

    assert_eq!(r.rows[0][0], Value::NVarChar("users".to_string()));
    assert_eq!(r.rows[0][1], Value::NVarChar("app".to_string()));
    assert_eq!(r.rows[0][2], Value::Int(2));
    assert_eq!(r.rows[0][3], Value::NVarChar("app".to_string()));
    assert_eq!(r.rows[0][4], Value::NVarChar("name".to_string()));
    assert_eq!(r.rows[0][5], Value::Int(4));
    assert_eq!(r.rows[0][6], Value::Int(20));
}

#[test]
fn test_type_metadata() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TYPE dbo.IntList AS TABLE (id INT)");

    let r = query(
        &mut engine,
        "SELECT \
            TYPE_ID('int') AS int_id, \
            TYPE_NAME(56) AS int_name, \
            TYPE_ID('dbo.IntList') AS tvp_id, \
            TYPE_NAME(TYPE_ID('dbo.IntList')) AS tvp_name, \
            TYPEPROPERTY('int', 'Precision') AS int_precision, \
            TYPEPROPERTY('dbo.IntList', 'IsTableType') AS is_table_type",
    );

    assert_eq!(r.rows[0][0], Value::Int(56));
    assert_eq!(r.rows[0][1], Value::NVarChar("int".to_string()));
    assert!(matches!(r.rows[0][2], Value::Int(v) if v < 0));
    assert_eq!(r.rows[0][3], Value::NVarChar("dbo.IntList".to_string()));
    assert_eq!(r.rows[0][4], Value::Int(10));
    assert_eq!(r.rows[0][5], Value::Int(1));
}

#[test]
fn test_index_metadata() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE SCHEMA app");
    exec(&mut engine, "CREATE TABLE app.users (id INT, name NVARCHAR(10))");
    exec(&mut engine, "CREATE INDEX app.ix_users_name ON app.users (name)");

    let r = query(
        &mut engine,
        "SELECT \
            INDEX_COL(OBJECT_ID('app.users'), OBJECT_ID('app.ix_users_name'), 1) AS idx_col, \
            INDEXKEY_PROPERTY(OBJECT_ID('app.users'), OBJECT_ID('app.ix_users_name'), 1, 'ColumnId') AS key_col_id, \
            INDEXKEY_PROPERTY(OBJECT_ID('app.users'), OBJECT_ID('app.ix_users_name'), 1, 'IsDescending') AS key_desc, \
            INDEXPROPERTY(OBJECT_ID('app.users'), 'ix_users_name', 'IsUnique') AS is_unique, \
            OBJECTPROPERTYEX(OBJECT_ID('app.users'), 'TableHasIndex') AS has_index",
    );

    assert_eq!(r.rows[0][0], Value::NVarChar("name".to_string()));
    assert_eq!(r.rows[0][1], Value::Int(2));
    assert_eq!(r.rows[0][2], Value::Int(0));
    assert_eq!(r.rows[0][3], Value::Int(0));
    assert_eq!(r.rows[0][4], Value::Int(1));
}

#[test]
fn test_object_definition_and_properties() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE dbo.log (pid INT)");
    exec(
        &mut engine,
        "CREATE PROCEDURE dbo.capture_proc AS BEGIN INSERT INTO dbo.log SELECT @@PROCID; END",
    );
    exec(
        &mut engine,
        "CREATE FUNCTION dbo.capture_fn() RETURNS INT AS RETURN @@PROCID",
    );
    exec(
        &mut engine,
        "CREATE VIEW dbo.capture_view AS SELECT pid FROM dbo.log",
    );
    exec(&mut engine, "CREATE TABLE dbo.trg_src (id INT)");
    exec(
        &mut engine,
        "CREATE TRIGGER dbo.capture_trg ON dbo.trg_src AFTER INSERT AS BEGIN INSERT INTO dbo.log SELECT @@PROCID FROM inserted; END",
    );

    let defs = query(
        &mut engine,
        "SELECT \
            OBJECT_DEFINITION(OBJECT_ID('dbo.capture_proc')) AS proc_def, \
            OBJECT_DEFINITION(OBJECT_ID('dbo.capture_fn')) AS fn_def, \
            OBJECT_DEFINITION(OBJECT_ID('dbo.capture_view')) AS view_def, \
            OBJECT_DEFINITION(OBJECT_ID('dbo.capture_trg')) AS trg_def, \
            OBJECT_DEFINITION(OBJECT_ID('dbo.log')) AS table_def, \
            OBJECTPROPERTYEX(OBJECT_ID('dbo.log'), 'IsTable') AS is_table, \
            OBJECTPROPERTYEX(OBJECT_ID('dbo.capture_proc'), 'IsProcedure') AS is_proc, \
            OBJECTPROPERTYEX(OBJECT_ID('dbo.capture_proc'), 'TableHasIndex') AS has_index",
    );

    assert!(defs.rows[0][0].to_string_value().starts_with("CREATE PROCEDURE dbo.capture_proc"));
    assert!(defs.rows[0][1].to_string_value().starts_with("CREATE FUNCTION dbo.capture_fn"));
    assert!(defs.rows[0][2].to_string_value().starts_with("CREATE VIEW dbo.capture_view"));
    assert!(defs.rows[0][3].to_string_value().starts_with("CREATE TRIGGER dbo.capture_trg"));
    assert!(defs.rows[0][4].is_null());
    assert_eq!(defs.rows[0][5], Value::Int(1));
    assert_eq!(defs.rows[0][6], Value::Int(1));
    assert!(defs.rows[0][7].is_null());

    let outside = query(&mut engine, "SELECT @@PROCID AS pid");
    assert!(outside.rows[0][0].is_null());

    exec(&mut engine, "EXEC dbo.capture_proc");
    let proc_result = query(&mut engine, "SELECT pid FROM dbo.log");
    assert!(matches!(proc_result.rows[0][0], Value::Int(v) if v < 0));

    let fn_result = query(&mut engine, "SELECT dbo.capture_fn() AS pid");
    assert!(matches!(fn_result.rows[0][0], Value::Int(v) if v < 0));

    exec(&mut engine, "INSERT INTO dbo.trg_src VALUES (1)");
    let trg_result = query(&mut engine, "SELECT pid FROM dbo.log ORDER BY pid");
    assert!(trg_result.rows.iter().all(|row| matches!(row[0], Value::Int(v) if v < 0)));
}

#[test]
fn test_databasepropertyex_and_original_db() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT DATABASEPROPERTYEX('master', 'Status') AS status, DATABASEPROPERTYEX('master', 'Nope') AS missing, ORIGINAL_DB_NAME() AS original_db",
    );
    assert_eq!(r.rows[0][0], Value::NVarChar("ONLINE".to_string()));
    assert!(r.rows[0][1].is_null());
    assert_eq!(r.rows[0][2], Value::NVarChar("master".to_string()));
}

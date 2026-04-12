use tsql_core::executor::database::Engine;
use tsql_core::types::Value;

fn setup_engine() -> Engine {
    Engine::new()
}

fn exec(engine: &mut Engine, sql: &str) {
    engine.exec(sql).expect("Execution failed");
}

#[test]
fn test_sys_metadata_fidelity() {
    let mut engine = setup_engine();

    // 1. Setup metadata
    exec(&mut engine, "CREATE TYPE dbo.MyTableType AS TABLE (id INT, name VARCHAR(50))");
    exec(&mut engine, "CREATE PROCEDURE dbo.MyProc @p1 INT, @p2 VARCHAR(10) OUTPUT AS BEGIN SELECT @p1 END");
    exec(&mut engine, "CREATE FUNCTION dbo.MyFunc (@f1 INT) RETURNS INT AS BEGIN RETURN @f1 END");

    // 2. Check sys.procedures
    let res = engine.query("SELECT name FROM sys.procedures WHERE name = 'MyProc'").unwrap();
    assert_eq!(res.rows.len(), 1, "sys.procedures should contain MyProc");

    // 3. Check sys.functions
    let res = engine.query("SELECT name FROM sys.functions WHERE name = 'MyFunc'").unwrap();
    assert_eq!(res.rows.len(), 1, "sys.functions should contain MyFunc");

    // 4. Check sys.parameters
    let res = engine.query("SELECT name, parameter_id, is_output FROM sys.parameters WHERE object_id = OBJECT_ID('dbo.MyProc') ORDER BY parameter_id").unwrap();
    assert_eq!(res.rows.len(), 2, "sys.parameters should contain 2 parameters for MyProc");
    assert_eq!(res.rows[0][0], Value::VarChar("@p1".to_string()));
    assert_eq!(res.rows[0][2], Value::Bit(false));
    assert_eq!(res.rows[1][0], Value::VarChar("@p2".to_string()));
    assert_eq!(res.rows[1][2], Value::Bit(true));

    // 5. Check sys.table_types
    let res = engine.query("SELECT name FROM sys.table_types WHERE name = 'MyTableType'").unwrap();
    assert_eq!(res.rows.len(), 1, "sys.table_types should contain MyTableType");

    // 6. Check sys.types for table type
    let res = engine.query("SELECT name, is_user_defined FROM sys.types WHERE name = 'MyTableType'").unwrap();
    assert_eq!(res.rows.len(), 1, "sys.types should contain MyTableType");
    assert_eq!(res.rows[0][1], Value::Bit(true));

    // 7. Check sys.columns for table type
    let res = engine.query("SELECT name FROM sys.columns WHERE object_id = (SELECT type_table_object_id FROM sys.table_types WHERE name = 'MyTableType')").unwrap();
    assert_eq!(res.rows.len(), 2, "sys.columns should contain 2 columns for MyTableType");
}

#[test]
fn test_info_schema_fidelity() {
    let engine = Engine::new();
    engine.exec("CREATE TABLE dbo.InfoTable (id INT)").unwrap();

    // 1. Check ROUTINE_COLUMNS (exists, though empty for now)
    let res = engine.query("SELECT * FROM INFORMATION_SCHEMA.ROUTINE_COLUMNS").unwrap();
    assert_eq!(res.rows.len(), 0);

    // 2. Check TABLE_PRIVILEGES
    let res = engine.query("SELECT TABLE_NAME, PRIVILEGE_TYPE FROM INFORMATION_SCHEMA.TABLE_PRIVILEGES WHERE TABLE_NAME = 'InfoTable'").unwrap();
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0][1], Value::VarChar("SELECT".to_string()));

    // 3. Check COLUMN_PRIVILEGES
    let res = engine.query("SELECT COLUMN_NAME, PRIVILEGE_TYPE FROM INFORMATION_SCHEMA.COLUMN_PRIVILEGES WHERE TABLE_NAME = 'InfoTable'").unwrap();
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0][1], Value::VarChar("SELECT".to_string()));
}

#[test]
fn test_sys_types_fidelity() {
    let mut engine = Engine::new();
    // Verify system types are present
    let res = engine.query("SELECT name FROM sys.types WHERE name = 'int'").unwrap();
    assert_eq!(res.rows.len(), 1);

    // Verify user-defined table type is in sys.types
    engine.exec("CREATE TYPE dbo.TestType AS TABLE (id INT)").unwrap();
    let res = engine.query("SELECT name, is_user_defined, system_type_id FROM sys.types WHERE name = 'TestType'").unwrap();
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0][0], Value::VarChar("TestType".to_string()));
    assert_eq!(res.rows[0][1], Value::Bit(true));
    assert_eq!(res.rows[0][2], Value::TinyInt(243));
}

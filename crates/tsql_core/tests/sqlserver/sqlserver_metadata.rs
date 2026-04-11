use tsql_core::{parse_sql, types::Value, Engine};

fn engine_exec(engine: &mut Engine, sql: &str) -> Option<tsql_core::QueryResult> {
    let stmt = parse_sql(sql).unwrap_or_else(|_| panic!("Parser falhou: {}", sql));
    engine
        .execute(stmt)
        .unwrap_or_else(|_| panic!("Engine falhou: {}", sql))
}

fn exec(engine: &mut Engine, sql: &str) {
    let stmt = parse_sql(sql).expect("parse failed");
    engine.execute(stmt).expect("execute failed");
}

// ─── sys.tables ────────────────────────────────────────────────────────

#[test]
fn test_sys_tables_compare() {
    let mut engine = Engine::new();

    engine_exec(
        &mut engine,
        "CREATE TABLE t_meta1 (id INT, name VARCHAR(20))",
    );
    let _engine_result = engine_exec(
        &mut engine,
        "SELECT name FROM sys.tables WHERE name = 't_meta1'",
    )
    .unwrap();
    assert!(!_engine_result.rows.is_empty());
}

// ─── sys.columns ────────────────────────────────────────────────────────

#[test]
fn test_sys_columns_compare() {
    let mut engine = Engine::new();

    engine_exec(
        &mut engine,
        "CREATE TABLE t_meta2 (id INT, name VARCHAR(20), age INT)",
    );
    let _engine_result = engine_exec(
        &mut engine,
        "SELECT name FROM sys.columns WHERE object_id = OBJECT_ID('t_meta2') ORDER BY column_id",
    )
    .unwrap();
    assert!(!_engine_result.rows.is_empty());
}

// ─── OBJECT_ID function ────────────────────────────────────────────────

#[test]
fn test_object_id_compare() {
    let mut engine = Engine::new();

    engine_exec(&mut engine, "CREATE TABLE t_objid (id INT)");
    let _engine_result = engine_exec(&mut engine, "SELECT OBJECT_ID('t_objid')").unwrap();
}

#[test]
fn test_sys_tables_exposes_new_durability_columns() {
    let mut engine = Engine::new();

    exec(&mut engine, "CREATE TABLE dbo.regression_table (id INT)");

    let result = engine_exec(
        &mut engine,
        "SELECT name, is_memory_optimized, durability, durability_desc, history_table_id \
         FROM sys.tables WHERE name = 'regression_table'",
    )
    .unwrap();

    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0][0],
        Value::VarChar("regression_table".to_string())
    );
    assert_eq!(result.rows[0][1], Value::Bit(false));
    assert_eq!(result.rows[0][2], Value::TinyInt(0));
    assert_eq!(
        result.rows[0][3],
        Value::VarChar("SCHEMA_AND_DATA".to_string())
    );
    assert!(result.rows[0][4].is_null());
}

#[test]
fn test_sys_indexes_exposes_extended_columns() {
    let mut engine = Engine::new();

    exec(
        &mut engine,
        "CREATE TABLE dbo.regression_indexed (id INT, name VARCHAR(20))",
    );
    exec(
        &mut engine,
        "CREATE INDEX ix_regression_name ON dbo.regression_indexed (name)",
    );

    let result = engine_exec(
        &mut engine,
        "SELECT name, type_desc, is_unique, data_space_id, is_primary_key, \
                is_unique_constraint, is_hypothetical \
         FROM sys.indexes \
         WHERE object_id = OBJECT_ID('dbo.regression_indexed') \
         ORDER BY index_id",
    )
    .unwrap();

    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0][0],
        Value::VarChar("ix_regression_name".to_string())
    );
    assert_eq!(
        result.rows[0][1],
        Value::VarChar("NONCLUSTERED".to_string())
    );
    assert_eq!(result.rows[0][2], Value::Bit(false));
    assert_eq!(result.rows[0][3], Value::Int(1));
    assert_eq!(result.rows[0][4], Value::Bit(false));
    assert_eq!(result.rows[0][5], Value::Bit(false));
    assert_eq!(result.rows[0][6], Value::Bit(false));
}

#[test]
fn test_sys_master_files_lists_all_catalog_databases() {
    let mut engine = Engine::new();

    let result = engine_exec(
        &mut engine,
        "SELECT database_id, file_id, type_desc, name, physical_name, state_desc, size \
         FROM sys.master_files ORDER BY database_id",
    )
    .unwrap();

    assert_eq!(result.rows.len(), 5);
    assert_eq!(result.rows[0][0], Value::Int(1));
    assert_eq!(result.rows[0][1], Value::Int(1));
    assert_eq!(result.rows[0][2], Value::NVarChar("ROWS".to_string()));
    assert_eq!(result.rows[0][3], Value::NVarChar("db_1".to_string()));
    assert_eq!(
        result.rows[0][4],
        Value::NVarChar("C:\\data\\db_1.mdf".to_string())
    );
    assert_eq!(result.rows[0][5], Value::NVarChar("ONLINE".to_string()));
    assert_eq!(result.rows[0][6], Value::Int(1024));

    assert_eq!(result.rows[4][0], Value::Int(5));
    assert_eq!(result.rows[4][3], Value::NVarChar("db_5".to_string()));
}

#[test]
fn test_hadr_virtual_tables_are_present_and_stubbed() {
    let mut engine = Engine::new();

    let availability_replicas = engine_exec(
        &mut engine,
        "SELECT replica_id FROM sys.availability_replicas",
    )
    .unwrap();
    assert!(availability_replicas.rows.is_empty());

    let availability_groups =
        engine_exec(&mut engine, "SELECT group_id FROM sys.availability_groups").unwrap();
    assert!(availability_groups.rows.is_empty());

    let replica_states = engine_exec(
        &mut engine,
        "SELECT database_id FROM sys.dm_hadr_database_replica_states",
    )
    .unwrap();
    assert!(replica_states.rows.is_empty());

    let mirroring = engine_exec(
        &mut engine,
        "SELECT database_id, mirroring_state, mirroring_role_desc \
         FROM sys.database_mirroring ORDER BY database_id",
    )
    .unwrap();
    assert_eq!(mirroring.rows.len(), 5);
    assert_eq!(mirroring.rows[0][0], Value::Int(1));
    assert!(mirroring
        .rows
        .iter()
        .all(|row| row[1].is_null() && row[2].is_null()));
}

#[test]
fn test_sys_system_views_is_queryable_and_empty() {
    let mut engine = Engine::new();

    let result = engine_exec(&mut engine, "SELECT name FROM sys.system_views").unwrap();
    assert!(result.rows.is_empty());
}

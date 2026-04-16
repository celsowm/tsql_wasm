use iridium_core::executor::database::Engine;
use iridium_core::types::Value;

#[tokio::test]
async fn test_new_dmvs_presence() {
    let engine = Engine::new();
    let session = engine.create_session();

    // Test sys.dm_db_index_usage_stats
    let result = engine.execute_session_batch_sql(session, "SELECT COUNT(*) FROM sys.dm_db_index_usage_stats").unwrap().unwrap();
    assert_eq!(result.rows.len(), 1);

    // Test sys.dm_db_partition_stats
    let result = engine.execute_session_batch_sql(session, "SELECT COUNT(*) FROM sys.dm_db_partition_stats").unwrap().unwrap();
    assert_eq!(result.rows.len(), 1);

    // Test sys.dm_db_index_physical_stats
    let result = engine.execute_session_batch_sql(session, "SELECT COUNT(*) FROM sys.dm_db_index_physical_stats").unwrap().unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::BigInt(0));
}

#[tokio::test]
async fn test_set_ansi_defaults() {
    let engine = Engine::new();
    let session = engine.create_session();

    // Set ANSI_DEFAULTS ON
    engine.execute_session_batch_sql(session, "SET ANSI_DEFAULTS ON").unwrap();

    // Try to set it OFF
    engine.execute_session_batch_sql(session, "SET ANSI_DEFAULTS OFF").unwrap();
}

#[tokio::test]
async fn test_info_schema_domain_constraints() {
    let engine = Engine::new();
    let session = engine.create_session();

    let result = engine.execute_session_batch_sql(session, "SELECT COUNT(*) FROM INFORMATION_SCHEMA.DOMAIN_CONSTRAINTS").unwrap().unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::BigInt(0));
}

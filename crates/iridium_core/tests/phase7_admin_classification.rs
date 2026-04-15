use iridium_core::Engine;

#[test]
fn test_backup_unsupported() {
    let engine = Engine::new();
    let result = engine.exec("BACKUP DATABASE test TO DISK = 'test.bak'");
    assert!(result.is_err(), "BACKUP should fail");
}

#[test]
fn test_restore_unsupported() {
    let engine = Engine::new();
    let result = engine.exec("RESTORE DATABASE test FROM DISK = 'test.bak'");
    assert!(result.is_err(), "RESTORE should fail");
}

#[test]
fn test_create_assembly_unsupported() {
    let engine = Engine::new();
    let result = engine.exec("CREATE ASSEMBLY test FROM 'test.dll'");
    assert!(result.is_err(), "CREATE ASSEMBLY should fail");
}

#[test]
fn test_create_broker_unsupported() {
    let engine = Engine::new();
    let result = engine.exec("CREATE MESSAGE TYPE TestMessage");
    assert!(result.is_err(), "CREATE MESSAGE TYPE should fail");
}

#[test]
fn test_create_partition_function_unsupported() {
    let engine = Engine::new();
    let result =
        engine.exec("CREATE PARTITION FUNCTION pf(int) AS RANGE LEFT FOR VALUES (1, 100, 1000)");
    assert!(result.is_err(), "CREATE PARTITION FUNCTION should fail");
}

#[test]
fn test_sp_add_job_unsupported() {
    let engine = Engine::new();
    let result = engine.exec("EXEC sp_add_job @job_name = 'test'");
    assert!(result.is_err(), "sp_add_job should fail (SQL Agent)");
}

#[test]
fn test_sys_filegroups_exists() {
    let engine = Engine::new();
    let result = engine
        .query("SELECT name FROM sys.filegroups")
        .expect("query");
    assert!(result.columns.iter().any(|c| c == "name"));
}

#[test]
fn test_server_property_version() {
    let engine = Engine::new();
    let result = engine.query("SELECT @@VERSION AS version").expect("query");
    assert!(!result.rows[0][0].is_null());
}

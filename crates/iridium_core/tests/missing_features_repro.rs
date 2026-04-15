use iridium_core::executor::database::Engine;
use iridium_core::types::Value;

#[test]
fn test_missing_features() {
    let engine = Engine::new();

    // Test sys.dm_os_sys_info
    let result = engine.query("SELECT cpu_count, physical_memory_kb FROM sys.dm_os_sys_info");
    assert!(result.is_ok(), "sys.dm_os_sys_info should be available: {:?}", result.err());
    let res = result.unwrap();
    assert_eq!(res.rows.len(), 1);

    // Test sys.dm_exec_requests
    let result = engine.query("SELECT session_id, status, command FROM sys.dm_exec_requests");
    assert!(result.is_ok(), "sys.dm_exec_requests should be available: {:?}", result.err());
    let res = result.unwrap();
    assert_eq!(res.rows.len(), 1);

    // Test HOST_ID()
    let result = engine.query("SELECT HOST_ID()");
    assert!(result.is_ok(), "HOST_ID() should be available: {:?}", result.err());
    let res = result.unwrap();
    assert_eq!(res.rows.len(), 1);

    // Test SESSIONPROPERTY()
    let result = engine.query("SELECT SESSIONPROPERTY('ANSI_NULLS')");
    assert!(result.is_ok(), "SESSIONPROPERTY() should be available: {:?}", result.err());
    let res = result.unwrap();
    assert_eq!(res.rows.len(), 1);

    // Test specific values
    let result = engine.query("SELECT HOST_ID(), SESSIONPROPERTY('ANSI_NULLS'), SESSIONPROPERTY('QUOTED_IDENTIFIER')");
    let res = result.unwrap();
    assert_eq!(res.rows[0][0], Value::Int(12345));
    assert_eq!(res.rows[0][1], Value::Int(1)); // ANSI_NULLS is true by default
    assert_eq!(res.rows[0][2], Value::Int(1)); // QUOTED_IDENTIFIER is true by default

    // Test SET influence on SESSIONPROPERTY
    engine.exec("SET ANSI_NULLS OFF").unwrap();
    let result = engine.query("SELECT SESSIONPROPERTY('ANSI_NULLS')");
    let res = result.unwrap();
    assert_eq!(res.rows[0][0], Value::Int(0));
}

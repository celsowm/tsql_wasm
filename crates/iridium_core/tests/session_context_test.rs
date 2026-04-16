use iridium_core::Engine;
use iridium_core::types::Value;

#[test]
fn test_context_info() {
    let engine = Engine::new();

    // Initial value should be 128 bytes of zeros
    let res = engine.query("SELECT CONTEXT_INFO()").unwrap();
    let val = &res.rows[0][0];
    if let Value::VarBinary(b) = val {
        assert_eq!(b.len(), 128);
        assert!(b.iter().all(|&x| x == 0));
    } else {
        panic!("Expected VarBinary, got {:?}", val);
    }

    // Set CONTEXT_INFO with literal
    engine.exec("SET CONTEXT_INFO 0xABCDEF").unwrap();
    let res = engine.query("SELECT CONTEXT_INFO()").unwrap();
    if let Value::VarBinary(b) = &res.rows[0][0] {
        assert_eq!(b[0], 0xAB);
        assert_eq!(b[1], 0xCD);
        assert_eq!(b[2], 0xEF);
        assert_eq!(b[3], 0);
    }

    // Set CONTEXT_INFO with variable
    let session_id = engine.create_session();
    engine.execute_session_batch_sql(session_id, "DECLARE @ci VARBINARY(128) = 0x123456; SET CONTEXT_INFO @ci").unwrap();
    let res = engine.execute_session_batch_sql(session_id, "SELECT CONTEXT_INFO()").unwrap().unwrap();
    if let Value::VarBinary(b) = &res.rows[0][0] {
        assert_eq!(b[0], 0x12);
        assert_eq!(b[1], 0x34);
        assert_eq!(b[2], 0x56);
        assert_eq!(b[3], 0);
    }
}

#[test]
fn test_session_context() {
    let engine = Engine::new();

    // Initial value should be NULL
    let res = engine.query("SELECT SESSION_CONTEXT(N'user_id')").unwrap();
    assert_eq!(res.rows[0][0], Value::Null);

    // Set SESSION_CONTEXT
    // sp_set_session_context returns a result set (return status), so use query()
    engine.query("EXEC sp_set_session_context @key = N'user_id', @value = 42").unwrap();
    let res = engine.query("SELECT SESSION_CONTEXT(N'user_id')").unwrap();
    assert_eq!(res.rows[0][0], Value::Int(42));

    // Update SESSION_CONTEXT
    engine.query("EXEC sp_set_session_context N'user_id', 43").unwrap();
    let res = engine.query("SELECT SESSION_CONTEXT(N'user_id')").unwrap();
    assert_eq!(res.rows[0][0], Value::Int(43));

    // Set read-only SESSION_CONTEXT
    engine.query("EXEC sp_set_session_context N'app_name', N'Iridium', @read_only = 1").unwrap();
    let res = engine.query("SELECT SESSION_CONTEXT(N'app_name')").unwrap();
    assert_eq!(res.rows[0][0], Value::NVarChar("Iridium".to_string()));

    // Try to update read-only key (should fail)
    let err = engine.query("EXEC sp_set_session_context N'app_name', N'NewName'").unwrap_err();
    assert!(err.to_string().contains("read-only"));
}

#[test]
fn test_sys_dm_exec_sessions_context_info() {
    let engine = Engine::new();

    engine.exec("SET CONTEXT_INFO 0xDEADBEEF").unwrap();

    let res = engine.query("SELECT context_info FROM sys.dm_exec_sessions WHERE session_id = @@SPID").unwrap();
    if let Value::VarBinary(b) = &res.rows[0][0] {
        assert_eq!(b[0], 0xDE);
        assert_eq!(b[1], 0xAD);
        assert_eq!(b[2], 0xBE);
        assert_eq!(b[3], 0xEF);
    } else {
        panic!("Expected VarBinary, got {:?}", res.rows[0][0]);
    }
}

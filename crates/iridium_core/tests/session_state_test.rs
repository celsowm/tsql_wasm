use iridium_core::Engine;
use iridium_core::types::Value;

#[test]
fn test_context_info() {
    let engine = Engine::new();
    let session_id = engine.create_session();

    // Default is 128 zeros
    let res = engine.execute_session_batch_sql(session_id, "SELECT CONTEXT_INFO()").unwrap().unwrap();
    if let Value::VarBinary(bytes) = &res.rows[0][0] {
        assert_eq!(bytes.len(), 128);
        assert!(bytes.iter().all(|&b| b == 0));
    } else {
        panic!("Expected VarBinary");
    }

    // Set context info from string
    engine.execute_session_batch_sql(session_id, "SET CONTEXT_INFO 'hello'").unwrap();
    let res = engine.execute_session_batch_sql(session_id, "SELECT CONTEXT_INFO()").unwrap().unwrap();
    if let Value::VarBinary(bytes) = &res.rows[0][0] {
        assert_eq!(&bytes[0..5], b"hello");
        assert!(bytes[5..].iter().all(|&b| b == 0));
    } else {
        panic!("Expected VarBinary");
    }

    // Set context info from binary
    engine.execute_session_batch_sql(session_id, "SET CONTEXT_INFO 0xABCD").unwrap();
    let res = engine.execute_session_batch_sql(session_id, "SELECT CONTEXT_INFO()").unwrap().unwrap();
    if let Value::VarBinary(bytes) = &res.rows[0][0] {
        assert_eq!(bytes[0], 0xAB);
        assert_eq!(bytes[1], 0xCD);
        assert!(bytes[2..].iter().all(|&b| b == 0));
    } else {
        panic!("Expected VarBinary");
    }

    // sys.dm_exec_sessions exposure
    let res = engine.execute_session_batch_sql(session_id, "SELECT context_info FROM sys.dm_exec_sessions WHERE session_id = @@SPID").unwrap().unwrap();
    if let Value::VarBinary(bytes) = &res.rows[0][0] {
        assert_eq!(bytes[0], 0xAB);
    } else {
        panic!("Expected VarBinary in dm_exec_sessions");
    }
}

#[test]
fn test_session_context() {
    let engine = Engine::new();
    let session_id = engine.create_session();

    // Default is NULL
    let res = engine.execute_session_batch_sql(session_id, "SELECT SESSION_CONTEXT(N'user_id')").unwrap().unwrap();
    assert_eq!(res.rows[0][0], Value::Null);

    // Set session context
    engine.execute_session_batch_sql(session_id, "EXEC sp_set_session_context @key = N'user_id', @value = 42").unwrap();
    let res = engine.execute_session_batch_sql(session_id, "SELECT SESSION_CONTEXT(N'user_id')").unwrap().unwrap();
    assert_eq!(res.rows[0][0], Value::Int(42));

    // Update session context
    engine.execute_session_batch_sql(session_id, "EXEC sp_set_session_context @key = N'user_id', @value = 43").unwrap();
    let res = engine.execute_session_batch_sql(session_id, "SELECT SESSION_CONTEXT(N'user_id')").unwrap().unwrap();
    assert_eq!(res.rows[0][0], Value::Int(43));

    // Set as read-only
    engine.execute_session_batch_sql(session_id, "EXEC sp_set_session_context @key = N'app_name', @value = N'my_app', @read_only = 1").unwrap();
    let res = engine.execute_session_batch_sql(session_id, "SELECT SESSION_CONTEXT(N'app_name')").unwrap().unwrap();
    assert_eq!(res.rows[0][0], Value::NVarChar("my_app".into()));

    // Try to update read-only key
    let err = engine.execute_session_batch_sql(session_id, "EXEC sp_set_session_context @key = N'app_name', @value = N'evil_app'").unwrap_err();
    assert!(err.to_string().contains("read-only"));
}

#[test]
fn test_new_identity_functions() {
    let engine = Engine::new();
    let session_id = engine.create_session();

    let res = engine.execute_session_batch_sql(session_id, "SELECT SUSER_NAME(), SUSER_SID(), USER_SID()").unwrap().unwrap();
    assert_eq!(res.rows[0][0], Value::NVarChar("sa".into()));
    if let Value::VarBinary(sid) = &res.rows[0][1] {
        assert_eq!(sid.len(), 12);
    } else {
        panic!("Expected SUSER_SID() to be binary");
    }
    if let Value::VarBinary(sid) = &res.rows[0][2] {
        assert_eq!(sid.len(), 12);
    } else {
        panic!("Expected USER_SID() to be binary");
    }
}

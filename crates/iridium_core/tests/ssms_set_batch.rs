use iridium_core::Engine;

#[test]
fn ssms_chained_set_batch_is_accepted() {
    let engine = Engine::new();
    let session_id = engine.create_session();

    let sql = "SET ROWCOUNT 0 SET TEXTSIZE 2147483647 SET NOCOUNT OFF SET CONCAT_NULL_YIELDS_NULL ON SET ARITHABORT ON SET LOCK_TIMEOUT -1 SET QUERY_GOVERNOR_COST_LIMIT 0 SET DEADLOCK_PRIORITY NORMAL SET TRANSACTION ISOLATION LEVEL READ COMMITTED SET ANSI_NULLS ON SET ANSI_NULL_DFLT_ON ON SET ANSI_PADDING ON SET ANSI_WARNINGS ON SET CURSOR_CLOSE_ON_COMMIT OFF SET IMPLICIT_TRANSACTIONS OFF SET QUOTED_IDENTIFIER ON";

    let result = engine.execute_session_batch_sql(session_id, sql);
    assert!(
        result.is_ok(),
        "SSMS chained SET batch should be accepted, got: {:?}",
        result.err()
    );
}

#[test]
fn ssms_contained_auth_probe_batch_with_use_is_accepted() {
    let engine = Engine::new();
    let session_id = engine.create_session();

    let sql = "use [iridium_sql];if (db_id() = 1) begin select case when is_srvrolemember('sysadmin')=1 then 0 else 1 end end else begin exec('select case when authenticating_database_id = 1 then 0 else 1 end from sys.dm_exec_sessions where session_id = @@SPID') end;use [iridium_sql];";

    let result = engine.execute_session_batch_sql(session_id, sql);
    assert!(
        result.is_ok(),
        "SSMS contained-auth probe batch should be accepted, got: {:?}",
        result.err()
    );

    let current_db = engine.execute_session_batch_sql(
        session_id,
        "SELECT DB_NAME() AS current_db, DB_ID() AS current_db_id",
    );
    assert!(
        current_db.is_ok(),
        "Expected DB_NAME query to succeed after USE batch, got: {:?}",
        current_db.err()
    );
    let query_result = current_db.unwrap().expect("Expected query result");
    assert_eq!(query_result.rows.len(), 1);
    assert_eq!(
        query_result.rows[0][0],
        iridium_core::types::Value::NVarChar("iridium_sql".to_string())
    );
    assert_eq!(query_result.rows[0][1], iridium_core::types::Value::Int(5));
}

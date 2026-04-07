use tsql_core::Engine;

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

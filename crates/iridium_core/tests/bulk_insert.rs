use iridium_core::{Database, StatementExecutor};

#[test]
fn test_parse_bulk_insert() {
    let sql = "BULK INSERT MyTable FROM 'C:\\data\\file.csv' WITH (FIELDTERMINATOR = ',', ROWTERMINATOR = '\n')";
    let db = Database::new();
    let sid = db.create_session();

    // BULK INSERT is currently a shim/placeholder for server-side files,
    // but it should parse correctly.
    let res = db.execute_session_batch_sql(sid, sql);
    assert!(res.is_err());
    assert!(res.unwrap_err().to_string().contains("BULK INSERT from server-side file is not yet implemented"));
}

#[test]
fn test_parse_insert_bulk() {
    let sql = "INSERT BULK MyTable (Col1 INT, Col2 NVARCHAR(50))";
    let db = Database::new();
    let sid = db.create_session();

    // Create the table first
    db.execute_session_batch_sql(sid, "CREATE TABLE MyTable (Col1 INT, Col2 NVARCHAR(50))").unwrap();

    // INSERT BULK should execute and set bulk active flag (though we can't easily check the flag here)
    let res = db.execute_session_batch_sql(sid, sql).unwrap();
    assert!(res.is_none());

    let (active, target, _cols, _received_metadata) = db.get_bulk_load_state(sid);
    assert!(active);
    assert_eq!(target.as_ref().unwrap().name, "MyTable");
}

use tsql_core::Engine;

#[test]
fn test_raiserror_basic() {
    let mut engine = Engine::new();

    // Severity < 16: just print
    engine.execute("RAISERROR('hello', 10, 1)").unwrap();
    let output = engine.print_output();
    assert_eq!(output.len(), 1);
    assert_eq!(output[0], "hello");

    // Severity >= 16: error
    let res = engine.execute("RAISERROR('fatal', 16, 1)");
    assert!(res.is_err());
    assert_eq!(res.unwrap_err().to_string(), "Execution error: fatal");
}

#[test]
fn test_output_into_table_var() {
    let mut engine = Engine::new();
    engine.execute("CREATE TABLE dbo.Users (Id INT PRIMARY KEY, Name NVARCHAR(100))").unwrap();

    let batch = "
        DECLARE @out TABLE (Id INT, Name NVARCHAR(100));
        INSERT INTO dbo.Users (Id, Name)
        OUTPUT INSERTED.Id, INSERTED.Name INTO @out
        VALUES (1, 'Alice'), (2, 'Bob');
        SELECT * FROM @out;
    ";
    let result = engine.query(batch).unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][1].to_string(), "Alice");
    assert_eq!(result.rows[1][1].to_string(), "Bob");
}

#[test]
fn test_output_into_temp_table() {
    let mut engine = Engine::new();
    engine.execute("CREATE TABLE dbo.Source (Val INT)").unwrap();
    engine.execute("INSERT INTO dbo.Source VALUES (10), (20)").unwrap();
    engine.execute("CREATE TABLE #Audit (OldVal INT, NewVal INT)").unwrap();

    let sql = "
        UPDATE dbo.Source
        SET Val = Val + 5
        OUTPUT DELETED.Val, INSERTED.Val INTO #Audit;
    ";
    engine.execute(sql).unwrap();

    let result = engine.query("SELECT * FROM #Audit ORDER BY OldVal").unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][0].to_string(), "10");
    assert_eq!(result.rows[0][1].to_string(), "15");
}

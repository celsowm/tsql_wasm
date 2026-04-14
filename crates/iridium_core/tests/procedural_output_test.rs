use iridium_core::Engine;

#[test]
fn test_raiserror_severity() {
    let engine = Engine::new();

    // Low severity shouldn't abort
    engine.exec("RAISERROR('hello', 10, 1)").unwrap();
    assert_eq!(engine.print_output()[0], "hello");

    // High severity should abort
    let res = engine.exec("RAISERROR('fatal', 16, 1)");
    assert!(res.is_err());
}

#[test]
fn test_insert_output() {
    let engine = Engine::new();
    engine
        .exec("CREATE TABLE dbo.Users (Id INT PRIMARY KEY, Name NVARCHAR(100))")
        .unwrap();

    let sql = "
        INSERT INTO dbo.Users (Id, Name)
        OUTPUT INSERTED.Id, INSERTED.Name
        VALUES (1, 'Alice'), (2, 'Bob')
    ";

    let result = engine.query(sql).unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][1].to_string_value(), "Alice");
    assert_eq!(result.rows[1][1].to_string_value(), "Bob");
}

#[test]
fn test_update_output_into() {
    let engine = Engine::new();
    engine.exec("CREATE TABLE dbo.Source (Val INT)").unwrap();
    engine
        .exec("INSERT INTO dbo.Source VALUES (10), (20)")
        .unwrap();
    engine
        .exec("CREATE TABLE #Audit (OldVal INT, NewVal INT)")
        .unwrap();

    let sql = "
        UPDATE dbo.Source
        SET Val = Val + 5
        OUTPUT DELETED.Val, INSERTED.Val
        INTO #Audit
    ";

    engine.exec(sql).unwrap();

    let result = engine
        .query("SELECT OldVal, NewVal FROM #Audit ORDER BY OldVal")
        .unwrap();
    assert_eq!(result.rows[0][0].to_string_value(), "10");
    assert_eq!(result.rows[0][1].to_string_value(), "15");
}


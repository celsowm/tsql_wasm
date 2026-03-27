use tsql_core::Engine;

#[test]
fn test_merge_with_conditions() {
    let mut engine = Engine::new();
    engine.exec("CREATE TABLE dbo.Target (Id INT PRIMARY KEY, Val NVARCHAR(50), Status INT)").unwrap();
    engine.exec("CREATE TABLE dbo.Source (Id INT, Val NVARCHAR(50))").unwrap();

    engine.exec("INSERT INTO dbo.Target (Id, Val, Status) VALUES (1, 'Old', 0), (2, 'Keep', 1)").unwrap();
    engine.exec("INSERT INTO dbo.Source (Id, Val) VALUES (1, 'New'), (2, 'UpdateMe'), (3, 'BrandNew')").unwrap();

    // MERGE with conditions and multiple WHEN clauses
    let sql = "
        MERGE INTO dbo.Target AS T
        USING dbo.Source AS S
        ON T.Id = S.Id
        WHEN MATCHED AND T.Status = 0 THEN
            UPDATE SET T.Val = S.Val, T.Status = 1
        WHEN MATCHED AND T.Status = 1 THEN
            UPDATE SET T.Val = 'Updated'
        WHEN NOT MATCHED THEN
            INSERT (Id, Val, Status) VALUES (S.Id, S.Val, 2);
    ";

    engine.exec(sql).unwrap();

    let result = engine.query("SELECT Id, Val, Status FROM dbo.Target ORDER BY Id").unwrap();
    assert_eq!(result.rows.len(), 3);

    // Id 1: matched, Status was 0 -> Updated to 'New', Status 1
    assert_eq!(result.rows[0][1].to_string_value(), "New");
    assert_eq!(result.rows[0][2].to_string_value(), "1");

    // Id 2: matched, Status was 1 -> Updated to 'Updated', Status 1
    assert_eq!(result.rows[1][1].to_string_value(), "Updated");
    assert_eq!(result.rows[1][2].to_string_value(), "1");

    // Id 3: not matched -> Inserted 'BrandNew', Status 2
    assert_eq!(result.rows[2][0].to_string_value(), "3");
    assert_eq!(result.rows[2][1].to_string_value(), "BrandNew");
    assert_eq!(result.rows[2][2].to_string_value(), "2");
}

#[test]
fn test_update_from_multiple_joins() {
    let mut engine = Engine::new();
    engine.exec("CREATE TABLE dbo.A (Id INT, Val INT)").unwrap();
    engine.exec("CREATE TABLE dbo.B (Id INT, AId INT, Val INT)").unwrap();
    engine.exec("CREATE TABLE dbo.C (Id INT, BId INT, Val INT)").unwrap();

    engine.exec("INSERT INTO dbo.A (Id, Val) VALUES (1, 10)").unwrap();
    engine.exec("INSERT INTO dbo.B (Id, AId, Val) VALUES (1, 1, 20)").unwrap();
    engine.exec("INSERT INTO dbo.C (Id, BId, Val) VALUES (1, 1, 30)").unwrap();

    let sql = "
        UPDATE A
        SET Val = A.Val + B.Val + C.Val
        FROM dbo.A
        INNER JOIN dbo.B ON A.Id = B.AId
        INNER JOIN dbo.C ON B.Id = C.BId
    ";

    engine.exec(sql).unwrap();

    let val = engine.query("SELECT Val FROM dbo.A WHERE Id = 1").unwrap().rows[0][0].to_string_value();
    assert_eq!(val, "60"); // 10 + 20 + 30
}

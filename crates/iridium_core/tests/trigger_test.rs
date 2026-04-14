use iridium_core::Engine;

#[test]
fn test_after_insert_trigger() {
    let engine = Engine::new();
    engine
        .exec("CREATE TABLE dbo.Users (Id INT PRIMARY KEY, Name NVARCHAR(100))")
        .unwrap();
    engine
        .exec("CREATE TABLE dbo.Logs (Msg NVARCHAR(100))")
        .unwrap();

    let trigger_sql = "
        CREATE TRIGGER tr_Users_Insert
        ON dbo.Users
        AFTER INSERT
        AS
        BEGIN
            INSERT INTO dbo.Logs (Msg)
            SELECT 'User ' + Name + ' added' FROM INSERTED;
        END
    ";
    engine.exec(trigger_sql).unwrap();

    engine
        .exec("INSERT INTO dbo.Users (Id, Name) VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();

    let result = engine
        .query("SELECT Msg FROM dbo.Logs ORDER BY Msg")
        .unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][0].to_string_value(), "User Alice added");
    assert_eq!(result.rows[1][0].to_string_value(), "User Bob added");
}

#[test]
fn test_after_update_trigger() {
    let engine = Engine::new();
    engine
        .exec("CREATE TABLE dbo.Products (Id INT PRIMARY KEY, Price DECIMAL(10,2))")
        .unwrap();
    engine
        .exec("CREATE TABLE dbo.Audit (OldPrice DECIMAL(10,2), NewPrice DECIMAL(10,2))")
        .unwrap();

    engine
        .exec("INSERT INTO dbo.Products (Id, Price) VALUES (1, 10.00)")
        .unwrap();

    let trigger_sql = "
        CREATE TRIGGER tr_Products_Update
        ON dbo.Products
        AFTER UPDATE
        AS
        BEGIN
            INSERT INTO dbo.Audit (OldPrice, NewPrice)
            SELECT d.Price, i.Price
            FROM DELETED d
            JOIN INSERTED i ON d.Id = i.Id;
        END
    ";
    engine.exec(trigger_sql).unwrap();

    engine
        .exec("UPDATE dbo.Products SET Price = 12.50 WHERE Id = 1")
        .unwrap();

    let result = engine
        .query("SELECT OldPrice, NewPrice FROM dbo.Audit")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0].to_string_value(), "10.00");
    assert_eq!(result.rows[0][1].to_string_value(), "12.50");
}

#[test]
fn test_instead_of_insert_trigger() {
    let engine = Engine::new();
    engine
        .exec("CREATE TABLE dbo.Base (Id INT PRIMARY KEY, Val NVARCHAR(100))")
        .unwrap();
    engine
        .exec("CREATE TABLE dbo.Audit (Msg NVARCHAR(100))")
        .unwrap();

    let trigger_sql = "
        CREATE TRIGGER tr_Base_InsteadInsert
        ON dbo.Base
        INSTEAD OF INSERT
        AS
        BEGIN
            INSERT INTO dbo.Audit (Msg)
            SELECT 'Intercepted ' + Val FROM INSERTED;

            -- Manually insert with modification
            INSERT INTO dbo.Base (Id, Val)
            SELECT Id, Val + '_mod' FROM INSERTED;
        END
    ";
    engine.exec(trigger_sql).unwrap();

    engine
        .exec("INSERT INTO dbo.Base (Id, Val) VALUES (1, 'Hello')")
        .unwrap();

    let base = engine.query("SELECT Val FROM dbo.Base").unwrap();
    assert_eq!(base.rows[0][0].to_string_value(), "Hello_mod");

    let audit = engine.query("SELECT Msg FROM dbo.Audit").unwrap();
    assert_eq!(audit.rows[0][0].to_string_value(), "Intercepted Hello");
}

#[test]
fn test_qualified_inserted_reference() {
    let engine = Engine::new();
    engine
        .exec("CREATE TABLE dbo.T (Id INT PRIMARY KEY)")
        .unwrap();
    engine.exec("CREATE TABLE dbo.L (Id INT)").unwrap();

    let trigger_sql = "
        CREATE TRIGGER tr_T
        ON dbo.T
        AFTER INSERT
        AS
        BEGIN
            INSERT INTO dbo.L (Id)
            SELECT Id FROM dbo.INSERTED; -- Qualified reference
        END
    ";
    engine.exec(trigger_sql).unwrap();

    engine.exec("INSERT INTO dbo.T (Id) VALUES (42)").unwrap();
    let res = engine.query("SELECT Id FROM dbo.L").unwrap();
    assert_eq!(res.rows[0][0].to_string_value(), "42");
}

#[test]
fn test_recursive_trigger_prevention() {
    let engine = Engine::new();
    engine
        .exec("CREATE TABLE dbo.Rec (Id INT PRIMARY KEY, Val INT)")
        .unwrap();

    let trigger_sql = "
        CREATE TRIGGER tr_Rec
        ON dbo.Rec
        AFTER UPDATE
        AS
        BEGIN
            UPDATE dbo.Rec SET Val = Val + 1 WHERE Id IN (SELECT Id FROM INSERTED);
        END
    ";
    engine.exec(trigger_sql).unwrap();

    engine
        .exec("INSERT INTO dbo.Rec (Id, Val) VALUES (1, 10)")
        .unwrap();

    let res = engine.exec("UPDATE dbo.Rec SET Val = 20 WHERE Id = 1");
    // Should fail with nesting level error
    assert!(res.is_err());
    assert!(res
        .unwrap_err()
        .to_string()
        .contains("Maximum trigger nesting level"));
}


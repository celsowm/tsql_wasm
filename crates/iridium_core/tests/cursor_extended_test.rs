use iridium_core::Engine;

#[test]
fn test_cursor_extended_directions() {
    let engine = Engine::new();
    engine
        .exec("CREATE TABLE dbo.Items (Id INT PRIMARY KEY, Val NVARCHAR(10))")
        .unwrap();
    engine
        .exec("INSERT INTO dbo.Items VALUES (1, 'A'), (2, 'B'), (3, 'C'), (4, 'D')")
        .unwrap();

    let batch = "
        DECLARE @id INT;
        DECLARE @val NVARCHAR(10);
        DECLARE cur CURSOR FOR SELECT Id, Val FROM dbo.Items ORDER BY Id;
        OPEN cur;

        -- FETCH FIRST
        FETCH FIRST FROM cur INTO @id, @val;
        PRINT 'First: ' + CAST(@id AS NVARCHAR);

        -- FETCH NEXT
        FETCH NEXT FROM cur INTO @id, @val;
        PRINT 'Next: ' + CAST(@id AS NVARCHAR);

        -- FETCH LAST
        FETCH LAST FROM cur INTO @id, @val;
        PRINT 'Last: ' + CAST(@id AS NVARCHAR);

        -- FETCH PRIOR
        FETCH PRIOR FROM cur INTO @id, @val;
        PRINT 'Prior: ' + CAST(@id AS NVARCHAR);

        -- FETCH ABSOLUTE 2
        FETCH ABSOLUTE 2 FROM cur INTO @id, @val;
        PRINT 'Abs 2: ' + CAST(@id AS NVARCHAR);

        -- FETCH RELATIVE 2 (should be 4)
        FETCH RELATIVE 2 FROM cur INTO @id, @val;
        PRINT 'Rel 2 from 2: ' + CAST(@id AS NVARCHAR);

        CLOSE cur;
        DEALLOCATE cur;
    ";
    for stmt in iridium_core::parser::parse_batch(batch).unwrap() {
        engine.execute(stmt).unwrap();
    }
    let output = engine.print_output();
    assert_eq!(output[0], "First: 1");
    assert_eq!(output[1], "Next: 2");
    assert_eq!(output[2], "Last: 4");
    assert_eq!(output[3], "Prior: 3");
    assert_eq!(output[4], "Abs 2: 2");
    assert_eq!(output[5], "Rel 2 from 2: 4");
}

#[test]
fn test_cursor_boundaries() {
    let engine = Engine::new();
    engine
        .exec("CREATE TABLE dbo.Items (Id INT PRIMARY KEY)")
        .unwrap();
    engine
        .exec("INSERT INTO dbo.Items VALUES (1), (2)")
        .unwrap();

    let batch = "
        DECLARE @id INT;
        DECLARE cur CURSOR FOR SELECT Id FROM dbo.Items ORDER BY Id;
        OPEN cur;

        FETCH ABSOLUTE 10 FROM cur INTO @id;
        PRINT 'Status OOB: ' + CAST(@@FETCH_STATUS AS NVARCHAR);

        FETCH FIRST FROM cur INTO @id;
        PRINT 'Status First: ' + CAST(@@FETCH_STATUS AS NVARCHAR);

        CLOSE cur;
        DEALLOCATE cur;
    ";
    for stmt in iridium_core::parser::parse_batch(batch).unwrap() {
        engine.execute(stmt).unwrap();
    }
    let output = engine.print_output();
    assert_eq!(output[0], "Status OOB: -1");
    assert_eq!(output[1], "Status First: 0");
}

#[test]
fn test_cursor_close_on_commit_closes_open_cursor() {
    let engine = Engine::new();
    engine
        .exec("CREATE TABLE dbo.Items (Id INT PRIMARY KEY)")
        .unwrap();
    engine
        .exec("INSERT INTO dbo.Items VALUES (1), (2)")
        .unwrap();

    let batch = "
        BEGIN TRANSACTION;
        SET CURSOR_CLOSE_ON_COMMIT ON;
        DECLARE @id INT;
        DECLARE cur CURSOR FOR SELECT Id FROM dbo.Items ORDER BY Id;
        OPEN cur;
        FETCH NEXT FROM cur INTO @id;
        PRINT 'Before: ' + CAST(@@FETCH_STATUS AS NVARCHAR);
        COMMIT;
        PRINT 'After: ' + CAST(@@FETCH_STATUS AS NVARCHAR);
    ";
    for stmt in iridium_core::parser::parse_batch(batch).unwrap() {
        engine.execute(stmt).unwrap();
    }

    let output = engine.print_output();
    assert_eq!(output[0], "Before: 0");
    assert_eq!(output[1], "After: -1");
}

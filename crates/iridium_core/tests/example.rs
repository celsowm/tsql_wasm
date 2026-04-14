use iridium_core::{parse_sql, Engine};

#[test]
fn test_example_basic() {
    let engine = Engine::new();

    // Criar tabelas
    engine.execute(parse_sql("CREATE TABLE dbo.Users (Id INT IDENTITY(1,1) PRIMARY KEY, Name NVARCHAR(100) NOT NULL, IsActive BIT NOT NULL DEFAULT 1)").unwrap()).unwrap();
    engine.execute(parse_sql("CREATE TABLE dbo.Posts (Id INT IDENTITY(1,1) PRIMARY KEY, UserId INT NOT NULL, Title NVARCHAR(100) NOT NULL)").unwrap()).unwrap();

    // Inserir dados
    engine
        .execute(parse_sql("INSERT INTO dbo.Users (Name, IsActive) VALUES (N'Ana', 1)").unwrap())
        .unwrap();
    engine
        .execute(parse_sql("INSERT INTO dbo.Users (Name, IsActive) VALUES (N'Bruno', 0)").unwrap())
        .unwrap();
    engine
        .execute(parse_sql("INSERT INTO dbo.Users (Name, IsActive) VALUES (N'Carla', 1)").unwrap())
        .unwrap();

    engine
        .execute(parse_sql("INSERT INTO dbo.Posts (UserId, Title) VALUES (1, N'A')").unwrap())
        .unwrap();
    engine
        .execute(parse_sql("INSERT INTO dbo.Posts (UserId, Title) VALUES (1, N'B')").unwrap())
        .unwrap();
    engine
        .execute(parse_sql("INSERT INTO dbo.Posts (UserId, Title) VALUES (3, N'C')").unwrap())
        .unwrap();

    // Testar SELECT com JOIN, TOP, CAST, CONVERT
    let stmt = parse_sql("SELECT TOP 2 u.Name AS UserName, p.Title, CAST(u.Id AS BIGINT) AS UserId64, CONVERT(NVARCHAR(20), u.IsActive) AS ActiveText FROM dbo.Users u LEFT JOIN dbo.Posts p ON u.Id = p.UserId WHERE u.IsActive = 1 ORDER BY u.Id DESC").unwrap();
    let result = engine.execute(stmt).unwrap().unwrap();
    println!("Result: {:?}", result);
    assert_eq!(
        result.columns,
        vec!["UserName", "Title", "UserId64", "ActiveText"]
    );
    assert_eq!(result.rows.len(), 2);

    // Testar SELECT com GROUP BY e COUNT
    let stmt = parse_sql("SELECT u.Name, COUNT(*) AS TotalPosts FROM dbo.Users u INNER JOIN dbo.Posts p ON u.Id = p.UserId GROUP BY u.Name ORDER BY u.Name ASC").unwrap();
    let result = engine.execute(stmt).unwrap().unwrap();
    println!("Grouped Result: {:?}", result);
    assert_eq!(result.columns, vec!["Name", "TotalPosts"]);
    assert_eq!(result.rows.len(), 2);
}


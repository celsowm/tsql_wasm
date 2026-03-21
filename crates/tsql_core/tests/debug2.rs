use tsql_core::{parse_sql, Engine};

#[test]
fn test_debug_join_where() {
    let mut engine = Engine::new();
    
    // Criar tabelas
    engine.execute(parse_sql("CREATE TABLE dbo.Users (Id INT IDENTITY(1,1) PRIMARY KEY, Name NVARCHAR(100) NOT NULL, IsActive BIT NOT NULL DEFAULT 1)").unwrap()).unwrap();
    engine.execute(parse_sql("CREATE TABLE dbo.Posts (Id INT IDENTITY(1,1) PRIMARY KEY, UserId INT NOT NULL, Title NVARCHAR(100) NOT NULL)").unwrap()).unwrap();
    
    // Inserir dados
    engine.execute(parse_sql("INSERT INTO dbo.Users (Name, IsActive) VALUES (N'Ana', 1)").unwrap()).unwrap();
    engine.execute(parse_sql("INSERT INTO dbo.Users (Name, IsActive) VALUES (N'Bruno', 0)").unwrap()).unwrap();
    engine.execute(parse_sql("INSERT INTO dbo.Users (Name, IsActive) VALUES (N'Carla', 1)").unwrap()).unwrap();
    
    engine.execute(parse_sql("INSERT INTO dbo.Posts (UserId, Title) VALUES (1, N'A')").unwrap()).unwrap();
    engine.execute(parse_sql("INSERT INTO dbo.Posts (UserId, Title) VALUES (1, N'B')").unwrap()).unwrap();
    engine.execute(parse_sql("INSERT INTO dbo.Posts (UserId, Title) VALUES (3, N'C')").unwrap()).unwrap();
    
    // Testar SELECT simples primeiro
    let stmt = parse_sql("SELECT * FROM dbo.Users").unwrap();
    let result = engine.execute(stmt).unwrap().unwrap();
    println!("Users: {:?}", result);
    assert_eq!(result.rows.len(), 3);
    
    let stmt = parse_sql("SELECT * FROM dbo.Posts").unwrap();
    let result = engine.execute(stmt).unwrap().unwrap();
    println!("Posts: {:?}", result);
    assert_eq!(result.rows.len(), 3);
    
    // Testar WHERE simples
    let stmt = parse_sql("SELECT * FROM dbo.Users WHERE IsActive = 1").unwrap();
    let result = engine.execute(stmt).unwrap().unwrap();
    println!("Active Users: {:?}", result);
    assert_eq!(result.rows.len(), 2);
    
    // Testar JOIN simples
    let stmt = parse_sql("SELECT u.Name, p.Title FROM dbo.Users u LEFT JOIN dbo.Posts p ON u.Id = p.UserId").unwrap();
    let result = engine.execute(stmt).unwrap().unwrap();
    println!("JOIN result: {:?}", result);
    assert_eq!(result.rows.len(), 4); // 3 users + 1 duplicate for user 1 with 2 posts
    
    // Testar JOIN com WHERE
    let stmt = parse_sql("SELECT u.Name, p.Title FROM dbo.Users u LEFT JOIN dbo.Posts p ON u.Id = p.UserId WHERE u.IsActive = 1").unwrap();
    let result = engine.execute(stmt).unwrap().unwrap();
    println!("JOIN with WHERE: {:?}", result);
    assert_eq!(result.rows.len(), 3); // Ana (2 posts) + Carla (1 post)
}

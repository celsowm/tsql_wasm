import { IridiumDatabase } from "../../iridium-client/src/index";

const out = document.getElementById("out")!;

async function main() {
  const db = await IridiumDatabase.create();

  await db.exec(`
    CREATE TABLE dbo.Users (
      Id INT IDENTITY(1,1) PRIMARY KEY,
      Name NVARCHAR(100) NOT NULL,
      IsActive BIT NOT NULL DEFAULT 1
    )
  `);

  await db.exec(`
    CREATE TABLE dbo.Posts (
      Id INT IDENTITY(1,1) PRIMARY KEY,
      UserId INT NOT NULL,
      Title NVARCHAR(100) NOT NULL
    )
  `);

  await db.exec(`
    INSERT INTO dbo.Users (Name, IsActive)
    VALUES (N'Ana', 1), (N'Bruno', 0), (N'Carla', 1)
  `);

  await db.exec(`
    INSERT INTO dbo.Posts (UserId, Title)
    VALUES (1, N'A'), (1, N'B'), (3, N'C')
  `);

  const joined = await db.query(`
    SELECT TOP 2 u.Name AS UserName, p.Title,
           CAST(u.Id AS BIGINT) AS UserId64,
           CONVERT(NVARCHAR(20), u.IsActive) AS ActiveText
    FROM dbo.Users u
    LEFT JOIN dbo.Posts p ON u.Id = p.UserId
    WHERE u.IsActive = 1
    ORDER BY u.Id DESC
  `);

  const grouped = await db.query(`
    SELECT u.Name, COUNT(*) AS TotalPosts
    FROM dbo.Users u
    INNER JOIN dbo.Posts p ON u.Id = p.UserId
    GROUP BY u.Name
    ORDER BY u.Name ASC
  `);

  out.textContent = JSON.stringify({ joined, grouped }, null, 2);
}

main().catch((err) => {
  out.textContent = String(err);
});


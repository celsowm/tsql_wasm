import { describe, expect, it } from "vitest";
import { TsqlDatabase } from "../../client/src/index";

describe("join group by cast convert", () => {
  it("supports join, group by and count", async () => {
    const db = await TsqlDatabase.create();

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

    const grouped = await db.query(`
      SELECT u.Name, COUNT(*) AS TotalPosts
      FROM dbo.Users u
      INNER JOIN dbo.Posts p ON u.Id = p.UserId
      GROUP BY u.Name
      ORDER BY u.Name ASC
    `);

    expect(grouped.columns).toEqual(["Name", "TotalPosts"]);
    expect(grouped.rows).toEqual([
      ["Ana", 2],
      ["Carla", 1]
    ]);
  });

  it("supports cast, convert and left join", async () => {
    const db = await TsqlDatabase.create();

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
      VALUES (N'Ana', 1), (N'Bruno', 0)
    `);

    await db.exec(`
      INSERT INTO dbo.Posts (UserId, Title)
      VALUES (1, N'A')
    `);

    const joined = await db.query(`
      SELECT u.Name AS UserName,
             p.Title,
             CAST(u.Id AS BIGINT) AS UserId64,
             CONVERT(NVARCHAR(20), u.IsActive) AS ActiveText
      FROM dbo.Users u
      LEFT JOIN dbo.Posts p ON u.Id = p.UserId
      ORDER BY u.Id ASC
    `);

    expect(joined.columns).toEqual(["UserName", "Title", "UserId64", "ActiveText"]);
    expect(joined.rows[0]).toEqual(["Ana", "A", 1, "1"]);
    expect(joined.rows[1]).toEqual(["Bruno", null, 2, "0"]);
  });
});

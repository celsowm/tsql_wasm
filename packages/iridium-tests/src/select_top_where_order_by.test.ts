import { describe, expect, it } from "vitest";
import { IridiumDatabase } from "../../iridium-client/src/index";

describe("select top where order by", () => {
  it("filters, orders and limits rows", async () => {
    const db = await IridiumDatabase.create();

    await db.exec(`
      CREATE TABLE dbo.Users (
        Id INT IDENTITY(1,1) PRIMARY KEY,
        Name NVARCHAR(100) NOT NULL,
        IsActive BIT NOT NULL DEFAULT 1
      )
    `);

    await db.exec(`
      INSERT INTO dbo.Users (Name, IsActive)
      VALUES (N'Ana', 1), (N'Bruno', 0), (N'Carlos', 1)
    `);

    const result = await db.query(`
      SELECT TOP 1 Id, Name AS UserName
      FROM dbo.Users
      WHERE IsActive = 1
      ORDER BY Id DESC
    `);

    expect(result.row_count).toBe(1);
    expect(result.columns).toEqual(["Id", "UserName"]);
  });

  it("supports update delete count and null predicates", async () => {
    const db = await IridiumDatabase.create();

    await db.exec(`
      CREATE TABLE dbo.Items (
        Id INT IDENTITY(1,1) PRIMARY KEY,
        Name NVARCHAR(100) NULL,
        IsActive BIT NOT NULL DEFAULT 1
      )
    `);

    await db.exec(`INSERT INTO dbo.Items DEFAULT VALUES`);
    await db.exec(`INSERT INTO dbo.Items (Name, IsActive) VALUES (N'B', 0), (N'C', 1)`);
    await db.exec(`UPDATE dbo.Items SET IsActive = 1 WHERE Name IS NULL OR Name = N'B'`);
    await db.exec(`DELETE FROM dbo.Items WHERE Name = N'B'`);

    const count = await db.query(`
      SELECT COUNT(*) AS Total
      FROM dbo.Items
      WHERE IsActive = 1 AND Name IS NOT NULL
    `);

    expect(count.columns).toEqual(["Total"]);
    expect(count.rows[0]).toEqual([1]);
  });
});


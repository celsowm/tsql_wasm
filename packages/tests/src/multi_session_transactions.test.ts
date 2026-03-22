import { describe, expect, it } from "vitest";
import { TsqlDatabase } from "../../client/src/index";

describe("multi-session transactions", () => {
  it("shares state and reports deterministic commit conflicts", async () => {
    const db = await TsqlDatabase.create();

    await db.exec(`CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT NOT NULL)`);
    await db.exec(`INSERT INTO t (id, v) VALUES (1, 10)`);

    const s1 = await db.createSession();
    const s2 = await db.createSession();

    await s1.exec(`SET TRANSACTION ISOLATION LEVEL SNAPSHOT`);
    await s2.exec(`SET TRANSACTION ISOLATION LEVEL SNAPSHOT`);

    await s1.exec(`BEGIN TRANSACTION`);
    await s2.exec(`BEGIN TRANSACTION`);

    await s1.exec(`UPDATE t SET v = 11 WHERE id = 1`);
    await s2.exec(`UPDATE t SET v = 15 WHERE id = 1`);

    await s2.exec(`COMMIT`);
    await expect(s1.exec(`COMMIT`)).rejects.toThrow(/transaction conflict detected during COMMIT/i);

    const result = await db.query(`SELECT v FROM t WHERE id = 1`);
    expect(result.rows[0]).toEqual([15]);

    await db.closeSession(s1);
    await db.closeSession(s2);
  });
});

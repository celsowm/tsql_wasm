import { describe, expect, it } from "vitest";
import { IridiumDatabase } from "../../iridium-client/src/index";

describe("multi-session transactions", () => {
  it("shares state and reports deterministic commit conflicts", async () => {
    const db = await IridiumDatabase.create();

    await db.exec(`CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT NOT NULL)`);
    await db.exec(`INSERT INTO t (id, v) VALUES (1, 10)`);

    const s1 = await db.createSession();
    const s2 = await db.createSession();

    await s1.exec(`SET TRANSACTION ISOLATION LEVEL SNAPSHOT`);
    await s2.exec(`SET TRANSACTION ISOLATION LEVEL SNAPSHOT`);

    await s1.exec(`BEGIN TRANSACTION`);
    await s2.exec(`BEGIN TRANSACTION`);

    await s1.exec(`UPDATE t SET v = 11 WHERE id = 1`);
    await expect(s2.exec(`UPDATE t SET v = 15 WHERE id = 1`)).rejects.toThrow(
      /lock conflict \(no-wait\)/i,
    );

    await s2.exec(`COMMIT`);
    await s1.exec(`COMMIT`);

    const result = await db.query(`SELECT v FROM t WHERE id = 1`);
    expect(result.rows[0]).toEqual([11]);

    await db.closeSession(s1);
    await db.closeSession(s2);
  });

  it("exports and restores checkpoints", async () => {
    const db = await IridiumDatabase.create();
    await db.exec(`CREATE TABLE r (id INT NOT NULL PRIMARY KEY, v INT NOT NULL)`);
    await db.exec(`INSERT INTO r (id, v) VALUES (1, 42)`);

    const checkpoint = await db.exportCheckpoint();
    const restored = await IridiumDatabase.fromCheckpoint(checkpoint);
    const result = await restored.query(`SELECT v FROM r WHERE id = 1`);

    expect(result.rows[0]).toEqual([42]);
  });
});


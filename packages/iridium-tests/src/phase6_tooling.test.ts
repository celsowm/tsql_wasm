import { describe, expect, it } from "vitest";
import { IridiumDatabase } from "../../iridium-client/src/index";

describe("phase6 tooling APIs", () => {
  it("analyzes compatibility and exposes explain/trace/session options", async () => {
    const db = await IridiumDatabase.create();
    const session = await db.createSession();

    const report = await db.analyze(
      "SET DATEFIRST 9; SELECT 1 AS x; UNSUPPORTED TOKEN;",
    );
    expect(report.entries.length).toBe(3);
    expect(report.entries[0].status).toBe("Partial");
    expect(report.entries[2].status).toBe("Unsupported");

    await session.exec(`CREATE TABLE t (id INT NOT NULL PRIMARY KEY)`);
    const trace = await session.traceExecBatch(
      "SET NOCOUNT ON; INSERT INTO t (id) VALUES (1); SELECT id FROM t ORDER BY id;",
    );
    expect(trace.events.length).toBe(3);
    expect(trace.events[2]?.row_count).toBeNull();

    const plan = await db.explain("SELECT id FROM t ORDER BY id");
    expect(plan.statement_kind).toBe("SELECT");
    expect(plan.operators.some((op) => op.op === "Sort")).toBe(true);

    const options = await session.sessionOptions();
    expect(options.nocount).toBe(true);
  });
});


import init, { WasmDb } from "../../../crates/tsql_wasm/pkg/tsql_wasm.js";

export type QueryResult = {
  columns: string[];
  rows: unknown[];
  row_count: number;
};

export class TsqlDatabase {
  private db: WasmDb;

  private constructor(db: WasmDb) {
    this.db = db;
  }

  static async create(): Promise<TsqlDatabase> {
    if (isNodeRuntime()) {
      const fs = await import(nodeFsPromisesSpecifier());
      const wasmUrl = new URL(
        "../../../crates/tsql_wasm/pkg/tsql_wasm_bg.wasm",
        import.meta.url,
      );
      const wasmBytes = await fs.readFile(wasmUrl);
      await init({ module_or_path: wasmBytes });
    } else {
      await init();
    }
    return new TsqlDatabase(new WasmDb());
  }

  async exec(sql: string): Promise<void> {
    this.db.exec(sql);
  }

  async execBatch(sql: string): Promise<void> {
    this.db.exec_batch(sql);
  }

  async query(sql: string): Promise<QueryResult> {
    const json = this.db.query(sql);
    return JSON.parse(json) as QueryResult;
  }

  async reset(): Promise<void> {
    this.db.reset();
  }

  async createSession(): Promise<TsqlSession> {
    const id = this.db.create_session();
    return new TsqlSession(this.db, id);
  }

  async closeSession(session: TsqlSession): Promise<void> {
    this.db.close_session(session.id);
  }
}

export class TsqlSession {
  constructor(private db: WasmDb, public readonly id: number) {}

  async exec(sql: string): Promise<void> {
    this.db.exec_session(this.id, sql);
  }

  async execBatch(sql: string): Promise<void> {
    this.db.exec_batch_session(this.id, sql);
  }

  async query(sql: string): Promise<QueryResult> {
    const json = this.db.query_session(this.id, sql);
    return JSON.parse(json) as QueryResult;
  }
}

function isNodeRuntime(): boolean {
  return typeof process !== "undefined" && !!process.versions?.node;
}

function nodeFsPromisesSpecifier(): string {
  return "node:fs/promises";
}

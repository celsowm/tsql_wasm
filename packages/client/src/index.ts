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
    await init();
    return new TsqlDatabase(new WasmDb());
  }

  async exec(sql: string): Promise<void> {
    this.db.exec(sql);
  }

  async query(sql: string): Promise<QueryResult> {
    const json = this.db.query(sql);
    return JSON.parse(json) as QueryResult;
  }

  async reset(): Promise<void> {
    this.db.reset();
  }
}

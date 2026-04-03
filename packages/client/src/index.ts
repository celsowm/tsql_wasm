import init, { WasmDb } from "../../../crates/tsql_wasm/pkg/tsql_wasm.js";

export type QueryResult = {
  columns: string[];
  rows: unknown[];
  row_count: number;
};

export type SessionOptions = {
  ansi_nulls: boolean;
  quoted_identifier: boolean;
  nocount: boolean;
  xact_abort: boolean;
  datefirst: number;
  language: string;
};

export type SourceSpan = {
  start_line: number;
  start_col: number;
  end_line: number;
  end_col: number;
};

export type ExplainOperator = {
  op: string;
  detail: string;
};

export type ExplainPlan = {
  statement_kind: string;
  operators: ExplainOperator[];
  read_tables: string[];
  write_tables: string[];
};

export type TraceStatementEvent = {
  index: number;
  sql: string;
  normalized_sql: string;
  span: SourceSpan;
  status: string;
  warnings: string[];
  error: string | null;
  row_count: number | null;
  read_tables: string[];
  write_tables: string[];
};

export type ExecutionTrace = {
  events: TraceStatementEvent[];
  stopped_on_error: boolean;
};

export class TsqlDatabase {
  private db: WasmDb;

  private constructor(db: WasmDb) {
    this.db = db;
  }

  static async create(): Promise<TsqlDatabase> {
    await initWasm();
    return new TsqlDatabase(new WasmDb());
  }

  static async fromCheckpoint(payload: string): Promise<TsqlDatabase> {
    await initWasm();
    const db = new WasmDb();
    db.import_checkpoint(payload);
    return new TsqlDatabase(db);
  }

  async exportCheckpoint(): Promise<string> {
    return this.db.export_checkpoint();
  }

  async importCheckpoint(payload: string): Promise<void> {
    this.db.import_checkpoint(payload);
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

  async explain(sql: string): Promise<ExplainPlan> {
    const json = (this.db as any).explain_sql(sql);
    return JSON.parse(json) as ExplainPlan;
  }

  async traceExecBatch(sql: string): Promise<ExecutionTrace> {
    const json = (this.db as any).trace_exec_batch(sql);
    return JSON.parse(json) as ExecutionTrace;
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

  async sessionOptions(session: TsqlSession): Promise<SessionOptions> {
    const json = (this.db as any).session_options(session.id);
    return JSON.parse(json) as SessionOptions;
  }
}

export class TsqlSession {
  constructor(private db: WasmDb, public readonly id: bigint) {}

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

  async traceExecBatch(sql: string): Promise<ExecutionTrace> {
    const json = (this.db as any).trace_exec_batch_session(this.id, sql);
    return JSON.parse(json) as ExecutionTrace;
  }

  async sessionOptions(): Promise<SessionOptions> {
    const json = (this.db as any).session_options(this.id);
    return JSON.parse(json) as SessionOptions;
  }
}

async function initWasm(): Promise<void> {
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
}

function isNodeRuntime(): boolean {
  return typeof process !== "undefined" && !!process.versions?.node;
}

function nodeFsPromisesSpecifier(): string {
  return "node:fs/promises";
}

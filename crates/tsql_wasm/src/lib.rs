use wasm_bindgen::prelude::*;

use tsql_core::{
    parse_batch, parse_sql, Database, SessionId,
    CheckpointManager, SessionManager, StatementExecutor, SqlAnalyzer,
};

#[wasm_bindgen]
pub struct WasmDb {
    inner: Database,
    default_session: SessionId,
}

impl Default for WasmDb {
    fn default() -> Self {
        Self::new()
    }
}

#[wasm_bindgen]
impl WasmDb {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        console_error_panic_hook::set_once();
        let inner = Database::new();
        let default_session = inner.session_manager().create_session();
        Self {
            inner,
            default_session,
        }
    }

    pub fn exec(&mut self, sql: &str) -> Result<(), JsValue> {
        self.exec_session(self.default_session, sql)
    }

    pub fn exec_batch(&mut self, sql: &str) -> Result<(), JsValue> {
        self.exec_batch_session(self.default_session, sql)
    }

    pub fn query(&mut self, sql: &str) -> Result<String, JsValue> {
        self.query_session(self.default_session, sql)
    }

    pub fn create_session(&self) -> u64 {
        self.inner.session_manager().create_session()
    }

    pub fn close_session(&self, session_id: u64) -> Result<(), JsValue> {
        self.inner.session_manager().close_session(session_id).map_err(js_err)
    }

    pub fn exec_session(&mut self, session_id: u64, sql: &str) -> Result<(), JsValue> {
        let stmt = parse_sql(sql).map_err(js_err)?;
        let result = self
            .inner
            .executor()
            .execute_session(session_id, stmt)
            .map_err(js_err)?;
        if result.is_some() {
            return Err(JsValue::from_str(
                "exec() received a query statement; use query()",
            ));
        }
        Ok(())
    }

    pub fn exec_batch_session(&mut self, session_id: u64, sql: &str) -> Result<(), JsValue> {
        let stmts = parse_batch(sql).map_err(js_err)?;
        let result = self
            .inner
            .executor()
            .execute_session_batch(session_id, stmts)
            .map_err(js_err)?;
        if result.is_some() {
            return Err(JsValue::from_str(
                "exec_batch() ended with a query statement; use query() for SELECT",
            ));
        }
        Ok(())
    }

    pub fn query_session(&mut self, session_id: u64, sql: &str) -> Result<String, JsValue> {
        let stmt = parse_sql(sql).map_err(js_err)?;
        let result = self
            .inner
            .executor()
            .execute_session(session_id, stmt)
            .map_err(js_err)?;
        let result =
            result.ok_or_else(|| JsValue::from_str("query() expected a SELECT statement"))?;
        serde_json::to_string(&result.to_json_result())
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    pub fn explain_sql(&self, sql: &str) -> Result<String, JsValue> {
        let plan = self.inner.analyzer().explain_sql(sql).map_err(js_err)?;
        serde_json::to_string(&plan).map_err(|e| JsValue::from_str(&e.to_string()))
    }

    pub fn trace_exec_batch_session(&self, session_id: u64, sql: &str) -> Result<String, JsValue> {
        let trace = self
            .inner
            .analyzer()
            .trace_execute_session_sql(session_id, sql)
            .map_err(js_err)?;
        serde_json::to_string(&trace).map_err(|e| JsValue::from_str(&e.to_string()))
    }

    pub fn trace_exec_batch(&self, sql: &str) -> Result<String, JsValue> {
        self.trace_exec_batch_session(self.default_session, sql)
    }

    pub fn session_options(&self, session_id: u64) -> Result<String, JsValue> {
        let options = self.inner.analyzer().session_options(session_id).map_err(js_err)?;
        serde_json::to_string(&options).map_err(|e| JsValue::from_str(&e.to_string()))
    }

    pub fn reset(&mut self) {
        self.inner.reset();
    }

    pub fn export_checkpoint(&self) -> Result<String, JsValue> {
        self.inner.checkpoint_manager().export_checkpoint().map_err(js_err)
    }

    pub fn import_checkpoint(&mut self, payload: &str) -> Result<(), JsValue> {
        self.inner.checkpoint_manager().import_checkpoint(payload).map_err(js_err)
    }
}

fn js_err(err: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&err.to_string())
}

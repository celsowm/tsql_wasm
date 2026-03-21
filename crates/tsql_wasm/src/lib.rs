use wasm_bindgen::prelude::*;

use tsql_core::{parse_sql, Engine};

#[wasm_bindgen]
pub struct WasmDb {
    inner: Engine,
}

#[wasm_bindgen]
impl WasmDb {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        console_error_panic_hook::set_once();
        Self { inner: Engine::new() }
    }

    pub fn exec(&mut self, sql: &str) -> Result<(), JsValue> {
        let stmt = parse_sql(sql).map_err(js_err)?;
        let result = self.inner.execute(stmt).map_err(js_err)?;
        if result.is_some() {
            return Err(JsValue::from_str("exec() received a query statement; use query()"));
        }
        Ok(())
    }

    pub fn query(&mut self, sql: &str) -> Result<String, JsValue> {
        let stmt = parse_sql(sql).map_err(js_err)?;
        let result = self.inner.execute(stmt).map_err(js_err)?;
        let result = result.ok_or_else(|| JsValue::from_str("query() expected a SELECT statement"))?;
        serde_json::to_string(&result.to_json_result()).map_err(|e| JsValue::from_str(&e.to_string()))
    }

    pub fn reset(&mut self) {
        self.inner.reset();
    }
}

fn js_err(err: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&err.to_string())
}

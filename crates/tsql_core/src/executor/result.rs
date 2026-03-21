use serde::Serialize;

use crate::types::JsonValue;

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<JsonValue>>,
}

#[derive(Debug, Serialize)]
pub struct JsonQueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<serde_json::Value>,
    pub row_count: usize,
}

impl QueryResult {
    pub fn to_json_result(&self) -> JsonQueryResult {
        let rows = self
            .rows
            .iter()
            .map(|r| {
                let values = r
                    .iter()
                    .map(|v| serde_json::to_value(v).unwrap())
                    .collect::<Vec<_>>();
                serde_json::Value::Array(values)
            })
            .collect::<Vec<_>>();

        JsonQueryResult {
            columns: self.columns.clone(),
            row_count: rows.len(),
            rows,
        }
    }
}

use serde::{Deserialize, Serialize};

use crate::types::Value;

use crate::types::DataType;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub column_types: Vec<DataType>,
    pub rows: Vec<Vec<Value>>,
    pub return_status: Option<i32>,
    pub is_procedure: bool,
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
                    .map(|v| serde_json::to_value(v.to_json()).unwrap_or(serde_json::Value::Null))
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

use crate::executor::metadata::DB_CATALOG;
use crate::executor::result::QueryResult;
use crate::types::Value;

pub(crate) fn execute_xp_msver() -> QueryResult {
    QueryResult {
        columns: vec![
            "ID".to_string(),
            "Name".to_string(),
            "Internal_Value".to_string(),
            "Value".to_string(),
        ],
        column_types: vec![
            crate::types::DataType::Int,
            crate::types::DataType::NVarChar { max_len: 128 },
            crate::types::DataType::Int,
            crate::types::DataType::NVarChar { max_len: 512 },
        ],
        column_nullabilities: vec![true, true, true, true],
        rows: vec![
            vec![
                Value::Int(1),
                Value::NVarChar("ProductName".to_string()),
                Value::Int(0),
                Value::NVarChar(DB_CATALOG.to_string()),
            ],
            vec![
                Value::Int(2),
                Value::NVarChar("ProductVersion".to_string()),
                Value::Int(0),
                Value::NVarChar("16.0.1000.6".to_string()),
            ],
            vec![
                Value::Int(3),
                Value::NVarChar("Language".to_string()),
                Value::Int(0),
                Value::NVarChar("us_english".to_string()),
            ],
            vec![
                Value::Int(4),
                Value::NVarChar("Platform".to_string()),
                Value::Int(0),
                Value::NVarChar("Windows".to_string()),
            ],
            vec![
                Value::Int(5),
                Value::NVarChar("ProcessorCount".to_string()),
                Value::Int(1),
                Value::NVarChar("1".to_string()),
            ],
            vec![
                Value::Int(6),
                Value::NVarChar("PhysicalMemory".to_string()),
                Value::Int(0),
                Value::NVarChar("0".to_string()),
            ],
            vec![
                Value::Int(7),
                Value::NVarChar("ServerName".to_string()),
                Value::Int(0),
                Value::NVarChar("localhost".to_string()),
            ],
        ],
        ..Default::default()
    }
}

pub(crate) fn procedure_return_value(value: Option<Value>) -> Value {
    match value {
        Some(v) => match v {
            Value::Null => Value::Int(0),
            Value::Int(_) => v,
            other => Value::Int(other.to_integer_i64().unwrap_or(0) as i32),
        },
        None => Value::Int(0),
    }
}

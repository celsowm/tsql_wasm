use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataType {
    Bit,
    Int,
    BigInt,
    VarChar { max_len: u16 },
    NVarChar { max_len: u16 },
    DateTime,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bit(bool),
    Int(i32),
    BigInt(i64),
    VarChar(String),
    NVarChar(String),
    DateTime(String),
}

#[derive(Debug, Clone)]
pub struct TypedValue {
    pub ty: DataType,
    pub value: Value,
}

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum JsonValue {
    Null,
    Bool(bool),
    Number(i64),
    String(String),
}

impl Value {
    pub fn to_json(&self) -> JsonValue {
        match self {
            Value::Null => JsonValue::Null,
            Value::Bit(v) => JsonValue::Bool(*v),
            Value::Int(v) => JsonValue::Number(*v as i64),
            Value::BigInt(v) => JsonValue::Number(*v),
            Value::VarChar(v) => JsonValue::String(v.clone()),
            Value::NVarChar(v) => JsonValue::String(v.clone()),
            Value::DateTime(v) => JsonValue::String(v.clone()),
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

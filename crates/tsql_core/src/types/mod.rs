use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataType {
    Bit,
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Decimal { precision: u8, scale: u8 },
    Char { len: u16 },
    VarChar { max_len: u16 },
    NChar { len: u16 },
    NVarChar { max_len: u16 },
    Date,
    Time,
    DateTime,
    DateTime2,
    UniqueIdentifier,
    SqlVariant,
}

impl DataType {
    pub fn precedence(&self) -> u8 {
        match self {
            DataType::UniqueIdentifier => 15,
            DataType::DateTime2 => 14,
            DataType::DateTime => 13,
            DataType::Date => 12,
            DataType::Time => 11,
            DataType::NVarChar { .. } => 10,
            DataType::NChar { .. } => 9,
            DataType::VarChar { .. } => 8,
            DataType::Char { .. } => 7,
            DataType::Decimal { .. } => 6,
            DataType::BigInt => 5,
            DataType::Int => 4,
            DataType::SmallInt => 3,
            DataType::TinyInt => 2,
            DataType::Bit => 1,
            DataType::SqlVariant => 16,
        }
    }

    pub fn is_string_type(&self) -> bool {
        matches!(
            self,
            DataType::VarChar { .. }
                | DataType::NVarChar { .. }
                | DataType::Char { .. }
                | DataType::NChar { .. }
        )
    }

    pub fn is_integer_type(&self) -> bool {
        matches!(
            self,
            DataType::TinyInt | DataType::SmallInt | DataType::Int | DataType::BigInt
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Value {
    Null,
    Bit(bool),
    TinyInt(u8),
    SmallInt(i16),
    Int(i32),
    BigInt(i64),
    Decimal(i128, u8),
    Char(String),
    VarChar(String),
    NChar(String),
    NVarChar(String),
    Date(String),
    Time(String),
    DateTime(String),
    DateTime2(String),
    UniqueIdentifier(String),
    SqlVariant(Box<Value>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypedValue {
    pub ty: DataType,
    pub value: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Hash)]
#[serde(untagged)]
pub enum JsonValue {
    Null,
    Bool(bool),
    Number(i64),
    String(String),
}

impl Eq for JsonValue {}

impl std::cmp::PartialEq<serde_json::Value> for JsonValue {
    fn eq(&self, other: &serde_json::Value) -> bool {
        match (self, other) {
            (JsonValue::Null, serde_json::Value::Null) => true,
            (JsonValue::Bool(a), serde_json::Value::Bool(b)) => a == b,
            (JsonValue::Number(a), serde_json::Value::Number(b)) => b.as_i64() == Some(*a),
            (JsonValue::String(a), serde_json::Value::String(b)) => a == b,
            _ => false,
        }
    }
}

impl JsonValue {
    pub fn is_null(&self) -> bool {
        matches!(self, JsonValue::Null)
    }
}

impl Value {
    pub fn to_json(&self) -> JsonValue {
        match self {
            Value::Null => JsonValue::Null,
            Value::Bit(v) => JsonValue::Bool(*v),
            Value::TinyInt(v) => JsonValue::Number(*v as i64),
            Value::SmallInt(v) => JsonValue::Number(*v as i64),
            Value::Int(v) => JsonValue::Number(*v as i64),
            Value::BigInt(v) => JsonValue::Number(*v),
            Value::Decimal(raw, scale) => JsonValue::String(format_decimal(*raw, *scale)),
            Value::Char(v) | Value::VarChar(v) | Value::NChar(v) | Value::NVarChar(v) => {
                JsonValue::String(v.clone())
            }
            Value::Date(v)
            | Value::Time(v)
            | Value::DateTime(v)
            | Value::DateTime2(v)
            | Value::UniqueIdentifier(v) => JsonValue::String(v.clone()),
            Value::SqlVariant(v) => v.to_json(),
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn data_type(&self) -> Option<DataType> {
        match self {
            Value::Null => None,
            Value::Bit(_) => Some(DataType::Bit),
            Value::TinyInt(_) => Some(DataType::TinyInt),
            Value::SmallInt(_) => Some(DataType::SmallInt),
            Value::Int(_) => Some(DataType::Int),
            Value::BigInt(_) => Some(DataType::BigInt),
            Value::Decimal(_, scale) => Some(DataType::Decimal {
                precision: 38,
                scale: *scale,
            }),
            Value::Char(s) => Some(DataType::Char {
                len: s.len() as u16,
            }),
            Value::VarChar(s) => Some(DataType::VarChar {
                max_len: s.len() as u16,
            }),
            Value::NChar(s) => Some(DataType::NChar {
                len: s.len() as u16,
            }),
            Value::NVarChar(s) => Some(DataType::NVarChar {
                max_len: s.len() as u16,
            }),
            Value::Date(_) => Some(DataType::Date),
            Value::Time(_) => Some(DataType::Time),
            Value::DateTime(_) => Some(DataType::DateTime),
            Value::DateTime2(_) => Some(DataType::DateTime2),
            Value::UniqueIdentifier(_) => Some(DataType::UniqueIdentifier),
            Value::SqlVariant(_) => Some(DataType::SqlVariant),
        }
    }

    pub fn to_string_value(&self) -> String {
        match self {
            Value::Null => String::new(),
            Value::Bit(v) => (if *v { 1 } else { 0 }).to_string(),
            Value::TinyInt(v) => v.to_string(),
            Value::SmallInt(v) => v.to_string(),
            Value::Int(v) => v.to_string(),
            Value::BigInt(v) => v.to_string(),
            Value::Decimal(raw, scale) => format_decimal(*raw, *scale),
            Value::Char(v)
            | Value::VarChar(v)
            | Value::NChar(v)
            | Value::NVarChar(v)
            | Value::Date(v)
            | Value::Time(v)
            | Value::DateTime(v)
            | Value::DateTime2(v)
            | Value::UniqueIdentifier(v) => v.clone(),
            Value::SqlVariant(v) => v.to_string_value(),
        }
    }

    pub fn to_integer_i64(&self) -> Option<i64> {
        match self {
            Value::Bit(v) => Some(if *v { 1 } else { 0 }),
            Value::TinyInt(v) => Some(*v as i64),
            Value::SmallInt(v) => Some(*v as i64),
            Value::Int(v) => Some(*v as i64),
            Value::BigInt(v) => Some(*v),
            Value::Decimal(raw, scale) => {
                let divisor = 10i128.pow(*scale as u32);
                Some((*raw / divisor) as i64)
            }
            Value::SqlVariant(v) => v.to_integer_i64(),
            _ => None,
        }
    }
}

pub fn format_decimal(raw: i128, scale: u8) -> String {
    if scale == 0 {
        return raw.to_string();
    }
    let negative = raw < 0;
    let abs = raw.unsigned_abs();
    let divisor = 10u128.pow(scale as u32);
    let whole = abs / divisor;
    let frac = abs % divisor;
    let frac_str = format!("{:0>width$}", frac, width = scale as usize);
    format!("{}{}.{}", if negative { "-" } else { "" }, whole, frac_str)
}

pub fn type_precedence_join(a: &DataType, b: &DataType) -> DataType {
    if a.precedence() >= b.precedence() {
        a.clone()
    } else {
        b.clone()
    }
}


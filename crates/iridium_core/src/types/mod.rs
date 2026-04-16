use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataType {
    Bit,
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Float,
    Decimal { precision: u8, scale: u8 },
    Money,
    SmallMoney,
    Char { len: u16 },
    VarChar { max_len: u16 },
    NChar { len: u16 },
    NVarChar { max_len: u16 },
    Binary { len: u16 },
    VarBinary { max_len: u16 },
    Date,
    Time,
    DateTime,
    DateTime2,
    UniqueIdentifier,
    SqlVariant,
    Xml,
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
            DataType::Float => 18,
            DataType::Decimal { .. } => 6,
            DataType::Money => 17,
            DataType::SmallMoney => 6,
            DataType::BigInt => 5,
            DataType::Int => 4,
            DataType::SmallInt => 3,
            DataType::TinyInt => 2,
            DataType::Bit => 1,
            DataType::Binary { .. } => 19,
            DataType::VarBinary { .. } => 20,
            DataType::SqlVariant => 16,
            DataType::Xml => 21,
        }
    }

    pub fn is_string_type(&self) -> bool {
        matches!(
            self,
            DataType::VarChar { .. }
                | DataType::NVarChar { .. }
                | DataType::Char { .. }
                | DataType::NChar { .. }
                | DataType::Xml
        )
    }

    pub fn is_integer_type(&self) -> bool {
        matches!(
            self,
            DataType::TinyInt | DataType::SmallInt | DataType::Int | DataType::BigInt
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Value {
    Null,
    Bit(bool),
    TinyInt(u8),
    SmallInt(i16),
    Int(i32),
    BigInt(i64),
    Float(u64),
    Decimal(i128, u8),
    Money(i128),
    SmallMoney(i64),
    Char(String),
    VarChar(String),
    NChar(String),
    NVarChar(String),
    Binary(Vec<u8>),
    VarBinary(Vec<u8>),
    Date(NaiveDate),
    Time(NaiveTime),
    DateTime(NaiveDateTime),
    DateTime2(NaiveDateTime),
    UniqueIdentifier(Uuid),
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
            Value::Float(v) => JsonValue::String(format_float(f64::from_bits(*v))),
            Value::Decimal(raw, scale) => JsonValue::String(format_decimal(*raw, *scale)),
            Value::Money(v) => JsonValue::String(format_money(*v)),
            Value::SmallMoney(v) => JsonValue::String(format_money(*v as i128)),
            Value::Char(v) | Value::VarChar(v) | Value::NChar(v) | Value::NVarChar(v) => {
                JsonValue::String(v.clone())
            }
            Value::Binary(v) | Value::VarBinary(v) => JsonValue::String(format_binary(v)),
            Value::Date(v) => JsonValue::String(v.format("%Y-%m-%d").to_string()),
            Value::Time(v) => JsonValue::String(v.format("%H:%M:%S%.f").to_string()),
            Value::DateTime(v) | Value::DateTime2(v) => {
                JsonValue::String(v.format("%Y-%m-%d %H:%M:%S%.f").to_string())
            }
            Value::UniqueIdentifier(v) => JsonValue::String(v.to_string()),
            Value::SqlVariant(v) => v.to_json(),
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn to_sql_literal(&self) -> String {
        match self {
            Value::Null => "NULL".to_string(),
            Value::Bit(v) => (if *v { 1 } else { 0 }).to_string(),
            Value::TinyInt(v) => v.to_string(),
            Value::SmallInt(v) => v.to_string(),
            Value::Int(v) => v.to_string(),
            Value::BigInt(v) => v.to_string(),
            Value::Float(v) => format_float(f64::from_bits(*v)),
            Value::Decimal(raw, scale) => format_decimal(*raw, *scale),
            Value::Money(v) => format_money(*v),
            Value::SmallMoney(v) => format_money(*v as i128),
            Value::Char(v) | Value::VarChar(v) | Value::NChar(v) | Value::NVarChar(v) => {
                format!("'{}'", v.replace("'", "''"))
            }
            Value::Date(v) => format!("'{}'", v.format("%Y-%m-%d")),
            Value::Time(v) => format!("'{}'", v.format("%H:%M:%S%.f")),
            Value::DateTime(v) | Value::DateTime2(v) => {
                format!("'{}'", v.format("%Y-%m-%d %H:%M:%S%.f"))
            }
            Value::UniqueIdentifier(v) => format!("'{}'", v),
            Value::Binary(v) | Value::VarBinary(v) => format!("0x{}", hex::encode(v)),
            Value::SqlVariant(v) => v.to_sql_literal(),
        }
    }

    pub fn data_type(&self) -> Option<DataType> {
        match self {
            Value::Null => None,
            Value::Bit(_) => Some(DataType::Bit),
            Value::TinyInt(_) => Some(DataType::TinyInt),
            Value::SmallInt(_) => Some(DataType::SmallInt),
            Value::Int(_) => Some(DataType::Int),
            Value::BigInt(_) => Some(DataType::BigInt),
            Value::Float(_) => Some(DataType::Float),
            Value::Decimal(_, scale) => Some(DataType::Decimal {
                precision: 38,
                scale: *scale,
            }),
            Value::Money(_) => Some(DataType::Money),
            Value::SmallMoney(_) => Some(DataType::SmallMoney),
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
            Value::Binary(v) => Some(DataType::Binary {
                len: v.len() as u16,
            }),
            Value::VarBinary(v) => Some(DataType::VarBinary {
                max_len: v.len() as u16,
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
            Value::Float(v) => format_float(f64::from_bits(*v)),
            Value::Decimal(raw, scale) => format_decimal(*raw, *scale),
            Value::Money(v) => format_money(*v),
            Value::SmallMoney(v) => format_money(*v as i128),
            Value::Char(v) | Value::VarChar(v) | Value::NChar(v) | Value::NVarChar(v) => v.clone(),
            Value::Date(v) => v.format("%Y-%m-%d").to_string(),
            Value::Time(v) => v.format("%H:%M:%S%.f").to_string(),
            Value::DateTime(v) | Value::DateTime2(v) => {
                v.format("%Y-%m-%d %H:%M:%S%.f").to_string()
            }
            Value::UniqueIdentifier(v) => v.to_string(),
            Value::Binary(v) | Value::VarBinary(v) => format_binary(v),
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
            Value::Float(v) => Some(f64::from_bits(*v) as i64),
            Value::Decimal(raw, scale) => {
                let divisor = 10i128.pow(*scale as u32);
                Some((*raw / divisor) as i64)
            }
            Value::Money(v) => {
                let divisor = 10i128.pow(4u32);
                Some((*v / divisor) as i64)
            }
            Value::SmallMoney(v) => {
                let divisor = 10i64.pow(4u32);
                Some(v / divisor)
            }
            Value::Binary(v) | Value::VarBinary(v) => binary_to_i64(v),
            Value::SqlVariant(v) => v.to_integer_i64(),
            _ => None,
        }
    }

    pub fn to_f64(&self) -> Option<f64> {
        match self {
            Value::Float(v) => Some(f64::from_bits(*v)),
            Value::TinyInt(n) => Some(*n as f64),
            Value::SmallInt(n) => Some(*n as f64),
            Value::Int(n) => Some(*n as f64),
            Value::BigInt(n) => Some(*n as f64),
            Value::Decimal(raw, scale) => {
                let divisor = 10f64.powi(*scale as i32);
                Some(*raw as f64 / divisor)
            }
            Value::Money(raw) => Some(*raw as f64 / 10000.0),
            Value::SmallMoney(raw) => Some(*raw as f64 / 10000.0),
            Value::Bit(v) => Some(if *v { 1.0 } else { 0.0 }),
            Value::VarChar(s) | Value::NVarChar(s) | Value::Char(s) | Value::NChar(s) => {
                s.parse::<f64>().ok()
            }
            Value::SqlVariant(v) => v.to_f64(),
            _ => None,
        }
    }

    pub fn to_decimal_parts(&self) -> (i128, u8) {
        match self {
            Value::Decimal(raw, scale) => (*raw, *scale),
            Value::Bit(b) => (if *b { 1 } else { 0 }, 0),
            Value::TinyInt(v) => (*v as i128, 0),
            Value::SmallInt(v) => (*v as i128, 0),
            Value::Int(v) => (*v as i128, 0),
            Value::BigInt(v) => (*v as i128, 0),
            Value::Float(v) => {
                let f = f64::from_bits(*v);
                let scale = 6u8;
                ((f * 10f64.powi(scale as i32)) as i128, scale)
            }
            Value::Money(v) => (*v, 4),
            Value::SmallMoney(v) => (*v as i128, 4),
            Value::SqlVariant(v) => v.to_decimal_parts(),
            _ => (0, 0),
        }
    }
}

fn binary_to_i64(data: &[u8]) -> Option<i64> {
    if data.is_empty() {
        return Some(0);
    }
    if data.len() > 8 {
        return None;
    }

    let mut n: u64 = 0;
    for b in data {
        n = (n << 8) | (*b as u64);
    }

    Some(n as i64)
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

pub fn format_float(f: f64) -> String {
    let s = format!("{}", f);
    if s.contains('.')
        || s.contains('e')
        || s.contains('E')
        || s == "inf"
        || s == "-inf"
        || s == "nan"
    {
        // Strip trailing ".0" for whole numbers (e.g., "256.0" → "256")
        if let Some(dot_pos) = s.find('.') {
            let after_dot = &s[dot_pos + 1..];
            if after_dot.chars().all(|c| c == '0') && !s.contains('e') && !s.contains('E') {
                return s[..dot_pos].to_string();
            }
        }
        s
    } else {
        s
    }
}

pub fn format_money(raw: i128) -> String {
    let scale = 4u8;
    let negative = raw < 0;
    let abs = raw.unsigned_abs();
    let divisor = 10u128.pow(scale as u32);
    let whole = abs / divisor;
    let frac = abs % divisor;
    let frac_str = format!("{:0>width$}", frac, width = scale as usize);
    format!("{}${}.{}", if negative { "-" } else { "" }, whole, frac_str)
}

pub fn format_binary(data: &[u8]) -> String {
    let mut s = String::with_capacity(2 + data.len() * 2);
    s.push_str("0x");
    for b in data {
        use std::fmt::Write;
        let _ = write!(s, "{:02X}", b);
    }
    s
}

pub fn type_precedence_join(a: &DataType, b: &DataType) -> DataType {
    if a.precedence() >= b.precedence() {
        a.clone()
    } else {
        b.clone()
    }
}

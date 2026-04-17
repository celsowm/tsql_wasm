use super::super::value_helpers::{rescale_raw, value_to_f64};
use crate::types::{format_vector, Value};
use chrono::{NaiveDate, NaiveDateTime};
use std::cmp::Ordering;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueCategory {
    Integer,
    Float,
    Decimal,
    Money,
    String,
    Binary,
    Vector,
    DateTime,
    Uuid,
    Null,
}

pub fn categorize(v: &Value) -> ValueCategory {
    match v {
        Value::Null => ValueCategory::Null,
        Value::Bit(_)
        | Value::TinyInt(_)
        | Value::SmallInt(_)
        | Value::Int(_)
        | Value::BigInt(_) => ValueCategory::Integer,
        Value::Float(_) => ValueCategory::Float,
        Value::Decimal(_, _) => ValueCategory::Decimal,
        Value::Money(_) | Value::SmallMoney(_) => ValueCategory::Money,
        Value::Char(_) | Value::VarChar(_) | Value::NChar(_) | Value::NVarChar(_) => {
            ValueCategory::String
        }
        Value::Binary(_) | Value::VarBinary(_) => ValueCategory::Binary,
        Value::Vector(_) => ValueCategory::Vector,
        Value::Date(_)
        | Value::Time(_)
        | Value::DateTime(_)
        | Value::DateTime2(_)
        | Value::SmallDateTime(_) => {
            ValueCategory::DateTime
        }
        Value::DateTimeOffset(_) => ValueCategory::String,
        Value::UniqueIdentifier(_) => ValueCategory::Uuid,
        Value::SqlVariant(inner) => categorize(inner),
    }
}

pub fn compare_values(a: &Value, b: &Value) -> Ordering {
    let a = unwrap_sql_variant(a);
    let b = unwrap_sql_variant(b);

    let cat_a = categorize(&a);
    let cat_b = categorize(&b);

    match (cat_a, cat_b) {
        (ValueCategory::Null, ValueCategory::Null) => Ordering::Equal,
        (ValueCategory::Null, _) => Ordering::Less,
        (_, ValueCategory::Null) => Ordering::Greater,

        (ValueCategory::Integer, ValueCategory::Integer) => {
            let ai = a.to_integer_i64().unwrap_or(0);
            let bi = b.to_integer_i64().unwrap_or(0);
            ai.cmp(&bi)
        }

        (ValueCategory::Float, ValueCategory::Float)
        | (ValueCategory::Float, ValueCategory::Integer)
        | (ValueCategory::Integer, ValueCategory::Float)
        | (ValueCategory::Float, ValueCategory::Decimal)
        | (ValueCategory::Decimal, ValueCategory::Float) => {
            let af = value_to_f64(&a).unwrap_or(0.0);
            let bf = value_to_f64(&b).unwrap_or(0.0);
            af.partial_cmp(&bf).unwrap_or(Ordering::Equal)
        }

        (ValueCategory::Decimal, ValueCategory::Decimal)
        | (ValueCategory::Decimal, ValueCategory::Integer)
        | (ValueCategory::Integer, ValueCategory::Decimal) => {
            let (a_dec, b_dec) = to_comparable_decimals(&a, &b);
            a_dec.cmp(&b_dec)
        }

        (ValueCategory::Money, ValueCategory::Money)
        | (ValueCategory::Money, ValueCategory::Integer)
        | (ValueCategory::Integer, ValueCategory::Money)
        | (ValueCategory::Money, ValueCategory::Decimal)
        | (ValueCategory::Decimal, ValueCategory::Money) => {
            let am = extract_money_raw(&a);
            let bm = extract_money_raw(&b);
            am.cmp(&bm)
        }

        (ValueCategory::Money, ValueCategory::Float)
        | (ValueCategory::Float, ValueCategory::Money) => {
            let af = value_to_f64(&a).unwrap_or(0.0);
            let bf = value_to_f64(&b).unwrap_or(0.0);
            af.partial_cmp(&bf).unwrap_or(Ordering::Equal)
        }

        (ValueCategory::String, ValueCategory::String) => {
            extract_string(&a).cmp(&extract_string(&b))
        }

        (ValueCategory::Integer, ValueCategory::String)
        | (ValueCategory::Decimal, ValueCategory::String)
        | (ValueCategory::Float, ValueCategory::String)
        | (ValueCategory::Money, ValueCategory::String) => compare_numeric_with_string(&a, &b),

        (ValueCategory::String, ValueCategory::Integer)
        | (ValueCategory::String, ValueCategory::Decimal)
        | (ValueCategory::String, ValueCategory::Float)
        | (ValueCategory::String, ValueCategory::Money) => compare_numeric_with_string(&a, &b),

        (ValueCategory::DateTime, ValueCategory::DateTime) => {
            if let (Some(da), Some(db)) = (normalize_datetime(&a), normalize_datetime(&b)) {
                da.cmp(&db)
            } else {
                extract_string(&a).cmp(&extract_string(&b))
            }
        }

        (ValueCategory::DateTime, ValueCategory::String)
        | (ValueCategory::String, ValueCategory::DateTime) => {
            a.to_string_value().cmp(&b.to_string_value())
        }

        (ValueCategory::Uuid, ValueCategory::Uuid) => extract_string(&a).cmp(&extract_string(&b)),

        (ValueCategory::Binary, ValueCategory::Binary) => extract_bytes(&a).cmp(extract_bytes(&b)),
        (ValueCategory::Vector, ValueCategory::Vector) => extract_vector(&a).cmp(extract_vector(&b)),

        _ => value_key(&a).cmp(&value_key(&b)),
    }
}

pub fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bit(v) => *v,
        Value::TinyInt(v) => *v != 0,
        Value::SmallInt(v) => *v != 0,
        Value::Int(v) => *v != 0,
        Value::BigInt(v) => *v != 0,
        Value::Float(v) => f64::from_bits(*v) != 0.0,
        Value::Decimal(raw, _) => *raw != 0,
        Value::Money(v) => *v != 0,
        Value::SmallMoney(v) => *v != 0,
        Value::Char(v) | Value::VarChar(v) | Value::NChar(v) | Value::NVarChar(v) => !v.is_empty(),
        Value::Binary(v) | Value::VarBinary(v) => !v.is_empty(),
        Value::Vector(v) => !v.is_empty(),
        Value::Date(_)
        | Value::Time(_)
        | Value::DateTime(_)
        | Value::DateTime2(_)
        | Value::SmallDateTime(_)
        | Value::DateTimeOffset(_)
        | Value::UniqueIdentifier(_) => true,
        Value::SqlVariant(inner) => truthy(inner),
    }
}

pub fn value_key(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        Value::Bit(v) => format!("BIT:{}", v),
        Value::TinyInt(v) => format!("TINYINT:{}", v),
        Value::SmallInt(v) => format!("SMALLINT:{}", v),
        Value::Int(v) => format!("INT:{}", v),
        Value::BigInt(v) => format!("BIGINT:{}", v),
        Value::Float(v) => format!("FLOAT:{:?}", f64::from_bits(*v)),
        Value::Decimal(raw, scale) => format!("DECIMAL:{}:{}", raw, scale),
        Value::Money(v) => format!("MONEY:{}", v),
        Value::SmallMoney(v) => format!("SMALLMONEY:{}", v),
        Value::Char(v) => format!("CHAR:{}", v),
        Value::VarChar(v) => format!("VARCHAR:{}", v),
        Value::NChar(v) => format!("NCHAR:{}", v),
        Value::NVarChar(v) => format!("NVARCHAR:{}", v),
        Value::Binary(v) => format!("BINARY:{}", crate::types::format_binary(v)),
        Value::VarBinary(v) => format!("VARBINARY:{}", crate::types::format_binary(v)),
        Value::Vector(v) => format!("VECTOR:{}", format_vector(v)),
        Value::Date(v) => format!("DATE:{}", v),
        Value::Time(v) => format!("TIME:{}", v),
        Value::DateTime(v) => format!("DATETIME:{}", v),
        Value::DateTime2(v) => format!("DATETIME2:{}", v),
        Value::SmallDateTime(v) => format!("SMALLDATETIME:{}", v),
        Value::DateTimeOffset(v) => format!("DATETIMEOFFSET:{}", v),
        Value::UniqueIdentifier(v) => format!("UNIQUEIDENTIFIER:{}", v),
        Value::SqlVariant(inner) => format!("SQL_VARIANT:{}", value_key(inner)),
    }
}

fn unwrap_sql_variant(v: &Value) -> Value {
    match v {
        Value::SqlVariant(inner) => unwrap_sql_variant(inner),
        other => other.clone(),
    }
}

fn compare_numeric_with_string(num: &Value, str_val: &Value) -> Ordering {
    let num_str = extract_string(num);
    if let Some((ar, as_)) = parse_string_as_numeric(&num_str) {
        let str_parsed = parse_string_as_numeric(&extract_string(str_val));
        if let Some((br, bs)) = str_parsed {
            let (an, bn) = normalize_decimals(ar, as_, br, bs);
            return an.cmp(&bn);
        }
    }
    num.to_string_value().cmp(&str_val.to_string_value())
}

fn extract_string(v: &Value) -> String {
    match v {
        Value::Char(s) | Value::VarChar(s) | Value::NChar(s) | Value::NVarChar(s) => s.clone(),
        Value::Date(d) => d.format("%Y-%m-%d").to_string(),
        Value::Time(t) => t.format("%H:%M:%S%.f").to_string(),
        Value::DateTime(dt) | Value::DateTime2(dt) | Value::SmallDateTime(dt) => {
            dt.format("%Y-%m-%d %H:%M:%S%.f").to_string()
        }
        Value::DateTimeOffset(s) => s.clone(),
        _ => String::new(),
    }
}

fn extract_bytes(v: &Value) -> &[u8] {
    match v {
        Value::Binary(b) | Value::VarBinary(b) => b,
        _ => &[],
    }
}

fn extract_vector(v: &Value) -> &[u32] {
    match v {
        Value::Vector(bits) => bits,
        _ => &[],
    }
}

fn normalize_datetime(v: &Value) -> Option<NaiveDateTime> {
    match v {
        Value::Date(d) => d.and_hms_opt(0, 0, 0),
        Value::Time(t) => NaiveDate::from_ymd_opt(1900, 1, 1).map(|d| d.and_time(*t)),
        Value::DateTime(dt) | Value::DateTime2(dt) | Value::SmallDateTime(dt) => Some(*dt),
        Value::SqlVariant(inner) => normalize_datetime(inner),
        _ => None,
    }
}

fn extract_money_raw(v: &Value) -> i128 {
    match v {
        Value::Money(r) => *r,
        Value::SmallMoney(r) => *r as i128,
        Value::Decimal(raw, scale) => rescale_raw(*raw, *scale, 4),
        Value::Int(v) => *v as i128 * 10000,
        Value::BigInt(v) => *v as i128 * 10000,
        Value::TinyInt(v) => *v as i128 * 10000,
        Value::SmallInt(v) => *v as i128 * 10000,
        _ => 0,
    }
}

fn normalize_decimals(ar: i128, as_: u8, br: i128, bs: u8) -> (i128, i128) {
    let max_scale = as_.max(bs);
    let an = rescale_raw(ar, as_, max_scale);
    let bn = rescale_raw(br, bs, max_scale);
    (an, bn)
}

fn to_comparable_decimals(a: &Value, b: &Value) -> (i128, i128) {
    let (ar, as_) = match a {
        Value::Decimal(r, s) => (*r, *s),
        _ => (a.to_integer_i64().unwrap_or(0) as i128, 0),
    };
    let (br, bs) = match b {
        Value::Decimal(r, s) => (*r, *s),
        _ => (b.to_integer_i64().unwrap_or(0) as i128, 0),
    };
    normalize_decimals(ar, as_, br, bs)
}

fn parse_string_as_numeric(input: &str) -> Option<(i128, u8)> {
    let s = input.trim();
    if s.is_empty() {
        return None;
    }
    if let Ok(i) = s.parse::<i128>() {
        return Some((i, 0));
    }
    let negative = s.starts_with('-');
    let core = if negative || s.starts_with('+') {
        &s[1..]
    } else {
        s
    };
    let parts: Vec<&str> = core.splitn(2, '.').collect();
    if parts.len() != 2 {
        return None;
    }
    let whole = parts[0].parse::<i128>().ok()?;
    let frac_raw = parts[1];
    if frac_raw.is_empty() || !frac_raw.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let scale = frac_raw.len() as u8;
    let frac = frac_raw.parse::<i128>().ok()?;
    let mut raw = whole * 10i128.pow(scale as u32) + frac;
    if negative {
        raw = -raw;
    }
    Some((raw, scale))
}

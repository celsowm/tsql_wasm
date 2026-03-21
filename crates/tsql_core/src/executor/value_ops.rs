use std::cmp::Ordering;

use crate::error::DbError;
use crate::types::{DataType, Value};

pub(crate) fn coerce_value_to_type(value: Value, ty: &DataType) -> Result<Value, DbError> {
    match value {
        Value::Null => Ok(Value::Null),
        Value::Bit(v) => coerce_bit(v, ty),
        Value::TinyInt(v) => coerce_int(v as i64, ty),
        Value::SmallInt(v) => coerce_int(v as i64, ty),
        Value::Int(v) => coerce_int(v as i64, ty),
        Value::BigInt(v) => coerce_int(v, ty),
        Value::Decimal(raw, scale) => coerce_decimal(raw, scale, ty),
        Value::Char(v) | Value::VarChar(v) | Value::NChar(v) | Value::NVarChar(v) => {
            coerce_string(&v, ty)
        }
        Value::Date(v) => coerce_date_time_string(&v, ty),
        Value::Time(v) => coerce_date_time_string(&v, ty),
        Value::DateTime(v) => coerce_date_time_string(&v, ty),
        Value::DateTime2(v) => coerce_date_time_string(&v, ty),
        Value::UniqueIdentifier(v) => coerce_uuid(&v, ty),
    }
}

fn coerce_bit(v: bool, ty: &DataType) -> Result<Value, DbError> {
    let int_val: i64 = if v { 1 } else { 0 };
    match ty {
        DataType::Bit => Ok(Value::Bit(v)),
        DataType::TinyInt => Ok(Value::TinyInt(int_val as u8)),
        DataType::SmallInt => Ok(Value::SmallInt(int_val as i16)),
        DataType::Int => Ok(Value::Int(int_val as i32)),
        DataType::BigInt => Ok(Value::BigInt(int_val)),
        DataType::Decimal { scale, .. } => Ok(Value::Decimal(int_val as i128, *scale)),
        DataType::Char { .. } | DataType::VarChar { .. } => Ok(Value::VarChar(int_val.to_string())),
        DataType::NChar { .. } | DataType::NVarChar { .. } => {
            Ok(Value::NVarChar(int_val.to_string()))
        }
        DataType::DateTime | DataType::DateTime2 | DataType::Date | DataType::Time => Err(
            DbError::Execution(format!("cannot convert bit to {:?}", ty)),
        ),
        DataType::UniqueIdentifier => Err(DbError::Execution(
            "cannot convert bit to UNIQUEIDENTIFIER".into(),
        )),
    }
}

fn coerce_int(v: i64, ty: &DataType) -> Result<Value, DbError> {
    match ty {
        DataType::Bit => Ok(Value::Bit(v != 0)),
        DataType::TinyInt => {
            if !(0..=255).contains(&v) {
                Err(DbError::Execution(format!(
                    "Arithmetic overflow error converting value {} to TINYINT",
                    v
                )))
            } else {
                Ok(Value::TinyInt(v as u8))
            }
        }
        DataType::SmallInt => {
            if v < i16::MIN as i64 || v > i16::MAX as i64 {
                Err(DbError::Execution(format!(
                    "Arithmetic overflow error converting value {} to SMALLINT",
                    v
                )))
            } else {
                Ok(Value::SmallInt(v as i16))
            }
        }
        DataType::Int => {
            if v < i32::MIN as i64 || v > i32::MAX as i64 {
                Err(DbError::Execution(format!(
                    "Arithmetic overflow error converting value {} to INT",
                    v
                )))
            } else {
                Ok(Value::Int(v as i32))
            }
        }
        DataType::BigInt => Ok(Value::BigInt(v)),
        DataType::Decimal { scale, .. } => {
            let raw = v as i128 * 10i128.pow(*scale as u32);
            Ok(Value::Decimal(raw, *scale))
        }
        DataType::Char { .. } | DataType::VarChar { .. } => Ok(Value::VarChar(v.to_string())),
        DataType::NChar { .. } | DataType::NVarChar { .. } => Ok(Value::NVarChar(v.to_string())),
        DataType::DateTime | DataType::DateTime2 | DataType::Date | DataType::Time => Err(
            DbError::Execution(format!("cannot convert integer to {:?}", ty)),
        ),
        DataType::UniqueIdentifier => Err(DbError::Execution(
            "cannot convert integer to UNIQUEIDENTIFIER".into(),
        )),
    }
}

fn coerce_decimal(raw: i128, scale: u8, ty: &DataType) -> Result<Value, DbError> {
    match ty {
        DataType::Decimal { scale: ts, .. } => {
            if *ts == scale {
                Ok(Value::Decimal(raw, scale))
            } else {
                let converted = rescale_decimal(raw, scale, *ts);
                Ok(Value::Decimal(converted, *ts))
            }
        }
        DataType::Bit => Ok(Value::Bit(raw != 0)),
        DataType::TinyInt => {
            let v = if scale > 0 {
                raw / 10i128.pow(scale as u32)
            } else {
                raw
            };
            if !(0..=255).contains(&v) {
                Err(DbError::Execution(
                    "Arithmetic overflow error converting DECIMAL to TINYINT".into(),
                ))
            } else {
                Ok(Value::TinyInt(v as u8))
            }
        }
        DataType::SmallInt => {
            let v = if scale > 0 {
                raw / 10i128.pow(scale as u32)
            } else {
                raw
            };
            if v < i16::MIN as i128 || v > i16::MAX as i128 {
                Err(DbError::Execution(
                    "Arithmetic overflow error converting DECIMAL to SMALLINT".into(),
                ))
            } else {
                Ok(Value::SmallInt(v as i16))
            }
        }
        DataType::Int => {
            let v = if scale > 0 {
                raw / 10i128.pow(scale as u32)
            } else {
                raw
            };
            if v < i32::MIN as i128 || v > i32::MAX as i128 {
                Err(DbError::Execution(
                    "Arithmetic overflow error converting DECIMAL to INT".into(),
                ))
            } else {
                Ok(Value::Int(v as i32))
            }
        }
        DataType::BigInt => {
            let v = if scale > 0 {
                raw / 10i128.pow(scale as u32)
            } else {
                raw
            };
            if v < i64::MIN as i128 || v > i64::MAX as i128 {
                Err(DbError::Execution(
                    "Arithmetic overflow error converting DECIMAL to BIGINT".into(),
                ))
            } else {
                Ok(Value::BigInt(v as i64))
            }
        }
        DataType::Char { .. } | DataType::VarChar { .. } => {
            Ok(Value::VarChar(crate::types::format_decimal(raw, scale)))
        }
        DataType::NChar { .. } | DataType::NVarChar { .. } => {
            Ok(Value::NVarChar(crate::types::format_decimal(raw, scale)))
        }
        _ => Err(DbError::Execution(format!(
            "cannot convert DECIMAL to {:?}",
            ty
        ))),
    }
}

fn coerce_string(v: &str, ty: &DataType) -> Result<Value, DbError> {
    match ty {
        DataType::Bit => Ok(Value::Bit(v != "0" && !v.is_empty())),
        DataType::TinyInt => v
            .parse::<u8>()
            .map(Value::TinyInt)
            .map_err(|_| DbError::Execution(format!("cannot convert '{}' to TINYINT", v))),
        DataType::SmallInt => v
            .parse::<i16>()
            .map(Value::SmallInt)
            .map_err(|_| DbError::Execution(format!("cannot convert '{}' to SMALLINT", v))),
        DataType::Int => v
            .parse::<i32>()
            .map(Value::Int)
            .map_err(|_| DbError::Execution(format!("cannot convert '{}' to INT", v))),
        DataType::BigInt => v
            .parse::<i64>()
            .map(Value::BigInt)
            .map_err(|_| DbError::Execution(format!("cannot convert '{}' to BIGINT", v))),
        DataType::Decimal { scale, .. } => parse_decimal_string(v, *scale),
        DataType::Char { len } => {
            let padded = pad_right(v, *len as usize);
            Ok(Value::Char(padded))
        }
        DataType::VarChar { .. } => Ok(Value::VarChar(v.to_string())),
        DataType::NChar { len } => {
            let padded = pad_right(v, *len as usize);
            Ok(Value::NChar(padded))
        }
        DataType::NVarChar { .. } => Ok(Value::NVarChar(v.to_string())),
        DataType::Date => Ok(Value::Date(v.to_string())),
        DataType::Time => Ok(Value::Time(v.to_string())),
        DataType::DateTime => Ok(Value::DateTime(v.to_string())),
        DataType::DateTime2 => Ok(Value::DateTime2(v.to_string())),
        DataType::UniqueIdentifier => Ok(Value::UniqueIdentifier(v.to_string())),
    }
}

fn coerce_date_time_string(v: &str, ty: &DataType) -> Result<Value, DbError> {
    match ty {
        DataType::Char { .. } | DataType::VarChar { .. } => Ok(Value::VarChar(v.to_string())),
        DataType::NChar { .. } | DataType::NVarChar { .. } => Ok(Value::NVarChar(v.to_string())),
        DataType::Date => Ok(Value::Date(v.to_string())),
        DataType::Time => Ok(Value::Time(v.to_string())),
        DataType::DateTime => Ok(Value::DateTime(v.to_string())),
        DataType::DateTime2 => Ok(Value::DateTime2(v.to_string())),
        _ => Err(DbError::Execution(format!(
            "cannot convert datetime-like value to {:?}",
            ty
        ))),
    }
}

fn coerce_uuid(v: &str, ty: &DataType) -> Result<Value, DbError> {
    match ty {
        DataType::UniqueIdentifier => Ok(Value::UniqueIdentifier(v.to_string())),
        DataType::Char { .. } | DataType::VarChar { .. } => Ok(Value::VarChar(v.to_string())),
        DataType::NChar { .. } | DataType::NVarChar { .. } => Ok(Value::NVarChar(v.to_string())),
        _ => Err(DbError::Execution(format!(
            "cannot convert UNIQUEIDENTIFIER to {:?}",
            ty
        ))),
    }
}

pub(crate) fn parse_decimal_string(s: &str, scale: u8) -> Result<Value, DbError> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Err(DbError::Execution(
            "cannot convert empty string to DECIMAL".into(),
        ));
    }
    let negative = trimmed.starts_with('-');
    let abs_str = if negative || trimmed.starts_with('+') {
        &trimmed[1..]
    } else {
        trimmed
    };

    let parts: Vec<&str> = abs_str.splitn(2, '.').collect();
    let whole_str = parts[0];
    let frac_str = parts.get(1).copied().unwrap_or("");

    let whole: i128 = whole_str
        .parse()
        .map_err(|_| DbError::Execution(format!("cannot convert '{}' to DECIMAL", s)))?;

    let mut frac: i128 = 0;
    if scale > 0 && !frac_str.is_empty() {
        let truncated = if frac_str.len() > scale as usize {
            &frac_str[..scale as usize]
        } else {
            frac_str
        };
        frac = truncated
            .parse()
            .map_err(|_| DbError::Execution(format!("cannot convert '{}' to DECIMAL", s)))?;
        if frac_str.len() < scale as usize {
            frac *= 10i128.pow((scale as usize - frac_str.len()) as u32);
        }
    }

    let raw = whole * 10i128.pow(scale as u32) + frac;
    let raw = if negative { -raw } else { raw };
    Ok(Value::Decimal(raw, scale))
}

fn rescale_decimal(raw: i128, from_scale: u8, to_scale: u8) -> i128 {
    if from_scale == to_scale {
        return raw;
    }
    if to_scale > from_scale {
        raw * 10i128.pow((to_scale - from_scale) as u32)
    } else {
        raw / 10i128.pow((from_scale - to_scale) as u32)
    }
}

fn pad_right(s: &str, len: usize) -> String {
    if s.len() >= len {
        s[..len].to_string()
    } else {
        format!("{:width$}", s, width = len)
    }
}

pub(crate) fn compare_values(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,

        // Integer-to-integer comparisons
        (Value::TinyInt(x), Value::TinyInt(y)) => x.cmp(y),
        (Value::SmallInt(x), Value::SmallInt(y)) => x.cmp(y),
        (Value::Int(x), Value::Int(y)) => x.cmp(y),
        (Value::BigInt(x), Value::BigInt(y)) => x.cmp(y),

        // Cross-integer comparisons via i64
        (a, b) if is_integer_value(a) && is_integer_value(b) => {
            let ai = a.to_integer_i64().unwrap_or(0);
            let bi = b.to_integer_i64().unwrap_or(0);
            ai.cmp(&bi)
        }

        // Bit with integers (via to_integer_i64)
        (Value::Bit(_), b) if is_integer_value(b) => {
            let ai = a.to_integer_i64().unwrap_or(0);
            let bi = b.to_integer_i64().unwrap_or(0);
            ai.cmp(&bi)
        }
        (a, Value::Bit(_)) if is_integer_value(a) => {
            let ai = a.to_integer_i64().unwrap_or(0);
            let bi = b.to_integer_i64().unwrap_or(0);
            ai.cmp(&bi)
        }
        (Value::Bit(x), Value::Bit(y)) => x.cmp(y),

        // Decimal comparisons
        (Value::Decimal(ar, as_), Value::Decimal(br, bs)) => {
            let (an, bn) = normalize_decimals(*ar, *as_, *br, *bs);
            an.cmp(&bn)
        }
        (Value::Decimal(_, _), b) if is_numeric_value(b) => {
            let (a_dec, b_dec) = to_comparable_decimals(a, b);
            a_dec.cmp(&b_dec)
        }
        (a, Value::Decimal(_, _)) if is_numeric_value(a) => {
            let (a_dec, b_dec) = to_comparable_decimals(a, b);
            a_dec.cmp(&b_dec)
        }

        // String comparisons
        (Value::Char(x), Value::Char(y)) => x.cmp(y),
        (Value::VarChar(x), Value::VarChar(y)) => x.cmp(y),
        (Value::NChar(x), Value::NChar(y)) => x.cmp(y),
        (Value::NVarChar(x), Value::NVarChar(y)) => x.cmp(y),
        (a, b) if is_string_value(a) && is_string_value(b) => {
            let astr = extract_string(a);
            let bstr = extract_string(b);
            astr.cmp(bstr)
        }

        // DateTime-like comparisons
        (Value::Date(x), Value::Date(y)) => x.cmp(y),
        (Value::Time(x), Value::Time(y)) => x.cmp(y),
        (Value::DateTime(x), Value::DateTime(y)) => x.cmp(y),
        (Value::DateTime2(x), Value::DateTime2(y)) => x.cmp(y),
        (a, b) if is_datetime_value(a) && is_datetime_value(b) => {
            let astr = extract_string(a);
            let bstr = extract_string(b);
            astr.cmp(bstr)
        }

        // UUID
        (Value::UniqueIdentifier(x), Value::UniqueIdentifier(y)) => x.cmp(y),

        // Fallback
        _ => value_key(a).cmp(&value_key(b)),
    }
}

fn is_integer_value(v: &Value) -> bool {
    matches!(
        v,
        Value::Bit(_) | Value::TinyInt(_) | Value::SmallInt(_) | Value::Int(_) | Value::BigInt(_)
    )
}

fn is_numeric_value(v: &Value) -> bool {
    is_integer_value(v) || matches!(v, Value::Decimal(_, _))
}

fn is_string_value(v: &Value) -> bool {
    matches!(
        v,
        Value::Char(_) | Value::VarChar(_) | Value::NChar(_) | Value::NVarChar(_)
    )
}

fn is_datetime_value(v: &Value) -> bool {
    matches!(
        v,
        Value::Date(_) | Value::Time(_) | Value::DateTime(_) | Value::DateTime2(_)
    )
}

fn extract_string(v: &Value) -> &str {
    match v {
        Value::Char(s) | Value::VarChar(s) | Value::NChar(s) | Value::NVarChar(s) => s,
        Value::Date(s) | Value::Time(s) | Value::DateTime(s) | Value::DateTime2(s) => s,
        _ => "",
    }
}

fn normalize_decimals(ar: i128, as_: u8, br: i128, bs: u8) -> (i128, i128) {
    let max_scale = as_.max(bs);
    let an = rescale_decimal(ar, as_, max_scale);
    let bn = rescale_decimal(br, bs, max_scale);
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

pub(crate) fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bit(v) => *v,
        Value::TinyInt(v) => *v != 0,
        Value::SmallInt(v) => *v != 0,
        Value::Int(v) => *v != 0,
        Value::BigInt(v) => *v != 0,
        Value::Decimal(raw, _) => *raw != 0,
        Value::Char(v) | Value::VarChar(v) | Value::NChar(v) | Value::NVarChar(v) => !v.is_empty(),
        Value::Date(_)
        | Value::Time(_)
        | Value::DateTime(_)
        | Value::DateTime2(_)
        | Value::UniqueIdentifier(_) => true,
    }
}

pub(crate) fn value_key(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        Value::Bit(v) => format!("BIT:{}", v),
        Value::TinyInt(v) => format!("TINYINT:{}", v),
        Value::SmallInt(v) => format!("SMALLINT:{}", v),
        Value::Int(v) => format!("INT:{}", v),
        Value::BigInt(v) => format!("BIGINT:{}", v),
        Value::Decimal(raw, scale) => format!("DECIMAL:{}:{}", raw, scale),
        Value::Char(v) => format!("CHAR:{}", v),
        Value::VarChar(v) => format!("VARCHAR:{}", v),
        Value::NChar(v) => format!("NCHAR:{}", v),
        Value::NVarChar(v) => format!("NVARCHAR:{}", v),
        Value::Date(v) => format!("DATE:{}", v),
        Value::Time(v) => format!("TIME:{}", v),
        Value::DateTime(v) => format!("DATETIME:{}", v),
        Value::DateTime2(v) => format!("DATETIME2:{}", v),
        Value::UniqueIdentifier(v) => format!("UNIQUEIDENTIFIER:{}", v),
    }
}

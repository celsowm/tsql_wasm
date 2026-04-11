use crate::error::DbError;
use crate::types::{DataType, Value};
use std::fmt::Debug;
use uuid::Uuid;

use super::super::value_helpers::{pad_binary_right, pad_right, rescale_raw};

pub fn coerce_value_to_type(value: Value, ty: &DataType) -> Result<Value, DbError> {
    coerce_value_to_type_with_dateformat(value, ty, "mdy")
}

pub fn coerce_value_to_type_with_dateformat(
    value: Value,
    ty: &DataType,
    dateformat: &str,
) -> Result<Value, DbError> {
    if matches!(ty, DataType::SqlVariant) {
        return Ok(match value {
            Value::Null => Value::Null,
            Value::SqlVariant(inner) => Value::SqlVariant(inner),
            other => Value::SqlVariant(Box::new(other)),
        });
    }

    let value = match value {
        Value::SqlVariant(inner) => *inner,
        other => other,
    };

    match value {
        Value::Null => Ok(Value::Null),
        Value::Bit(v) => coerce_bit(v, ty),
        Value::TinyInt(v) => coerce_int(v as i64, ty),
        Value::SmallInt(v) => coerce_int(v as i64, ty),
        Value::Int(v) => coerce_int(v as i64, ty),
        Value::BigInt(v) => coerce_int(v, ty),
        Value::Float(v) => coerce_float(v, ty),
        Value::Decimal(raw, scale) => coerce_decimal(raw, scale, ty),
        Value::Money(v) => coerce_money(v, ty),
        Value::SmallMoney(v) => coerce_money(v as i128, ty),
        Value::Char(v) | Value::VarChar(v) | Value::NChar(v) | Value::NVarChar(v) => {
            coerce_string(&v, ty, dateformat)
        }
        Value::Binary(v) | Value::VarBinary(v) => coerce_binary(&v, ty),
        Value::Date(v) => coerce_date_value(v, ty),
        Value::Time(v) => coerce_time_value(v, ty),
        Value::DateTime(v) => coerce_datetime_value(v, ty),
        Value::DateTime2(v) => coerce_datetime_value(v, ty),
        Value::UniqueIdentifier(v) => coerce_uuid_value(v, ty),
        Value::SqlVariant(inner) => coerce_value_to_type_with_dateformat(*inner, ty, dateformat),
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
        DataType::Float => Ok(Value::Float((int_val as f64).to_bits())),
        DataType::Decimal { scale, .. } => Ok(Value::Decimal(int_val as i128, *scale)),
        DataType::Money => Ok(Value::Money(int_val as i128 * 10000)),
        DataType::SmallMoney => Ok(Value::SmallMoney(int_val * 10000)),
        DataType::Char { .. } | DataType::VarChar { .. } => Ok(Value::VarChar(int_val.to_string())),
        DataType::NChar { .. } | DataType::NVarChar { .. } => {
            Ok(Value::NVarChar(int_val.to_string()))
        }
        DataType::Binary { .. } => Ok(Value::Binary(int_val.to_le_bytes().to_vec())),
        DataType::VarBinary { .. } => Ok(Value::VarBinary(int_val.to_le_bytes().to_vec())),
        DataType::DateTime | DataType::DateTime2 | DataType::Date | DataType::Time => Err(
            DbError::Execution(format!("cannot convert bit to {:?}", ty)),
        ),
        DataType::UniqueIdentifier => Err(DbError::Execution(
            "cannot convert bit to UNIQUEIDENTIFIER".into(),
        )),
        DataType::SqlVariant => Ok(Value::SqlVariant(Box::new(Value::Bit(v)))),
        DataType::Xml => Ok(Value::VarChar(int_val.to_string())),
    }
}

fn coerce_int(v: i64, ty: &DataType) -> Result<Value, DbError> {
    match ty {
        DataType::Bit => Ok(Value::Bit(v != 0)),
        DataType::TinyInt => check_int_range::<u8>(v, "TINYINT").map(Value::TinyInt),
        DataType::SmallInt => check_int_range::<i16>(v, "SMALLINT").map(Value::SmallInt),
        DataType::Int => check_int_range::<i32>(v, "INT").map(Value::Int),
        DataType::BigInt => Ok(Value::BigInt(v)),
        DataType::Float => Ok(Value::Float((v as f64).to_bits())),
        DataType::Decimal { scale, .. } => {
            let raw = v as i128 * 10i128.pow(*scale as u32);
            Ok(Value::Decimal(raw, *scale))
        }
        DataType::Money => Ok(Value::Money(v as i128 * 10000)),
        DataType::SmallMoney => Ok(Value::SmallMoney(v * 10000)),
        DataType::Char { .. } | DataType::VarChar { .. } => Ok(Value::VarChar(v.to_string())),
        DataType::NChar { .. } | DataType::NVarChar { .. } => Ok(Value::NVarChar(v.to_string())),
        DataType::Binary { .. } => Ok(Value::Binary(v.to_le_bytes().to_vec())),
        DataType::VarBinary { .. } => Ok(Value::VarBinary(v.to_le_bytes().to_vec())),
        DataType::DateTime | DataType::DateTime2 | DataType::Date | DataType::Time => Err(
            DbError::Execution(format!("cannot convert integer to {:?}", ty)),
        ),
        DataType::UniqueIdentifier => Err(DbError::Execution(
            "cannot convert integer to UNIQUEIDENTIFIER".into(),
        )),
        DataType::SqlVariant => Ok(Value::SqlVariant(Box::new(Value::BigInt(v)))),
        DataType::Xml => Ok(Value::VarChar(v.to_string())),
    }
}

fn coerce_decimal(raw: i128, scale: u8, ty: &DataType) -> Result<Value, DbError> {
    match ty {
        DataType::Decimal { scale: ts, .. } => {
            if *ts == scale {
                Ok(Value::Decimal(raw, scale))
            } else {
                let converted = rescale_raw(raw, scale, *ts);
                Ok(Value::Decimal(converted, *ts))
            }
        }
        DataType::Bit => Ok(Value::Bit(raw != 0)),
        DataType::Float => {
            let divisor = 10f64.powi(scale as i32);
            Ok(Value::Float((raw as f64 / divisor).to_bits()))
        }
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
        DataType::Money => {
            let money_scale = 4u8;
            let converted = rescale_raw(raw, scale, money_scale);
            Ok(Value::Money(converted))
        }
        DataType::SmallMoney => {
            let money_scale = 4u8;
            let converted = rescale_raw(raw, scale, money_scale);
            if converted < i64::MIN as i128 || converted > i64::MAX as i128 {
                Err(DbError::Execution(
                    "Arithmetic overflow error converting DECIMAL to SMALLMONEY".into(),
                ))
            } else {
                Ok(Value::SmallMoney(converted as i64))
            }
        }
        DataType::Char { .. } | DataType::VarChar { .. } => {
            Ok(Value::VarChar(crate::types::format_decimal(raw, scale)))
        }
        DataType::NChar { .. } | DataType::NVarChar { .. } => {
            Ok(Value::NVarChar(crate::types::format_decimal(raw, scale)))
        }
        DataType::SqlVariant => Ok(Value::SqlVariant(Box::new(Value::Decimal(raw, scale)))),
        _ => Err(DbError::Execution(format!(
            "cannot convert DECIMAL to {:?}",
            ty
        ))),
    }
}

fn coerce_float(bits: u64, ty: &DataType) -> Result<Value, DbError> {
    let f = f64::from_bits(bits);
    match ty {
        DataType::Float => Ok(Value::Float(bits)),
        DataType::Bit => Ok(Value::Bit(f != 0.0)),
        DataType::TinyInt => {
            if !(0.0..=255.0).contains(&f) {
                Err(DbError::Execution(
                    "Arithmetic overflow error converting FLOAT to TINYINT".into(),
                ))
            } else {
                Ok(Value::TinyInt(f as u8))
            }
        }
        DataType::SmallInt => {
            if f < i16::MIN as f64 || f > i16::MAX as f64 {
                Err(DbError::Execution(
                    "Arithmetic overflow error converting FLOAT to SMALLINT".into(),
                ))
            } else {
                Ok(Value::SmallInt(f as i16))
            }
        }
        DataType::Int => {
            if f < i32::MIN as f64 || f > i32::MAX as f64 {
                Err(DbError::Execution(
                    "Arithmetic overflow error converting FLOAT to INT".into(),
                ))
            } else {
                Ok(Value::Int(f as i32))
            }
        }
        DataType::BigInt => {
            if f < i64::MIN as f64 || f > i64::MAX as f64 {
                Err(DbError::Execution(
                    "Arithmetic overflow error converting FLOAT to BIGINT".into(),
                ))
            } else {
                Ok(Value::BigInt(f as i64))
            }
        }
        DataType::Decimal { scale, .. } => {
            let raw = (f * 10f64.powi(*scale as i32)).round() as i128;
            Ok(Value::Decimal(raw, *scale))
        }
        DataType::Money => {
            let raw = (f * 10000.0) as i128;
            Ok(Value::Money(raw))
        }
        DataType::SmallMoney => {
            let raw = (f * 10000.0) as i64;
            Ok(Value::SmallMoney(raw))
        }
        DataType::Char { .. } | DataType::VarChar { .. } => {
            Ok(Value::VarChar(crate::types::format_float(f)))
        }
        DataType::NChar { .. } | DataType::NVarChar { .. } => {
            Ok(Value::NVarChar(crate::types::format_float(f)))
        }
        DataType::Binary { .. } => Ok(Value::Binary((f as i64).to_le_bytes().to_vec())),
        DataType::VarBinary { .. } => Ok(Value::VarBinary((f as i64).to_le_bytes().to_vec())),
        DataType::SqlVariant => Ok(Value::SqlVariant(Box::new(Value::Float(bits)))),
        _ => Err(DbError::Execution(format!(
            "cannot convert FLOAT to {:?}",
            ty
        ))),
    }
}

fn coerce_money(raw: i128, ty: &DataType) -> Result<Value, DbError> {
    match ty {
        DataType::Money => Ok(Value::Money(raw)),
        DataType::SmallMoney => {
            if raw < i64::MIN as i128 || raw > i64::MAX as i128 {
                Err(DbError::Execution(
                    "Arithmetic overflow error converting MONEY to SMALLMONEY".into(),
                ))
            } else {
                Ok(Value::SmallMoney(raw as i64))
            }
        }
        DataType::Bit => Ok(Value::Bit(raw != 0)),
        DataType::TinyInt => {
            let v = raw / 10000;
            if !(0..=255).contains(&v) {
                Err(DbError::Execution(
                    "Arithmetic overflow error converting MONEY to TINYINT".into(),
                ))
            } else {
                Ok(Value::TinyInt(v as u8))
            }
        }
        DataType::SmallInt => {
            let v = raw / 10000;
            if v < i16::MIN as i128 || v > i16::MAX as i128 {
                Err(DbError::Execution(
                    "Arithmetic overflow error converting MONEY to SMALLINT".into(),
                ))
            } else {
                Ok(Value::SmallInt(v as i16))
            }
        }
        DataType::Int => {
            let v = raw / 10000;
            if v < i32::MIN as i128 || v > i32::MAX as i128 {
                Err(DbError::Execution(
                    "Arithmetic overflow error converting MONEY to INT".into(),
                ))
            } else {
                Ok(Value::Int(v as i32))
            }
        }
        DataType::BigInt => {
            let v = raw / 10000;
            if v < i64::MIN as i128 || v > i64::MAX as i128 {
                Err(DbError::Execution(
                    "Arithmetic overflow error converting MONEY to BIGINT".into(),
                ))
            } else {
                Ok(Value::BigInt(v as i64))
            }
        }
        DataType::Float => Ok(Value::Float((raw as f64 / 10000.0).to_bits())),
        DataType::Decimal { scale, .. } => {
            let money_scale = 4u8;
            let converted = rescale_raw(raw, money_scale, *scale);
            Ok(Value::Decimal(converted, *scale))
        }
        DataType::Char { .. } | DataType::VarChar { .. } => {
            Ok(Value::VarChar(crate::types::format_money(raw)))
        }
        DataType::NChar { .. } | DataType::NVarChar { .. } => {
            Ok(Value::NVarChar(crate::types::format_money(raw)))
        }
        DataType::Binary { .. } => Ok(Value::Binary(raw.to_le_bytes().to_vec())),
        DataType::VarBinary { .. } => Ok(Value::VarBinary(raw.to_le_bytes().to_vec())),
        DataType::SqlVariant => Ok(Value::SqlVariant(Box::new(Value::Money(raw)))),
        _ => Err(DbError::Execution(format!(
            "cannot convert MONEY to {:?}",
            ty
        ))),
    }
}

fn coerce_string(v: &str, ty: &DataType, dateformat: &str) -> Result<Value, DbError> {
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
        DataType::Float => v
            .parse::<f64>()
            .map(|f| Value::Float(f.to_bits()))
            .map_err(|_| DbError::Execution(format!("cannot convert '{}' to FLOAT", v))),
        DataType::Decimal { scale, .. } => parse_decimal_string(v, *scale),
        DataType::Money => parse_money_string(v),
        DataType::SmallMoney => {
            let m = parse_money_string(v)?;
            match m {
                Value::Money(raw) => {
                    if raw < i64::MIN as i128 || raw > i64::MAX as i128 {
                        Err(DbError::Execution(
                            "Arithmetic overflow error converting to SMALLMONEY".into(),
                        ))
                    } else {
                        Ok(Value::SmallMoney(raw as i64))
                    }
                }
                other => Ok(other),
            }
        }
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
        DataType::Binary { len } => {
            let bytes = if v.starts_with("0x") || v.starts_with("0X") {
                parse_hex_string(&v[2..])?
            } else {
                v.as_bytes().to_vec()
            };
            let padded = pad_binary_right(&bytes, *len as usize);
            Ok(Value::Binary(padded))
        }
        DataType::VarBinary { .. } => {
            let bytes = if v.starts_with("0x") || v.starts_with("0X") {
                parse_hex_string(&v[2..])?
            } else {
                v.as_bytes().to_vec()
            };
            Ok(Value::VarBinary(bytes))
        }
        DataType::Date => {
            let parsed = parse_date_string(v, dateformat)
                .or_else(|_| parse_datetime_string(v, dateformat).map(|dt| dt.date()));
            match parsed {
                Ok(d) => Ok(Value::Date(d)),
                Err(_) => Err(DbError::Execution(format!("invalid date: {}", v))),
            }
        }
        DataType::Time => {
            let parsed = chrono::NaiveTime::parse_from_str(v, "%H:%M:%S")
                .or_else(|_| chrono::NaiveTime::parse_from_str(v, "%H:%M:%S%.f"));
            match parsed {
                Ok(t) => Ok(Value::Time(t)),
                Err(_) => Err(DbError::Execution(format!("invalid time: {}", v))),
            }
        }
        DataType::DateTime | DataType::DateTime2 => {
            let parsed = parse_datetime_string(v, dateformat);
            match parsed {
                Ok(dt) => Ok(Value::DateTime(dt)),
                Err(_) => Err(DbError::Execution(format!("invalid datetime: {}", v))),
            }
        }
        DataType::UniqueIdentifier => {
            let uuid = Uuid::parse_str(v)
                .map_err(|_| DbError::Execution(format!("invalid UNIQUEIDENTIFIER: {}", v)))?;
            Ok(Value::UniqueIdentifier(uuid))
        }
        DataType::SqlVariant => Ok(Value::SqlVariant(Box::new(Value::VarChar(v.to_string())))),
        DataType::Xml => Ok(Value::VarChar(v.to_string())),
    }
}

fn coerce_date_value(v: chrono::NaiveDate, ty: &DataType) -> Result<Value, DbError> {
    match ty {
        DataType::Char { .. } | DataType::VarChar { .. } => {
            Ok(Value::VarChar(v.format("%Y-%m-%d").to_string()))
        }
        DataType::NChar { .. } | DataType::NVarChar { .. } => {
            Ok(Value::NVarChar(v.format("%Y-%m-%d").to_string()))
        }
        DataType::Date => Ok(Value::Date(v)),
        DataType::SqlVariant => Ok(Value::SqlVariant(Box::new(Value::Date(v)))),
        _ => Err(DbError::Execution(format!(
            "cannot convert DATE value to {:?}",
            ty
        ))),
    }
}

fn coerce_time_value(v: chrono::NaiveTime, ty: &DataType) -> Result<Value, DbError> {
    match ty {
        DataType::Char { .. } | DataType::VarChar { .. } => {
            Ok(Value::VarChar(v.format("%H:%M:%S%.f").to_string()))
        }
        DataType::NChar { .. } | DataType::NVarChar { .. } => {
            Ok(Value::NVarChar(v.format("%H:%M:%S%.f").to_string()))
        }
        DataType::Time => Ok(Value::Time(v)),
        DataType::SqlVariant => Ok(Value::SqlVariant(Box::new(Value::Time(v)))),
        _ => Err(DbError::Execution(format!(
            "cannot convert TIME value to {:?}",
            ty
        ))),
    }
}

fn coerce_datetime_value(v: chrono::NaiveDateTime, ty: &DataType) -> Result<Value, DbError> {
    match ty {
        DataType::Char { .. } | DataType::VarChar { .. } => {
            Ok(Value::VarChar(v.format("%Y-%m-%d %H:%M:%S%.f").to_string()))
        }
        DataType::NChar { .. } | DataType::NVarChar { .. } => Ok(Value::NVarChar(
            v.format("%Y-%m-%d %H:%M:%S%.f").to_string(),
        )),
        DataType::DateTime | DataType::DateTime2 => Ok(Value::DateTime(v)),
        DataType::Date => Ok(Value::Date(v.date())),
        DataType::Time => Ok(Value::Time(v.time())),
        DataType::SqlVariant => Ok(Value::SqlVariant(Box::new(Value::DateTime(v)))),
        _ => Err(DbError::Execution(format!(
            "cannot convert DATETIME value to {:?}",
            ty
        ))),
    }
}

fn parse_date_string(v: &str, dateformat: &str) -> Result<chrono::NaiveDate, ()> {
    if let Ok(date) = chrono::NaiveDate::parse_from_str(v, "%Y-%m-%d") {
        return Ok(date);
    }
    if let Ok(date) = chrono::NaiveDate::parse_from_str(v, "%Y/%m/%d") {
        return Ok(date);
    }
    if let Ok(date) = chrono::NaiveDate::parse_from_str(v, "%Y.%m.%d") {
        return Ok(date);
    }

    let fmt = match dateformat.to_ascii_lowercase().as_str() {
        "dmy" => ["%d/%m/%Y", "%d-%m-%Y", "%d.%m.%Y"],
        "ymd" => ["%Y/%m/%d", "%Y-%m-%d", "%Y.%m.%d"],
        "ydm" => ["%Y/%d/%m", "%Y-%d-%m", "%Y.%d.%m"],
        "myd" => ["%m/%Y/%d", "%m-%Y-%d", "%m.%Y.%d"],
        "dym" => ["%d/%Y/%m", "%d-%Y-%m", "%d.%Y.%m"],
        _ => ["%m/%d/%Y", "%m-%d-%Y", "%m.%d.%Y"],
    };

    for candidate in fmt {
        if let Ok(date) = chrono::NaiveDate::parse_from_str(v, candidate) {
            return Ok(date);
        }
    }

    chrono::NaiveDate::parse_from_str(v, "%d/%m/%Y").map_err(|_| ())
}

fn parse_datetime_string(v: &str, dateformat: &str) -> Result<chrono::NaiveDateTime, ()> {
    chrono::NaiveDateTime::parse_from_str(v, "%Y-%m-%d %H:%M:%S")
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(v, "%Y-%m-%dT%H:%M:%S"))
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(v, "%m/%d/%Y %H:%M:%S"))
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(v, "%d/%m/%Y %H:%M:%S"))
        .or_else(|_| {
            parse_date_string(v, dateformat).map(|d| d.and_hms_opt(0, 0, 0).unwrap())
        })
        .map_err(|_| ())
}

#[allow(dead_code)]
fn coerce_date_time_string(v: &str, ty: &DataType) -> Result<Value, DbError> {
    match ty {
        DataType::Char { .. } | DataType::VarChar { .. } => Ok(Value::VarChar(v.to_string())),
        DataType::NChar { .. } | DataType::NVarChar { .. } => Ok(Value::NVarChar(v.to_string())),
        DataType::Date => {
            let parsed = chrono::NaiveDate::parse_from_str(v, "%Y-%m-%d")
                .or_else(|_| chrono::NaiveDate::parse_from_str(v, "%m/%d/%Y"));
            match parsed {
                Ok(d) => Ok(Value::Date(d)),
                Err(_) => Err(DbError::Execution(format!("invalid date: {}", v))),
            }
        }
        DataType::Time => {
            let parsed = chrono::NaiveTime::parse_from_str(v, "%H:%M:%S")
                .or_else(|_| chrono::NaiveTime::parse_from_str(v, "%H:%M:%S%.f"));
            match parsed {
                Ok(t) => Ok(Value::Time(t)),
                Err(_) => Err(DbError::Execution(format!("invalid time: {}", v))),
            }
        }
        DataType::DateTime | DataType::DateTime2 => {
            let parsed = chrono::NaiveDateTime::parse_from_str(v, "%Y-%m-%d %H:%M:%S")
                .or_else(|_| chrono::NaiveDateTime::parse_from_str(v, "%Y-%m-%dT%H:%M:%S"));
            match parsed {
                Ok(dt) => Ok(Value::DateTime(dt)),
                Err(_) => Err(DbError::Execution(format!("invalid datetime: {}", v))),
            }
        }
        DataType::SqlVariant => Ok(Value::SqlVariant(Box::new(Value::VarChar(v.to_string())))),
        _ => Err(DbError::Execution(format!(
            "cannot convert datetime-like value to {:?}",
            ty
        ))),
    }
}

fn coerce_binary(data: &[u8], ty: &DataType) -> Result<Value, DbError> {
    match ty {
        DataType::Bit
        | DataType::TinyInt
        | DataType::SmallInt
        | DataType::Int
        | DataType::BigInt
        | DataType::Float
        | DataType::Decimal { .. }
        | DataType::Money
        | DataType::SmallMoney => {
            let i = parse_binary_to_i64(data)?;
            coerce_int(i, ty)
        }
        DataType::Binary { len } => {
            let padded = pad_binary_right(data, *len as usize);
            Ok(Value::Binary(padded))
        }
        DataType::VarBinary { .. } => Ok(Value::VarBinary(data.to_vec())),
        DataType::Char { .. } | DataType::VarChar { .. } => {
            Ok(Value::VarChar(crate::types::format_binary(data)))
        }
        DataType::NChar { .. } | DataType::NVarChar { .. } => {
            Ok(Value::NVarChar(crate::types::format_binary(data)))
        }
        DataType::SqlVariant => Ok(Value::SqlVariant(Box::new(Value::Binary(data.to_vec())))),
        _ => Err(DbError::Execution(format!(
            "cannot convert BINARY to {:?}",
            ty
        ))),
    }
}

fn parse_binary_to_i64(data: &[u8]) -> Result<i64, DbError> {
    if data.is_empty() {
        return Ok(0);
    }
    if data.len() > 8 {
        return Err(DbError::Execution(
            "cannot convert BINARY longer than 8 bytes to integer".into(),
        ));
    }

    let mut n: u64 = 0;
    for b in data {
        n = (n << 8) | (*b as u64);
    }

    // Sign-extend according to payload width (SQL Server-style binary-to-int casts).
    let bit_width = (data.len() * 8) as u32;
    if bit_width < 64 && (n & (1u64 << (bit_width - 1))) != 0 {
        let mask = (!0u64) << bit_width;
        n |= mask;
    }

    Ok(n as i64)
}

#[allow(dead_code)]
fn coerce_uuid(v: &str, ty: &DataType) -> Result<Value, DbError> {
    let uuid = Uuid::parse_str(v)
        .map_err(|_| DbError::Execution(format!("invalid UNIQUEIDENTIFIER: {}", v)))?;
    match ty {
        DataType::UniqueIdentifier => Ok(Value::UniqueIdentifier(uuid)),
        DataType::Char { .. } | DataType::VarChar { .. } => Ok(Value::VarChar(v.to_string())),
        DataType::NChar { .. } | DataType::NVarChar { .. } => Ok(Value::NVarChar(v.to_string())),
        DataType::SqlVariant => Ok(Value::SqlVariant(Box::new(Value::UniqueIdentifier(uuid)))),
        _ => Err(DbError::Execution(format!(
            "cannot convert UNIQUEIDENTIFIER to {:?}",
            ty
        ))),
    }
}

fn coerce_uuid_value(v: Uuid, ty: &DataType) -> Result<Value, DbError> {
    match ty {
        DataType::Char { .. } | DataType::VarChar { .. } => Ok(Value::VarChar(v.to_string())),
        DataType::NChar { .. } | DataType::NVarChar { .. } => Ok(Value::NVarChar(v.to_string())),
        DataType::UniqueIdentifier => Ok(Value::UniqueIdentifier(v)),
        DataType::SqlVariant => Ok(Value::SqlVariant(Box::new(Value::UniqueIdentifier(v)))),
        _ => Err(DbError::Execution(format!(
            "cannot convert UNIQUEIDENTIFIER to {:?}",
            ty
        ))),
    }
}

pub fn parse_decimal_string(s: &str, scale: u8) -> Result<Value, DbError> {
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

pub fn parse_numeric_literal(s: &str) -> Result<Value, DbError> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Err(DbError::Execution("invalid numeric literal ''".into()));
    }

    if trimmed.contains('e') || trimmed.contains('E') {
        let f = trimmed
            .parse::<f64>()
            .map_err(|_| DbError::Execution(format!("invalid float literal '{}'", s)))?;
        return Ok(Value::Float(f.to_bits()));
    }

    if let Some(dot_idx) = trimmed.find('.') {
        let scale = (trimmed.len() - dot_idx - 1) as u8;
        return parse_decimal_string(trimmed, scale);
    }

    let f = trimmed
        .parse::<f64>()
        .map_err(|_| DbError::Execution(format!("invalid float literal '{}'", s)))?;
    Ok(Value::Float(f.to_bits()))
}

pub fn parse_money_string(s: &str) -> Result<Value, DbError> {
    let trimmed = s.trim().trim_start_matches('$');
    if trimmed.is_empty() {
        return Err(DbError::Execution(
            "cannot convert empty string to MONEY".into(),
        ));
    }
    let scale = 4u8;
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
        .map_err(|_| DbError::Execution(format!("cannot convert '{}' to MONEY", s)))?;

    let mut frac: i128 = 0;
    if !frac_str.is_empty() {
        let truncated = if frac_str.len() > scale as usize {
            &frac_str[..scale as usize]
        } else {
            frac_str
        };
        frac = truncated
            .parse()
            .map_err(|_| DbError::Execution(format!("cannot convert '{}' to MONEY", s)))?;
        if frac_str.len() < scale as usize {
            frac *= 10i128.pow((scale as usize - frac_str.len()) as u32);
        }
    }

    let raw = whole * 10i128.pow(scale as u32) + frac;
    let raw = if negative { -raw } else { raw };
    Ok(Value::Money(raw))
}

pub fn parse_hex_string(s: &str) -> Result<Vec<u8>, DbError> {
    let s = s.trim();
    if !s.len().is_multiple_of(2) {
        return Err(DbError::Execution(
            "hex string must have even number of digits".into(),
        ));
    }
    let mut bytes = Vec::with_capacity(s.len() / 2);
    let chars: Vec<char> = s.chars().collect();
    for i in (0..chars.len()).step_by(2) {
        let hi = hex_char_to_val(chars[i])
            .ok_or_else(|| DbError::Execution(format!("invalid hex digit '{}'", chars[i])))?;
        let lo = hex_char_to_val(chars[i + 1])
            .ok_or_else(|| DbError::Execution(format!("invalid hex digit '{}'", chars[i + 1])))?;
        bytes.push((hi << 4) | lo);
    }
    Ok(bytes)
}

fn hex_char_to_val(c: char) -> Option<u8> {
    match c {
        '0'..='9' => Some(c as u8 - b'0'),
        'a'..='f' => Some(c as u8 - b'a' + 10),
        'A'..='F' => Some(c as u8 - b'A' + 10),
        _ => None,
    }
}

fn check_int_range<T: TryFrom<i64>>(v: i64, type_name: &str) -> Result<T, DbError>
where
    T::Error: Debug,
{
    T::try_from(v).map_err(|_| {
        DbError::Execution(format!(
            "Arithmetic overflow error converting value {} to {}",
            v, type_name
        ))
    })
}

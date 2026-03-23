use std::cmp::Ordering;
use std::fmt::Debug;

use crate::error::DbError;
use crate::types::{DataType, Value};

use super::value_helpers::value_to_f64;

fn to_12hour(h: i32) -> (i32, &'static str) {
    let ampm = if h >= 12 { "PM" } else { "AM" };
    let h12 = match h {
        0 => 12,
        n if n > 12 => n - 12,
        _ => h,
    };
    (h12, ampm)
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueCategory {
    Integer,
    Float,
    Decimal,
    Money,
    String,
    Binary,
    DateTime,
    Uuid,
    Null,
}

fn categorize(v: &Value) -> ValueCategory {
    match v {
        Value::Null => ValueCategory::Null,
        Value::Bit(_) | Value::TinyInt(_) | Value::SmallInt(_) | Value::Int(_) | Value::BigInt(_) => {
            ValueCategory::Integer
        }
        Value::Float(_) => ValueCategory::Float,
        Value::Decimal(_, _) => ValueCategory::Decimal,
        Value::Money(_) | Value::SmallMoney(_) => ValueCategory::Money,
        Value::Char(_) | Value::VarChar(_) | Value::NChar(_) | Value::NVarChar(_) => {
            ValueCategory::String
        }
        Value::Binary(_) | Value::VarBinary(_) => ValueCategory::Binary,
        Value::Date(_) | Value::Time(_) | Value::DateTime(_) | Value::DateTime2(_) => {
            ValueCategory::DateTime
        }
        Value::UniqueIdentifier(_) => ValueCategory::Uuid,
        Value::SqlVariant(inner) => categorize(inner),
    }
}

pub fn convert_with_style(value: Value, ty: &DataType, style: i32) -> Result<Value, DbError> {
    match value {
        Value::Null => Ok(Value::Null),
        Value::Date(ref s)
        | Value::DateTime(ref s)
        | Value::DateTime2(ref s)
        | Value::Time(ref s) => convert_datetime_to_string(s, ty, style),
        Value::VarChar(ref s)
        | Value::NVarChar(ref s)
        | Value::Char(ref s)
        | Value::NChar(ref s) => convert_string_to_datetime(s, ty, style),
        _ => coerce_value_to_type(value, ty),
    }
}

fn convert_datetime_to_string(dt: &str, ty: &DataType, style: i32) -> Result<Value, DbError> {
    let formatted = format_datetime(dt, style);

    match ty {
        DataType::Char { len } => Ok(Value::Char(pad_right(&formatted, *len as usize))),
        DataType::VarChar { .. } => Ok(Value::VarChar(formatted)),
        DataType::NChar { len } => Ok(Value::NChar(pad_right(&formatted, *len as usize))),
        DataType::NVarChar { .. } => Ok(Value::NVarChar(formatted)),
        DataType::SqlVariant => Ok(Value::SqlVariant(Box::new(Value::VarChar(formatted)))),
        _ => coerce_value_to_type(Value::VarChar(formatted), ty),
    }
}

fn format_datetime(dt: &str, style: i32) -> String {
    let (y, mo, d, h, mi, s) = parse_dt_parts(dt);
    match style {
        0 | 100 => "Jan  1 2026 12:00AM".to_string(),
        1 | 101 => {
            let (h12, ampm) = to_12hour(h);
            format!("{:0>2}/{}/{}{:0>2}:{:0>2}:{:0>2} {}", d, mo, y, h12, mi, s, ampm)
        }
        2 | 102 => format!("{}.{:0>2}.{:0>2}", y, mo, d),
        3 | 103 => {
            let (h12, ampm) = to_12hour(h);
            format!("{}/{}/{} {}:{:0>2}:{:0>2} {}", d, mo, y, h12, mi, s, ampm)
        }
        4 | 104 => format!("{}.{:0>2}.{:0>2} {}:{:0>2}:{:0>2}", d, mo, y, h, mi, s),
        5 | 105 => format!("{}-{:0>2}-{:0>2}", y, mo, d),
        6 | 106 => format!("{} {} {}", d, month_abbr(mo), y),
        7 | 107 => {
            let (h12, ampm) = to_12hour(h);
            format!("{} {} {}  {}:{:0>2}:{:0>2} {}", month_abbr(mo), d, y, h12, mi, s, ampm)
        }
        8 | 108 => format!("{}:{:0>2}:{:0>2}", h, mi, s),
        9 | 109 => "Jan  1 2026 12:00:00:000AM".to_string(),
        10 | 110 => {
            let (h12, ampm) = to_12hour(h);
            format!("{}-{:0>2}-{}-{}:{:0>2}:{:0>2} {}", mo, d, y, h12, mi, s, ampm)
        }
        11 | 111 => {
            let (h12, ampm) = to_12hour(h);
            format!("{}/{}/{} {}:{:0>2}:{:0>2} {}", mo, d, y, h12, mi, s, ampm)
        }
        12 | 112 => format!("{}{:0>2}{:0>2}", y, mo, d),
        13 | 113 => "01 Jan 2026 00:00:00:000".to_string(),
        14 | 114 => "00:00:00:000".to_string(),
        20 | 120 => format!("{}-{:0>2}-{:0>2} {:0>2}:{:0>2}:{:0>2}", y, mo, d, h, mi, s),
        21 | 121 => format!(
            "{}-{:0>2}-{:0>2} {:0>2}:{:0>2}:{:0>2}.000",
            y, mo, d, h, mi, s
        ),
        22 | 126 => format!(
            "{}-{:0>2}-{:0>2}T{:0>2}:{:0>2}:{:0>2}.0000000",
            y, mo, d, h, mi, s
        ),
        130 => {
            let month_name = match mo {
                1 => "يناير",
                2 => "فبراير",
                3 => "مارس",
                4 => "أبريل",
                5 => "مايو",
                6 => "يونيو",
                7 => "يوليو",
                8 => "أغسطس",
                9 => "سبتمبر",
                10 => "أكتوبر",
                11 => "نوفمبر",
                12 => "ديسمبر",
                _ => "???",
            };
            format!(
                "{} {} {} {:0>2}:{:0>2}:{:0>2}:000AM",
                d, month_name, y, pad2(h), pad2(mi), pad2(s)
            )
        }
        131 => format!(
            "{}/{:0>2}/{} {}:{:0>2}:{:0>2}AM",
            d, mo, y, pad2(h), pad2(mi), pad2(s)
        ),
        _ => dt.to_string(),
    }
}

fn convert_string_to_datetime(s: &str, ty: &DataType, _style: i32) -> Result<Value, DbError> {
    let normalized = normalize_datetime_string(s);
    match ty {
        DataType::Date => Ok(Value::Date(normalized)),
        DataType::Time => Ok(Value::Time(normalized)),
        DataType::DateTime => Ok(Value::DateTime(normalized)),
        DataType::DateTime2 => Ok(Value::DateTime2(normalized)),
        DataType::SqlVariant => Ok(Value::SqlVariant(Box::new(Value::VarChar(s.to_string())))),
        _ => coerce_value_to_type(Value::VarChar(s.to_string()), ty),
    }
}

fn normalize_datetime_string(s: &str) -> String {
    let date_time: Vec<&str> = s.splitn(2, |c: char| c.is_ascii_whitespace()).collect();
    let date_part = date_time[0];
    let time_part = date_time.get(1).unwrap_or(&"");
    let date_parts: Vec<&str> = date_part.split(|c: char| c == '-' || c == '/').collect();
    if date_parts.len() >= 3 {
        let y = date_parts[0].trim();
        let m = date_parts[1].trim();
        let d = date_parts[2].trim();
        if time_part.is_empty() {
            return format!("{}-{}-{}", y, m, d);
        }
        return format!("{}-{}-{} {}", y, m, d, time_part.trim());
    }
    s.to_string()
}

fn parse_dt_parts(dt: &str) -> (i32, i32, i32, i32, i32, i32) {
    let parts: Vec<&str> = dt
        .split(|c: char| c == '-' || c == '/' || c == ':')
        .collect();
    let y = parts
        .get(0)
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0);
    let mo = parts
        .get(1)
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(1);
    let d_and_t = parts.get(2).unwrap_or(&"1");
    let (d, rest) = if let Some(pos) = d_and_t.find(|c: char| c.is_ascii_whitespace()) {
        (&d_and_t[..pos], d_and_t[pos..].trim())
    } else {
        (*d_and_t, "")
    };
    let d = d.parse().unwrap_or(1);
    let (h, mi, s) = if !rest.is_empty() {
        let tparts: Vec<&str> = rest.split(':').collect();
        (
            tparts
                .get(0)
                .and_then(|s| s.trim().parse().ok())
                .unwrap_or(0),
            tparts
                .get(1)
                .and_then(|s| s.trim().parse().ok())
                .unwrap_or(0),
            tparts
                .get(2)
                .and_then(|s| s.trim().parse::<f64>().ok().map(|f| f as i32))
                .unwrap_or(0),
        )
    } else {
        (0, 0, 0)
    };
    (y, mo, d, h, mi, s)
}

fn month_abbr(m: i32) -> &'static str {
    match m {
        1 => "Jan",
        2 => "Feb",
        3 => "Mar",
        4 => "Apr",
        5 => "May",
        6 => "Jun",
        7 => "Jul",
        8 => "Aug",
        9 => "Sep",
        10 => "Oct",
        11 => "Nov",
        12 => "Dec",
        _ => "???",
    }
}

fn pad2(n: i32) -> String {
    format!("{:0>2}", n)
}

pub fn coerce_value_to_type(value: Value, ty: &DataType) -> Result<Value, DbError> {
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
            coerce_string(&v, ty)
        }
        Value::Binary(v) | Value::VarBinary(v) => coerce_binary(&v, ty),
        Value::Date(v) => coerce_date_time_string(&v, ty),
        Value::Time(v) => coerce_date_time_string(&v, ty),
        Value::DateTime(v) => coerce_date_time_string(&v, ty),
        Value::DateTime2(v) => coerce_date_time_string(&v, ty),
        Value::UniqueIdentifier(v) => coerce_uuid(&v, ty),
        Value::SqlVariant(inner) => coerce_value_to_type(*inner, ty),
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
        DataType::Binary { .. } | DataType::VarBinary { .. } => {
            Ok(Value::Binary(int_val.to_le_bytes().to_vec()))
        }
        DataType::DateTime | DataType::DateTime2 | DataType::Date | DataType::Time => Err(
            DbError::Execution(format!("cannot convert bit to {:?}", ty)),
        ),
        DataType::UniqueIdentifier => Err(DbError::Execution(
            "cannot convert bit to UNIQUEIDENTIFIER".into(),
        )),
        DataType::SqlVariant => Ok(Value::SqlVariant(Box::new(Value::Bit(v)))),
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
        DataType::Binary { .. } | DataType::VarBinary { .. } => {
            Ok(Value::Binary(v.to_le_bytes().to_vec()))
        }
        DataType::DateTime | DataType::DateTime2 | DataType::Date | DataType::Time => Err(
            DbError::Execution(format!("cannot convert integer to {:?}", ty)),
        ),
        DataType::UniqueIdentifier => Err(DbError::Execution(
            "cannot convert integer to UNIQUEIDENTIFIER".into(),
        )),
        DataType::SqlVariant => Ok(Value::SqlVariant(Box::new(Value::BigInt(v)))),
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
            let converted = if scale == money_scale {
                raw
            } else {
                rescale_decimal(raw, scale, money_scale)
            };
            Ok(Value::Money(converted))
        }
        DataType::SmallMoney => {
            let money_scale = 4u8;
            let converted = if scale == money_scale {
                raw
            } else {
                rescale_decimal(raw, scale, money_scale)
            };
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
        DataType::Date => Ok(Value::Date(v.to_string())),
        DataType::Time => Ok(Value::Time(v.to_string())),
        DataType::DateTime => Ok(Value::DateTime(v.to_string())),
        DataType::DateTime2 => Ok(Value::DateTime2(v.to_string())),
        DataType::UniqueIdentifier => Ok(Value::UniqueIdentifier(v.to_string())),
        DataType::SqlVariant => Ok(Value::SqlVariant(Box::new(Value::VarChar(v.to_string())))),
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
        DataType::SqlVariant => Ok(Value::SqlVariant(Box::new(Value::VarChar(v.to_string())))),
        _ => Err(DbError::Execution(format!(
            "cannot convert datetime-like value to {:?}",
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
            if f < 0.0 || f > 255.0 {
                Err(DbError::Execution("Arithmetic overflow error converting FLOAT to TINYINT".into()))
            } else {
                Ok(Value::TinyInt(f as u8))
            }
        }
        DataType::SmallInt => {
            if f < i16::MIN as f64 || f > i16::MAX as f64 {
                Err(DbError::Execution("Arithmetic overflow error converting FLOAT to SMALLINT".into()))
            } else {
                Ok(Value::SmallInt(f as i16))
            }
        }
        DataType::Int => {
            if f < i32::MIN as f64 || f > i32::MAX as f64 {
                Err(DbError::Execution("Arithmetic overflow error converting FLOAT to INT".into()))
            } else {
                Ok(Value::Int(f as i32))
            }
        }
        DataType::BigInt => {
            if f < i64::MIN as f64 || f > i64::MAX as f64 {
                Err(DbError::Execution("Arithmetic overflow error converting FLOAT to BIGINT".into()))
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
        DataType::SqlVariant => Ok(Value::SqlVariant(Box::new(Value::Float(bits)))),
        _ => Err(DbError::Execution(format!("cannot convert FLOAT to {:?}", ty))),
    }
}

fn coerce_money(raw: i128, ty: &DataType) -> Result<Value, DbError> {
    match ty {
        DataType::Money => Ok(Value::Money(raw)),
        DataType::SmallMoney => {
            if raw < i64::MIN as i128 || raw > i64::MAX as i128 {
                Err(DbError::Execution("Arithmetic overflow error converting MONEY to SMALLMONEY".into()))
            } else {
                Ok(Value::SmallMoney(raw as i64))
            }
        }
        DataType::Bit => Ok(Value::Bit(raw != 0)),
        DataType::TinyInt => {
            let v = raw / 10000;
            if !(0..=255).contains(&v) {
                Err(DbError::Execution("Arithmetic overflow error converting MONEY to TINYINT".into()))
            } else {
                Ok(Value::TinyInt(v as u8))
            }
        }
        DataType::SmallInt => {
            let v = raw / 10000;
            if v < i16::MIN as i128 || v > i16::MAX as i128 {
                Err(DbError::Execution("Arithmetic overflow error converting MONEY to SMALLINT".into()))
            } else {
                Ok(Value::SmallInt(v as i16))
            }
        }
        DataType::Int => {
            let v = raw / 10000;
            if v < i32::MIN as i128 || v > i32::MAX as i128 {
                Err(DbError::Execution("Arithmetic overflow error converting MONEY to INT".into()))
            } else {
                Ok(Value::Int(v as i32))
            }
        }
        DataType::BigInt => {
            let v = raw / 10000;
            if v < i64::MIN as i128 || v > i64::MAX as i128 {
                Err(DbError::Execution("Arithmetic overflow error converting MONEY to BIGINT".into()))
            } else {
                Ok(Value::BigInt(v as i64))
            }
        }
        DataType::Float => {
            Ok(Value::Float((raw as f64 / 10000.0).to_bits()))
        }
        DataType::Decimal { scale, .. } => {
            let money_scale = 4u8;
            if *scale == money_scale {
                Ok(Value::Decimal(raw, *scale))
            } else {
                let converted = rescale_decimal(raw, money_scale, *scale);
                Ok(Value::Decimal(converted, *scale))
            }
        }
        DataType::Char { .. } | DataType::VarChar { .. } => {
            Ok(Value::VarChar(crate::types::format_money(raw)))
        }
        DataType::NChar { .. } | DataType::NVarChar { .. } => {
            Ok(Value::NVarChar(crate::types::format_money(raw)))
        }
        DataType::SqlVariant => Ok(Value::SqlVariant(Box::new(Value::Money(raw)))),
        _ => Err(DbError::Execution(format!("cannot convert MONEY to {:?}", ty))),
    }
}

fn coerce_binary(data: &[u8], ty: &DataType) -> Result<Value, DbError> {
    match ty {
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
        _ => Err(DbError::Execution(format!("cannot convert BINARY to {:?}", ty))),
    }
}

fn coerce_uuid(v: &str, ty: &DataType) -> Result<Value, DbError> {
    match ty {
        DataType::UniqueIdentifier => Ok(Value::UniqueIdentifier(v.to_string())),
        DataType::Char { .. } | DataType::VarChar { .. } => Ok(Value::VarChar(v.to_string())),
        DataType::NChar { .. } | DataType::NVarChar { .. } => Ok(Value::NVarChar(v.to_string())),
        DataType::SqlVariant => Ok(Value::SqlVariant(Box::new(Value::UniqueIdentifier(
            v.to_string(),
        )))),
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

pub(crate) fn parse_money_string(s: &str) -> Result<Value, DbError> {
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

pub(crate) fn parse_hex_string(s: &str) -> Result<Vec<u8>, DbError> {
    let s = s.trim();
    if s.len() % 2 != 0 {
        return Err(DbError::Execution("hex string must have even number of digits".into()));
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

fn pad_binary_right(data: &[u8], len: usize) -> Vec<u8> {
    if data.len() >= len {
        data[..len].to_vec()
    } else {
        let mut v = data.to_vec();
        v.resize(len, 0);
        v
    }
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

        (ValueCategory::Float, ValueCategory::Float) => {
            let af = value_to_f64(&a).unwrap_or(0.0);
            let bf = value_to_f64(&b).unwrap_or(0.0);
            af.partial_cmp(&bf).unwrap_or(Ordering::Equal)
        }

        (ValueCategory::Float, ValueCategory::Integer) | (ValueCategory::Integer, ValueCategory::Float) => {
            let af = value_to_f64(&a).unwrap_or(0.0);
            let bf = value_to_f64(&b).unwrap_or(0.0);
            af.partial_cmp(&bf).unwrap_or(Ordering::Equal)
        }

        (ValueCategory::Float, ValueCategory::Decimal) | (ValueCategory::Decimal, ValueCategory::Float) => {
            let af = value_to_f64(&a).unwrap_or(0.0);
            let bf = value_to_f64(&b).unwrap_or(0.0);
            af.partial_cmp(&bf).unwrap_or(Ordering::Equal)
        }

        (ValueCategory::Decimal, ValueCategory::Decimal) | (ValueCategory::Decimal, ValueCategory::Integer) | (ValueCategory::Integer, ValueCategory::Decimal) => {
            let (a_dec, b_dec) = to_comparable_decimals(&a, &b);
            a_dec.cmp(&b_dec)
        }

        (ValueCategory::Money, ValueCategory::Money) => {
            let am = extract_money_raw(&a);
            let bm = extract_money_raw(&b);
            am.cmp(&bm)
        }

        (ValueCategory::Money, ValueCategory::Integer) | (ValueCategory::Integer, ValueCategory::Money) => {
            let am = extract_money_raw(&a);
            let bm = extract_money_raw(&b);
            am.cmp(&bm)
        }

        (ValueCategory::Money, ValueCategory::Decimal) | (ValueCategory::Decimal, ValueCategory::Money) => {
            let am = extract_money_raw(&a);
            let bm = extract_money_raw(&b);
            am.cmp(&bm)
        }

        (ValueCategory::Money, ValueCategory::Float) | (ValueCategory::Float, ValueCategory::Money) => {
            let af = value_to_f64(&a).unwrap_or(0.0);
            let bf = value_to_f64(&b).unwrap_or(0.0);
            af.partial_cmp(&bf).unwrap_or(Ordering::Equal)
        }

        (ValueCategory::String, ValueCategory::String) => {
            extract_string(&a).cmp(extract_string(&b))
        }

        (ValueCategory::Integer, ValueCategory::String) | (ValueCategory::Decimal, ValueCategory::String) | (ValueCategory::Float, ValueCategory::String) | (ValueCategory::Money, ValueCategory::String) => {
            compare_numeric_with_string(&a, &b)
        }

        (ValueCategory::String, ValueCategory::Integer) | (ValueCategory::String, ValueCategory::Decimal) | (ValueCategory::String, ValueCategory::Float) | (ValueCategory::String, ValueCategory::Money) => {
            compare_numeric_with_string(&a, &b)
        }

        (ValueCategory::DateTime, ValueCategory::DateTime) => {
            extract_string(&a).cmp(extract_string(&b))
        }

        (ValueCategory::DateTime, ValueCategory::String) | (ValueCategory::String, ValueCategory::DateTime) => {
            a.to_string_value().cmp(&b.to_string_value())
        }

        (ValueCategory::Uuid, ValueCategory::Uuid) => {
            extract_string(&a).cmp(extract_string(&b))
        }

        (ValueCategory::Binary, ValueCategory::Binary) => {
            extract_bytes(&a).cmp(extract_bytes(&b))
        }

        _ => value_key(&a).cmp(&value_key(&b)),
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
    if let Some((ar, as_)) = parse_string_as_numeric(num_str) {
        let str_parsed = parse_string_as_numeric(extract_string(str_val));
        if let Some((br, bs)) = str_parsed {
            let (an, bn) = normalize_decimals(ar, as_, br, bs);
            return an.cmp(&bn);
        }
    }
    num.to_string_value().cmp(&str_val.to_string_value())
}

fn extract_string(v: &Value) -> &str {
    match v {
        Value::Char(s) | Value::VarChar(s) | Value::NChar(s) | Value::NVarChar(s) => s,
        Value::Date(s) | Value::Time(s) | Value::DateTime(s) | Value::DateTime2(s) => s,
        _ => "",
    }
}

fn extract_bytes(v: &Value) -> &[u8] {
    match v {
        Value::Binary(b) | Value::VarBinary(b) => b,
        _ => &[],
    }
}

fn extract_money_raw(v: &Value) -> i128 {
    match v {
        Value::Money(r) => *r,
        Value::SmallMoney(r) => *r as i128,
        Value::Decimal(raw, scale) => {
            // Convert to money-scale (4) for comparison
            rescale_decimal(*raw, *scale, 4)
        }
        Value::Int(v) => *v as i128 * 10000,
        Value::BigInt(v) => *v as i128 * 10000,
        Value::TinyInt(v) => *v as i128 * 10000,
        Value::SmallInt(v) => *v as i128 * 10000,
        _ => 0,
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
        Value::Date(_)
        | Value::Time(_)
        | Value::DateTime(_)
        | Value::DateTime2(_)
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
        Value::Date(v) => format!("DATE:{}", v),
        Value::Time(v) => format!("TIME:{}", v),
        Value::DateTime(v) => format!("DATETIME:{}", v),
        Value::DateTime2(v) => format!("DATETIME2:{}", v),
        Value::UniqueIdentifier(v) => format!("UNIQUEIDENTIFIER:{}", v),
        Value::SqlVariant(inner) => format!("SQL_VARIANT:{}", value_key(inner)),
    }
}

use crate::error::DbError;
use crate::types::{DataType, Value};
use std::fmt::Debug;
use crate::executor::value_helpers::rescale_raw;

pub(crate) fn coerce_int(v: i64, ty: &DataType) -> Result<Value, DbError> {
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
        DataType::DateTime
        | DataType::DateTime2
        | DataType::SmallDateTime
        | DataType::DateTimeOffset
        | DataType::Date
        | DataType::Time => Err(DbError::Execution(format!(
            "cannot convert integer to {:?}",
            ty
        ))),
        DataType::UniqueIdentifier => Err(DbError::Execution(
            "cannot convert integer to UNIQUEIDENTIFIER".into(),
        )),
        DataType::Vector { .. } => Err(DbError::Execution(format!(
            "cannot convert integer to {:?}",
            ty
        ))),
        DataType::SqlVariant => Ok(Value::SqlVariant(Box::new(Value::BigInt(v)))),
        DataType::Xml => Ok(Value::VarChar(v.to_string())),
    }
}

pub(crate) fn coerce_decimal(raw: i128, scale: u8, ty: &DataType) -> Result<Value, DbError> {
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

pub(crate) fn coerce_float(bits: u64, ty: &DataType) -> Result<Value, DbError> {
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

pub(crate) fn coerce_money(raw: i128, ty: &DataType) -> Result<Value, DbError> {
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

pub(crate) fn check_int_range<T: TryFrom<i64>>(v: i64, type_name: &str) -> Result<T, DbError>
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

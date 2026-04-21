use crate::error::DbError;
use crate::types::{DataType, Value, parse_vector_literal};
use crate::executor::value_helpers::{pad_binary_right, pad_right};
use crate::executor::value_ops::formatting::parse_datetime_string;
use super::numeric::{parse_money_string, parse_decimal_string};
use uuid::Uuid;
use super::datetime::parse_date_string;

pub(crate) fn coerce_string(v: &str, ty: &DataType, dateformat: &str) -> Result<Value, DbError> {
    match ty {
        DataType::Bit => Ok(Value::Bit(v != "0" && !v.is_empty())),
        DataType::TinyInt => v
            .parse::<u8>()
            .map(Value::TinyInt)
            .map_err(|_| DbError::conversion_failed("varchar", v, "tinyint")),
        DataType::SmallInt => v
            .parse::<i16>()
            .map(Value::SmallInt)
            .map_err(|_| DbError::conversion_failed("varchar", v, "smallint")),
        DataType::Int => v
            .parse::<i32>()
            .map(Value::Int)
            .map_err(|_| DbError::conversion_failed("varchar", v, "int")),
        DataType::BigInt => v
            .parse::<i64>()
            .map(Value::BigInt)
            .map_err(|_| DbError::conversion_failed("varchar", v, "bigint")),
        DataType::Float => v
            .parse::<f64>()
            .map(|f| Value::Float(f.to_bits()))
            .map_err(|_| DbError::conversion_failed("varchar", v, "float")),
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
        DataType::Vector { dimensions } => {
            let bits = parse_vector_literal(v)?;
            if bits.len() != *dimensions as usize {
                return Err(DbError::Execution(format!(
                    "vector dimension mismatch: expected {}, got {}",
                    dimensions,
                    bits.len()
                )));
            }
            Ok(Value::Vector(bits))
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
        DataType::DateTime
        | DataType::DateTime2
        | DataType::SmallDateTime
        | DataType::DateTimeOffset => {
            let parsed = parse_datetime_string(v, dateformat);
            match parsed {
                Ok(dt) => Ok(match ty {
                    DataType::DateTimeOffset => Value::DateTimeOffset(v.to_string()),
                    DataType::SmallDateTime => Value::SmallDateTime(dt),
                    _ => Value::DateTime(dt),
                }),
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

pub(crate) fn hex_char_to_val(c: char) -> Option<u8> {
    match c {
        '0'..='9' => Some(c as u8 - b'0'),
        'a'..='f' => Some(c as u8 - b'a' + 10),
        'A'..='F' => Some(c as u8 - b'A' + 10),
        _ => None,
    }
}

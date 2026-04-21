use crate::error::DbError;
use crate::types::{DataType, Value};
use crate::executor::value_helpers::pad_binary_right;
use uuid::Uuid;
use super::numeric::coerce_int;

pub(crate) fn coerce_binary(data: &[u8], ty: &DataType) -> Result<Value, DbError> {
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
        DataType::UniqueIdentifier => {
            if data.len() == 16 {
                let arr: [u8; 16] = data[..16].try_into().map_err(|_| {
                    DbError::Execution("cannot convert BINARY to UNIQUEIDENTIFIER".into())
                })?;
                let uuid = uuid::Uuid::from_bytes_le(arr);
                Ok(Value::UniqueIdentifier(uuid))
            } else {
                Err(DbError::Execution(
                    "cannot convert BINARY to UNIQUEIDENTIFIER: expected 16 bytes".into(),
                ))
            }
        }
        DataType::DateTime
        | DataType::DateTime2
        | DataType::SmallDateTime
        | DataType::DateTimeOffset
        | DataType::Date
        | DataType::Time => Err(DbError::Execution(format!(
            "cannot convert BINARY to {:?}",
            ty
        ))),
        _ => Err(DbError::Execution(format!(
            "cannot convert BINARY to {:?}",
            ty
        ))),
    }
}

pub(crate) fn parse_binary_to_i64(data: &[u8]) -> Result<i64, DbError> {
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

pub(crate) fn coerce_uuid_value(v: Uuid, ty: &DataType) -> Result<Value, DbError> {
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

use crate::error::DbError;
use crate::types::{DataType, Value};

use super::super::value_helpers::pad_right;
pub mod numeric;
pub mod string;
pub mod datetime;
pub mod binary;

use numeric::*;
use string::*;
use datetime::*;
use binary::*;

pub fn coerce_value_to_type(value: Value, ty: &DataType) -> Result<Value, DbError> {
    coerce_value_to_type_with_dateformat(value, ty, "mdy")
}

pub fn coerce_value_to_type_with_dateformat(
    value: Value,
    ty: &DataType,
    dateformat: &str,
) -> Result<Value, DbError> {
    if matches!(ty, DataType::SqlVariant) {
        let nested = match &value {
            Value::SqlVariant(inner) => inner.as_ref(),
            other => other,
        };
        if matches!(nested, Value::Vector(_)) {
            return Err(DbError::Execution(
                "cannot convert VECTOR to SQL_VARIANT".into(),
            ));
        }
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
        Value::Vector(bits) => coerce_vector(Value::Vector(bits), ty),
        Value::Date(v) => coerce_date_value(v, ty),
        Value::Time(v) => coerce_time_value(v, ty),
        Value::DateTime(v) => coerce_datetime_value(v, ty),
        Value::DateTime2(v) => coerce_datetime_value(v, ty),
        Value::SmallDateTime(v) => coerce_datetime_value(v, ty),
        Value::DateTimeOffset(v) => coerce_string(&v, ty, dateformat),
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
        DataType::DateTime
        | DataType::DateTime2
        | DataType::SmallDateTime
        | DataType::DateTimeOffset
        | DataType::Date
        | DataType::Time => Err(DbError::Execution(format!(
            "cannot convert bit to {:?}",
            ty
        ))),
        DataType::UniqueIdentifier => Err(DbError::Execution(
            "cannot convert bit to UNIQUEIDENTIFIER".into(),
        )),
        DataType::Vector { .. } => Err(DbError::Execution(format!(
            "cannot convert bit to {:?}",
            ty
        ))),
        DataType::SqlVariant => Ok(Value::SqlVariant(Box::new(Value::Bit(v)))),
        DataType::Xml => Ok(Value::VarChar(int_val.to_string())),
    }
}






fn coerce_vector(value: Value, ty: &DataType) -> Result<Value, DbError> {
    match (value, ty) {
        (Value::Vector(bits), DataType::Vector { dimensions }) => {
            if bits.len() != *dimensions as usize {
                Err(DbError::Execution(format!(
                    "vector dimension mismatch: expected {}, got {}",
                    dimensions,
                    bits.len()
                )))
            } else {
                Ok(Value::Vector(bits))
            }
        }
        (Value::Vector(bits), DataType::Char { len }) => Ok(Value::Char(pad_right(
            &crate::types::format_vector(&bits),
            *len as usize,
        ))),
        (Value::Vector(bits), DataType::VarChar { .. }) => {
            Ok(Value::VarChar(crate::types::format_vector(&bits)))
        }
        (Value::Vector(bits), DataType::NChar { len }) => Ok(Value::NChar(pad_right(
            &crate::types::format_vector(&bits),
            *len as usize,
        ))),
        (Value::Vector(bits), DataType::NVarChar { .. }) => {
            Ok(Value::NVarChar(crate::types::format_vector(&bits)))
        }
        (Value::Vector(_), DataType::SqlVariant) => Err(DbError::Execution(
            "cannot convert VECTOR to SQL_VARIANT".into(),
        )),
        (Value::Vector(_), other) => Err(DbError::Execution(format!(
            "cannot convert VECTOR to {:?}",
            other
        ))),
        (other, _) => Err(DbError::Execution(format!(
            "cannot convert {:?} to VECTOR",
            other.data_type()
        ))),
    }
}

use std::cmp::Ordering;

use crate::error::DbError;
use crate::types::{DataType, Value};

pub(crate) fn coerce_value_to_type(value: Value, ty: &DataType) -> Result<Value, DbError> {
    match value {
        Value::Null => Ok(Value::Null),
        Value::Int(v) => match ty {
            DataType::Bit => Ok(Value::Bit(v != 0)),
            DataType::Int => Ok(Value::Int(v)),
            DataType::BigInt => Ok(Value::BigInt(v as i64)),
            DataType::VarChar { .. } => Ok(Value::VarChar(v.to_string())),
            DataType::NVarChar { .. } => Ok(Value::NVarChar(v.to_string())),
            DataType::DateTime => Err(DbError::Execution("cannot convert integer to DATETIME yet".into())),
        },
        Value::BigInt(v) => match ty {
            DataType::Bit => Ok(Value::Bit(v != 0)),
            DataType::Int => Ok(Value::Int(v as i32)),
            DataType::BigInt => Ok(Value::BigInt(v)),
            DataType::VarChar { .. } => Ok(Value::VarChar(v.to_string())),
            DataType::NVarChar { .. } => Ok(Value::NVarChar(v.to_string())),
            DataType::DateTime => Err(DbError::Execution("cannot convert bigint to DATETIME yet".into())),
        },
        Value::Bit(v) => match ty {
            DataType::Bit => Ok(Value::Bit(v)),
            DataType::Int => Ok(Value::Int(if v { 1 } else { 0 })),
            DataType::BigInt => Ok(Value::BigInt(if v { 1 } else { 0 })),
            DataType::VarChar { .. } => Ok(Value::VarChar((if v { 1 } else { 0 }).to_string())),
            DataType::NVarChar { .. } => Ok(Value::NVarChar((if v { 1 } else { 0 }).to_string())),
            DataType::DateTime => Err(DbError::Execution("cannot convert bit to DATETIME yet".into())),
        },
        Value::VarChar(v) => match ty {
            DataType::Bit => Ok(Value::Bit(v != "0" && !v.is_empty())),
            DataType::Int => v
                .parse::<i32>()
                .map(Value::Int)
                .map_err(|_| DbError::Execution(format!("cannot convert '{}' to INT", v))),
            DataType::BigInt => v
                .parse::<i64>()
                .map(Value::BigInt)
                .map_err(|_| DbError::Execution(format!("cannot convert '{}' to BIGINT", v))),
            DataType::VarChar { .. } => Ok(Value::VarChar(v)),
            DataType::NVarChar { .. } => Ok(Value::NVarChar(v)),
            DataType::DateTime => Ok(Value::DateTime(v)),
        },
        Value::NVarChar(v) => match ty {
            DataType::Bit => Ok(Value::Bit(v != "0" && !v.is_empty())),
            DataType::Int => v
                .parse::<i32>()
                .map(Value::Int)
                .map_err(|_| DbError::Execution(format!("cannot convert '{}' to INT", v))),
            DataType::BigInt => v
                .parse::<i64>()
                .map(Value::BigInt)
                .map_err(|_| DbError::Execution(format!("cannot convert '{}' to BIGINT", v))),
            DataType::VarChar { .. } => Ok(Value::VarChar(v)),
            DataType::NVarChar { .. } => Ok(Value::NVarChar(v)),
            DataType::DateTime => Ok(Value::DateTime(v)),
        },
        Value::DateTime(v) => match ty {
            DataType::DateTime => Ok(Value::DateTime(v)),
            DataType::VarChar { .. } => Ok(Value::VarChar(v)),
            DataType::NVarChar { .. } => Ok(Value::NVarChar(v)),
            _ => Err(DbError::Execution(format!("cannot convert datetime to {:?}", ty))),
        },
    }
}

pub(crate) fn compare_values(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        (Value::Int(x), Value::Int(y)) => x.cmp(y),
        (Value::Int(x), Value::BigInt(y)) => (*x as i64).cmp(y),
        (Value::BigInt(x), Value::Int(y)) => x.cmp(&(*y as i64)),
        (Value::BigInt(x), Value::BigInt(y)) => x.cmp(y),
        (Value::Bit(x), Value::Bit(y)) => x.cmp(y),
        (Value::VarChar(x), Value::VarChar(y)) => x.cmp(y),
        (Value::NVarChar(x), Value::NVarChar(y)) => x.cmp(y),
        (Value::VarChar(x), Value::NVarChar(y)) => x.cmp(y),
        (Value::NVarChar(x), Value::VarChar(y)) => x.cmp(y),
        (Value::DateTime(x), Value::DateTime(y)) => x.cmp(y),
        _ => value_key(a).cmp(&value_key(b)),
    }
}

pub(crate) fn truthy(value: &Value) -> bool {
    match value {
        Value::Bit(v) => *v,
        Value::Int(v) => *v != 0,
        Value::BigInt(v) => *v != 0,
        Value::VarChar(v) | Value::NVarChar(v) => !v.is_empty(),
        Value::DateTime(_) => true,
        Value::Null => false,
    }
}

pub(crate) fn value_key(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        Value::Bit(v) => format!("BIT:{}", v),
        Value::Int(v) => format!("INT:{}", v),
        Value::BigInt(v) => format!("BIGINT:{}", v),
        Value::VarChar(v) => format!("VARCHAR:{}", v),
        Value::NVarChar(v) => format!("NVARCHAR:{}", v),
        Value::DateTime(v) => format!("DATETIME:{}", v),
    }
}

use crate::error::DbError;
use crate::types::Value;

pub(crate) fn is_string_type(v: &Value) -> bool {
    matches!(
        v,
        Value::Char(_) | Value::VarChar(_) | Value::NChar(_) | Value::NVarChar(_)
    )
}

pub(crate) fn to_i64(v: &Value) -> Result<i64, DbError> {
    match v {
        Value::Bit(b) => Ok(if *b { 1 } else { 0 }),
        Value::TinyInt(v) => Ok(*v as i64),
        Value::SmallInt(v) => Ok(*v as i64),
        Value::Int(v) => Ok(*v as i64),
        Value::BigInt(v) => Ok(*v),
        Value::Decimal(raw, scale) => {
            let divisor = 10i128.pow(*scale as u32);
            Ok((*raw / divisor) as i64)
        }
        _ => Err(DbError::Execution(format!(
            "cannot convert {:?} to integer",
            v.data_type()
        ))),
    }
}

pub(crate) fn to_decimal_parts(v: &Value) -> (i128, u8) {
    match v {
        Value::Decimal(raw, scale) => (*raw, *scale),
        Value::Bit(b) => (if *b { 1 } else { 0 }, 0),
        Value::TinyInt(v) => (*v as i128, 0),
        Value::SmallInt(v) => (*v as i128, 0),
        Value::Int(v) => (*v as i128, 0),
        Value::BigInt(v) => (*v as i128, 0),
        _ => (0, 0),
    }
}

pub(crate) fn rescale_raw(raw: i128, from_scale: u8, to_scale: u8) -> i128 {
    if from_scale == to_scale {
        return raw;
    }
    if to_scale > from_scale {
        raw * 10i128.pow((to_scale - from_scale) as u32)
    } else {
        raw / 10i128.pow((from_scale - to_scale) as u32)
    }
}

pub(crate) fn value_to_f64(v: &Value) -> Result<f64, DbError> {
    match v {
        Value::TinyInt(n) => Ok(*n as f64),
        Value::SmallInt(n) => Ok(*n as f64),
        Value::Int(n) => Ok(*n as f64),
        Value::BigInt(n) => Ok(*n as f64),
        Value::Decimal(raw, scale) => {
            let divisor = 10f64.powi(*scale as i32);
            Ok(*raw as f64 / divisor)
        }
        Value::VarChar(s) | Value::NVarChar(s) => s
            .parse::<f64>()
            .map_err(|_| DbError::Execution(format!("cannot convert '{}' to float", s))),
        _ => Err(DbError::Execution(format!(
            "cannot convert {:?} to float",
            v.data_type()
        ))),
    }
}

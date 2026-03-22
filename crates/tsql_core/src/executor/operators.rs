use std::cmp::Ordering;

use crate::ast::{BinaryOp, UnaryOp};
use crate::error::DbError;
use crate::types::Value;

use super::value_helpers::{is_string_type, rescale_raw, to_decimal_parts, to_i64};
use super::value_ops::{compare_values, truthy};

pub(crate) fn eval_binary(op: &BinaryOp, lv: Value, rv: Value, ansi_nulls: bool) -> Result<Value, DbError> {
    match op {
        BinaryOp::Eq => Ok(compare_bool(lv, rv, |o| o == Ordering::Equal, ansi_nulls)),
        BinaryOp::NotEq => Ok(compare_bool(lv, rv, |o| o != Ordering::Equal, ansi_nulls)),
        BinaryOp::Gt => Ok(compare_bool(lv, rv, |o| o == Ordering::Greater, ansi_nulls)),
        BinaryOp::Lt => Ok(compare_bool(lv, rv, |o| o == Ordering::Less, ansi_nulls)),
        BinaryOp::Gte => Ok(compare_bool(lv, rv, |o| {
            matches!(o, Ordering::Greater | Ordering::Equal)
        }, ansi_nulls)),
        BinaryOp::Lte => Ok(compare_bool(lv, rv, |o| {
            matches!(o, Ordering::Less | Ordering::Equal)
        }, ansi_nulls)),
        BinaryOp::And => eval_and(lv, rv),
        BinaryOp::Or => eval_or(lv, rv),
        BinaryOp::Add => eval_add(lv, rv),
        BinaryOp::Subtract => eval_subtract(lv, rv),
        BinaryOp::Multiply => eval_multiply(lv, rv),
        BinaryOp::Divide => eval_divide(lv, rv),
        BinaryOp::Modulo => eval_modulo(lv, rv),
    }
}

pub(crate) fn eval_unary(op: &UnaryOp, val: Value) -> Result<Value, DbError> {
    if val.is_null() {
        return Ok(Value::Null);
    }
    match op {
        UnaryOp::Negate => match val {
            Value::TinyInt(v) => Ok(Value::SmallInt(-(v as i16))),
            Value::SmallInt(v) => Ok(Value::SmallInt(-v)),
            Value::Int(v) => Ok(Value::Int(-v)),
            Value::BigInt(v) => Ok(Value::BigInt(-v)),
            Value::Decimal(raw, scale) => Ok(Value::Decimal(-raw, scale)),
            _ => Err(DbError::Execution(format!(
                "cannot negate value of type {:?}",
                val.data_type()
            ))),
        },
        UnaryOp::Not => Ok(Value::Bit(!truthy(&val))),
    }
}

fn eval_add(lv: Value, rv: Value) -> Result<Value, DbError> {
    if lv.is_null() || rv.is_null() {
        return Ok(Value::Null);
    }
    if is_string_type(&lv) || is_string_type(&rv) {
        let ls = lv.to_string_value();
        let rs = rv.to_string_value();
        return Ok(Value::VarChar(format!("{}{}", ls, rs)));
    }
    match (&lv, &rv) {
        (Value::Decimal(_, _), _) | (_, Value::Decimal(_, _)) => {
            let (ar, as_) = to_decimal_parts(&lv);
            let (br, bs) = to_decimal_parts(&rv);
            let max_scale = as_.max(bs);
            let a = rescale_raw(ar, as_, max_scale);
            let b = rescale_raw(br, bs, max_scale);
            Ok(Value::Decimal(a + b, max_scale))
        }
        _ => {
            let a = to_i64(&lv)?;
            let b = to_i64(&rv)?;
            Ok(Value::BigInt(a + b))
        }
    }
}

fn eval_subtract(lv: Value, rv: Value) -> Result<Value, DbError> {
    if lv.is_null() || rv.is_null() {
        return Ok(Value::Null);
    }
    match (&lv, &rv) {
        (Value::Decimal(_, _), _) | (_, Value::Decimal(_, _)) => {
            let (ar, as_) = to_decimal_parts(&lv);
            let (br, bs) = to_decimal_parts(&rv);
            let max_scale = as_.max(bs);
            let a = rescale_raw(ar, as_, max_scale);
            let b = rescale_raw(br, bs, max_scale);
            Ok(Value::Decimal(a - b, max_scale))
        }
        _ => {
            let a = to_i64(&lv)?;
            let b = to_i64(&rv)?;
            Ok(Value::BigInt(a - b))
        }
    }
}

fn eval_multiply(lv: Value, rv: Value) -> Result<Value, DbError> {
    if lv.is_null() || rv.is_null() {
        return Ok(Value::Null);
    }
    match (&lv, &rv) {
        (Value::Decimal(_, _), _) | (_, Value::Decimal(_, _)) => {
            let (ar, as_) = to_decimal_parts(&lv);
            let (br, bs) = to_decimal_parts(&rv);
            let result_scale = as_ + bs;
            Ok(Value::Decimal(ar * br, result_scale))
        }
        _ => {
            let a = to_i64(&lv)?;
            let b = to_i64(&rv)?;
            Ok(Value::BigInt(a * b))
        }
    }
}

fn eval_divide(lv: Value, rv: Value) -> Result<Value, DbError> {
    if lv.is_null() || rv.is_null() {
        return Ok(Value::Null);
    }
    match (&lv, &rv) {
        (Value::Decimal(_, _), _) | (_, Value::Decimal(_, _)) => {
            let (ar, as_) = to_decimal_parts(&lv);
            let (br, bs) = to_decimal_parts(&rv);
            if br == 0 {
                return Ok(Value::Null);
            }
            let scale = 6u8.max(as_);
            let numerator = rescale_raw(ar, as_, scale + bs);
            Ok(Value::Decimal(numerator / br, scale))
        }
        _ => {
            let a = to_i64(&lv)?;
            let b = to_i64(&rv)?;
            if b == 0 {
                return Ok(Value::Null);
            }
            Ok(Value::BigInt(a / b))
        }
    }
}

fn eval_modulo(lv: Value, rv: Value) -> Result<Value, DbError> {
    if lv.is_null() || rv.is_null() {
        return Ok(Value::Null);
    }
    let a = to_i64(&lv)?;
    let b = to_i64(&rv)?;
    if b == 0 {
        return Ok(Value::Null);
    }
    Ok(Value::BigInt(a % b))
}

fn eval_and(lv: Value, rv: Value) -> Result<Value, DbError> {
    match (&lv, &rv) {
        (Value::Null, Value::Null) => Ok(Value::Null),
        (Value::Null, Value::Bit(false)) => Ok(Value::Bit(false)),
        (Value::Bit(false), Value::Null) => Ok(Value::Bit(false)),
        (Value::Null, _) => Ok(Value::Null),
        (_, Value::Null) => Ok(Value::Null),
        _ => Ok(Value::Bit(truthy(&lv) && truthy(&rv))),
    }
}

fn eval_or(lv: Value, rv: Value) -> Result<Value, DbError> {
    match (&lv, &rv) {
        (Value::Null, Value::Null) => Ok(Value::Null),
        (Value::Null, Value::Bit(true)) => Ok(Value::Bit(true)),
        (Value::Bit(true), Value::Null) => Ok(Value::Bit(true)),
        (Value::Null, _) => Ok(Value::Null),
        (_, Value::Null) => Ok(Value::Null),
        _ => Ok(Value::Bit(truthy(&lv) || truthy(&rv))),
    }
}

pub(crate) fn compare_bool<F>(lv: Value, rv: Value, pred: F, ansi_nulls: bool) -> Value
where
    F: FnOnce(Ordering) -> bool,
{
    if lv.is_null() || rv.is_null() {
        if ansi_nulls {
            return Value::Null;
        } else {
            if lv.is_null() && rv.is_null() {
                return Value::Bit(pred(Ordering::Equal));
            } else {
                return Value::Bit(false);
            }
        }
    }
    Value::Bit(pred(compare_values(&lv, &rv)))
}

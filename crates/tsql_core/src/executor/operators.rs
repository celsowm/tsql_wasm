use std::cmp::Ordering;

use crate::ast::{BinaryOp, UnaryOp};
use crate::error::DbError;
use crate::types::Value;

use super::value_helpers::{is_string_type, rescale_raw, to_decimal_parts, to_i64, value_to_f64};
use super::value_ops::{compare_values, truthy};

pub(crate) fn eval_binary(
    op: &BinaryOp,
    lv: Value,
    rv: Value,
    ansi_nulls: bool,
    concat_null_yields_null: bool,
    arithabort: bool,
    ansi_warnings: bool,
) -> Result<Value, DbError> {
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
        BinaryOp::Add => eval_add(lv, rv, concat_null_yields_null, arithabort, ansi_warnings),
        BinaryOp::Subtract => eval_subtract(lv, rv, arithabort, ansi_warnings),
        BinaryOp::Multiply => eval_multiply(lv, rv, arithabort, ansi_warnings),
        BinaryOp::Divide => eval_divide(lv, rv, arithabort, ansi_warnings),
        BinaryOp::Modulo => eval_modulo(lv, rv, arithabort, ansi_warnings),
        BinaryOp::BitwiseAnd => eval_bitwise_and(lv, rv),
        BinaryOp::BitwiseOr => eval_bitwise_or(lv, rv),
        BinaryOp::BitwiseXor => eval_bitwise_xor(lv, rv),
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
            Value::Float(v) => Ok(Value::Float((-f64::from_bits(v)).to_bits())),
            Value::Decimal(raw, scale) => Ok(Value::Decimal(-raw, scale)),
            Value::Money(v) => Ok(Value::Money(-v)),
            Value::SmallMoney(v) => Ok(Value::SmallMoney(-v)),
            _ => Err(DbError::Execution(format!(
                "cannot negate value of type {:?}",
                val.data_type()
            ))),
        },
        UnaryOp::Not => Ok(Value::Bit(!truthy(&val))),
        UnaryOp::BitwiseNot => eval_bitwise_not(val),
    }
}

fn eval_add(
    lv: Value,
    rv: Value,
    concat_null_yields_null: bool,
    arithabort: bool,
    ansi_warnings: bool,
) -> Result<Value, DbError> {
    if is_string_type(&lv) || is_string_type(&rv) {
        if (lv.is_null() || rv.is_null()) && concat_null_yields_null {
            return Ok(Value::Null);
        }
        let ls = if lv.is_null() { String::new() } else { lv.to_string_value() };
        let rs = if rv.is_null() { String::new() } else { rv.to_string_value() };
        return Ok(Value::VarChar(format!("{}{}", ls, rs)));
    }
    if lv.is_null() || rv.is_null() {
        return Ok(Value::Null);
    }
    if is_float_type(&lv) || is_float_type(&rv) {
        let a = value_to_f64(&lv)?;
        let b = value_to_f64(&rv)?;
        return Ok(Value::Float((a + b).to_bits()));
    }
    if is_money_type(&lv) || is_money_type(&rv) {
        let a = extract_money_as_i128(&lv);
        let b = extract_money_as_i128(&rv);
        return Ok(Value::Money(a + b));
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
            match a.checked_add(b) {
                Some(v) => Ok(Value::BigInt(v)),
                None => arithmetic_overflow("addition", arithabort, ansi_warnings),
            }
        }
    }
}

fn eval_subtract(
    lv: Value,
    rv: Value,
    arithabort: bool,
    ansi_warnings: bool,
) -> Result<Value, DbError> {
    if lv.is_null() || rv.is_null() {
        return Ok(Value::Null);
    }
    if is_float_type(&lv) || is_float_type(&rv) {
        let a = value_to_f64(&lv)?;
        let b = value_to_f64(&rv)?;
        return Ok(Value::Float((a - b).to_bits()));
    }
    if is_money_type(&lv) || is_money_type(&rv) {
        let a = extract_money_as_i128(&lv);
        let b = extract_money_as_i128(&rv);
        return Ok(Value::Money(a - b));
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
            match a.checked_sub(b) {
                Some(v) => Ok(Value::BigInt(v)),
                None => arithmetic_overflow("subtraction", arithabort, ansi_warnings),
            }
        }
    }
}

fn eval_multiply(
    lv: Value,
    rv: Value,
    arithabort: bool,
    ansi_warnings: bool,
) -> Result<Value, DbError> {
    if lv.is_null() || rv.is_null() {
        return Ok(Value::Null);
    }
    if is_float_type(&lv) || is_float_type(&rv) {
        let a = value_to_f64(&lv)?;
        let b = value_to_f64(&rv)?;
        return Ok(Value::Float((a * b).to_bits()));
    }
    if is_money_type(&lv) || is_money_type(&rv) {
        let a = extract_money_as_i128(&lv);
        let b = extract_money_as_i128(&rv);
        return Ok(Value::Money((a * b) / 10000));
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
            match a.checked_mul(b) {
                Some(v) => Ok(Value::BigInt(v)),
                None => arithmetic_overflow("multiplication", arithabort, ansi_warnings),
            }
        }
    }
}

fn eval_divide(
    lv: Value,
    rv: Value,
    arithabort: bool,
    ansi_warnings: bool,
) -> Result<Value, DbError> {
    if lv.is_null() || rv.is_null() {
        return Ok(Value::Null);
    }
    if is_float_type(&lv) || is_float_type(&rv) {
        let a = value_to_f64(&lv)?;
        let b = value_to_f64(&rv)?;
        if b == 0.0 {
            return divide_by_zero(arithabort, ansi_warnings);
        }
        return Ok(Value::Float((a / b).to_bits()));
    }
    if is_money_type(&lv) || is_money_type(&rv) {
        let a = extract_money_as_i128(&lv);
        let b = extract_money_as_i128(&rv);
        if b == 0 {
            return divide_by_zero(arithabort, ansi_warnings);
        }
        return Ok(Value::Money(a * 10000 / b));
    }
    match (&lv, &rv) {
        (Value::Decimal(_, _), _) | (_, Value::Decimal(_, _)) => {
            let (ar, as_) = to_decimal_parts(&lv);
            let (br, bs) = to_decimal_parts(&rv);
            if br == 0 {
                return divide_by_zero(arithabort, ansi_warnings);
            }
            let scale = 6u8.max(as_);
            let numerator = rescale_raw(ar, as_, scale + bs);
            Ok(Value::Decimal(numerator / br, scale))
        }
        _ => {
            let a = to_i64(&lv)?;
            let b = to_i64(&rv)?;
            if b == 0 {
                return divide_by_zero(arithabort, ansi_warnings);
            }
            Ok(Value::BigInt(a / b))
        }
    }
}

fn eval_modulo(
    lv: Value,
    rv: Value,
    arithabort: bool,
    ansi_warnings: bool,
) -> Result<Value, DbError> {
    if lv.is_null() || rv.is_null() {
        return Ok(Value::Null);
    }
    let a = to_i64(&lv)?;
    let b = to_i64(&rv)?;
    if b == 0 {
        return divide_by_zero(arithabort, ansi_warnings);
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

fn is_float_type(v: &Value) -> bool {
    matches!(v, Value::Float(_))
}

fn is_money_type(v: &Value) -> bool {
    matches!(v, Value::Money(_) | Value::SmallMoney(_))
}

fn extract_money_as_i128(v: &Value) -> i128 {
    match v {
        Value::Money(r) => *r,
        Value::SmallMoney(r) => *r as i128,
        Value::Int(v) => *v as i128 * 10000,
        Value::BigInt(v) => *v as i128 * 10000,
        Value::TinyInt(v) => *v as i128 * 10000,
        Value::SmallInt(v) => *v as i128 * 10000,
        Value::Decimal(raw, scale) => {
            super::value_helpers::rescale_raw(*raw, *scale, 4)
        }
        Value::Float(v) => (f64::from_bits(*v) * 10000.0) as i128,
        _ => 0,
    }
}

fn eval_bitwise_and(lv: Value, rv: Value) -> Result<Value, DbError> {
    if lv.is_null() || rv.is_null() {
        return Ok(Value::Null);
    }
    let a = to_i64(&lv)?;
    let b = to_i64(&rv)?;
    Ok(Value::BigInt(a & b))
}

fn eval_bitwise_or(lv: Value, rv: Value) -> Result<Value, DbError> {
    if lv.is_null() || rv.is_null() {
        return Ok(Value::Null);
    }
    let a = to_i64(&lv)?;
    let b = to_i64(&rv)?;
    Ok(Value::BigInt(a | b))
}

fn eval_bitwise_xor(lv: Value, rv: Value) -> Result<Value, DbError> {
    if lv.is_null() || rv.is_null() {
        return Ok(Value::Null);
    }
    let a = to_i64(&lv)?;
    let b = to_i64(&rv)?;
    Ok(Value::BigInt(a ^ b))
}

fn arithmetic_overflow(
    what: &str,
    arithabort: bool,
    ansi_warnings: bool,
) -> Result<Value, DbError> {
    if arithabort || ansi_warnings {
        Err(DbError::Execution(format!(
            "Arithmetic overflow error during {}",
            what
        )))
    } else {
        Ok(Value::Null)
    }
}

fn divide_by_zero(arithabort: bool, ansi_warnings: bool) -> Result<Value, DbError> {
    if arithabort || ansi_warnings {
        Err(DbError::Execution("Divide by zero error encountered.".into()))
    } else {
        Ok(Value::Null)
    }
}

fn eval_bitwise_not(val: Value) -> Result<Value, DbError> {
    if val.is_null() {
        return Ok(Value::Null);
    }
    let a = to_i64(&val)?;
    Ok(Value::BigInt(!a))
}

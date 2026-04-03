use crate::ast::Expr;
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::types::Value;
use crate::storage::Storage;

use super::super::clock::Clock;
use super::super::context::ExecutionContext;
use super::super::evaluator::eval_expr;
use super::super::model::ContextTable;
use super::super::value_helpers::value_to_f64;

pub(crate) fn eval_math_unary<F>(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
    name: &str,
    func: F,
) -> Result<Value, DbError>
where
    F: Fn(f64) -> f64,
{
    if args.len() != 1 {
        return Err(DbError::Execution(format!("{} expects 1 argument", name)));
    }
    let val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    if val.is_null() {
        return Ok(Value::Null);
    }
    
    match &val {
        Value::Decimal(_, scale) => {
            let f = value_to_f64(&val)?;
            let result = func(f);
            let result_raw = (result * 10f64.powi(*scale as i32)).round() as i128;
            Ok(Value::Decimal(result_raw, *scale))
        }
        Value::Float(bits) => {
            let f = f64::from_bits(*bits);
            let result = func(f);
            Ok(Value::Float(result.to_bits()))
        }
        Value::Int(v) => {
            let f = *v as f64;
            let result = func(f);
            let raw = (result * 1000000.0).round() as i128;
            Ok(Value::Decimal(raw, 6))
        }
        Value::BigInt(v) => {
            let f = *v as f64;
            let result = func(f);
            let raw = (result * 1000000.0).round() as i128;
            Ok(Value::Decimal(raw, 6))
        }
        Value::TinyInt(v) => {
            let f = *v as f64;
            let result = func(f);
            let raw = (result * 1000000.0).round() as i128;
            Ok(Value::Decimal(raw, 6))
        }
        Value::SmallInt(v) => {
            let f = *v as f64;
            let result = func(f);
            let raw = (result * 1000000.0).round() as i128;
            Ok(Value::Decimal(raw, 6))
        }
        _ => {
            let f = value_to_f64(&val)?;
            let result = func(f);
            Ok(Value::Float(result.to_bits()))
        }
    }
}

pub(crate) fn eval_round(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.is_empty() || args.len() > 2 {
        return Err(DbError::Execution("ROUND expects 1 or 2 arguments".into()));
    }
    let val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let precision = if args.len() == 2 {
        let p = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;
        match p {
            Value::Int(v) => v,
            Value::TinyInt(v) => v as i32,
            Value::SmallInt(v) => v as i32,
            _ => {
                return Err(DbError::Execution(
                    "ROUND precision must be an integer".into(),
                ))
            }
        }
    } else {
        0
    };

    if val.is_null() {
        return Ok(Value::Null);
    }

    let f = match &val {
        Value::Decimal(raw, scale) => {
            let divisor = 10f64.powi(*scale as i32);
            *raw as f64 / divisor
        }
        Value::Float(bits) => f64::from_bits(*bits),
        Value::TinyInt(v) => *v as f64,
        Value::SmallInt(v) => *v as f64,
        Value::Int(v) => *v as f64,
        Value::BigInt(v) => *v as f64,
        _ => {
            return Err(DbError::Execution(
                "ROUND requires a numeric argument".into(),
            ))
        }
    };

    let multiplier = 10f64.powi(precision);
    let rounded = (f * multiplier).round() / multiplier;

    match &val {
        Value::Decimal(_raw, scale) => {
            let result_raw = (rounded * 10f64.powi(*scale as i32)).round() as i128;
            Ok(Value::Decimal(result_raw, *scale))
        }
        Value::Float(_) => Ok(Value::Float(rounded.to_bits())),
        Value::Int(_) => Ok(Value::Int(rounded as i32)),
        Value::BigInt(_) => Ok(Value::BigInt(rounded as i64)),
        Value::TinyInt(_) => Ok(Value::TinyInt(rounded as u8)),
        Value::SmallInt(_) => Ok(Value::SmallInt(rounded as i16)),
        Value::Money(_) => Ok(Value::Money((rounded * 10000.0).round() as i128)),
        Value::SmallMoney(_) => Ok(Value::SmallMoney((rounded * 10000.0).round() as i64)),
        _ => Ok(Value::Float(rounded.to_bits())),
    }
}

pub(crate) fn eval_abs(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 1 {
        return Err(DbError::Execution("ABS expects 1 argument".into()));
    }
    let val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    if val.is_null() {
        return Ok(Value::Null);
    }
    match &val {
        Value::TinyInt(v) => Ok(Value::TinyInt(*v)),
        Value::SmallInt(v) => Ok(Value::SmallInt(v.abs())),
        Value::Int(v) => Ok(Value::Int(v.abs())),
        Value::BigInt(v) => Ok(Value::BigInt(v.abs())),
        Value::Decimal(raw, scale) => Ok(Value::Decimal(raw.abs(), *scale)),
        Value::Float(bits) => Ok(Value::Float((f64::from_bits(*bits).abs()).to_bits())),
        Value::Money(v) => Ok(Value::Money(v.abs())),
        Value::SmallMoney(v) => Ok(Value::SmallMoney(v.abs())),
        _ => {
            let f = value_to_f64(&val)?;
            Ok(Value::Float(f.abs().to_bits()))
        }
    }
}

pub(crate) fn eval_power(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 2 {
        return Err(DbError::Execution("POWER expects 2 arguments".into()));
    }
    let base = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let exponent = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;

    if base.is_null() || exponent.is_null() {
        return Ok(Value::Null);
    }

    let b = value_to_f64(&base)?;
    let e = value_to_f64(&exponent)?;
    let result = b.powf(e);
    Ok(Value::Float(result.to_bits()))
}

pub(crate) fn eval_sqrt(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 1 {
        return Err(DbError::Execution("SQRT expects 1 argument".into()));
    }
    let val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;

    if val.is_null() {
        return Ok(Value::Null);
    }

    let f = value_to_f64(&val)?;
    let result = f.sqrt();
    Ok(Value::Float(result.to_bits()))
}

pub(crate) fn eval_sign(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 1 {
        return Err(DbError::Execution("SIGN expects 1 argument".into()));
    }
    let val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;

    if val.is_null() {
        return Ok(Value::Null);
    }

    let f = value_to_f64(&val)?;
    let result: f64 = if f > 0.0 {
        1.0
    } else if f < 0.0 {
        -1.0
    } else {
        0.0
    };
    Ok(Value::Float(result.to_bits()))
}

pub(crate) fn eval_checksum(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.is_empty() {
        return Err(DbError::Execution("CHECKSUM requires at least one argument".into()));
    }
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    for arg in args {
        let val = eval_expr(arg, row, ctx, catalog, storage, clock)?;
        match val {
            Value::Null => { 0i64.hash(&mut hasher); }
            Value::Bit(v) => { v.hash(&mut hasher); }
            Value::TinyInt(v) => { v.hash(&mut hasher); }
            Value::SmallInt(v) => { v.hash(&mut hasher); }
            Value::Int(v) => { v.hash(&mut hasher); }
            Value::BigInt(v) => { v.hash(&mut hasher); }
            Value::Float(v) => { v.hash(&mut hasher); }
            _ => { val.to_string_value().hash(&mut hasher); }
        }
    }
    let hash = hasher.finish();
    // CHECKSUM returns INT in SQL Server
    Ok(Value::Int((hash as i64) as i32))
}

pub(crate) fn eval_atn2(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 2 {
        return Err(DbError::Execution("ATN2 expects 2 arguments".into()));
    }
    let y = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let x = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;
    if y.is_null() || x.is_null() {
        return Ok(Value::Null);
    }
    let yf = value_to_f64(&y)?;
    let xf = value_to_f64(&x)?;
    Ok(Value::Float(yf.atan2(xf).to_bits()))
}

pub(crate) fn eval_log(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.is_empty() || args.len() > 2 {
        return Err(DbError::Execution("LOG expects 1 or 2 arguments".into()));
    }
    let val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    if val.is_null() {
        return Ok(Value::Null);
    }
    let f = value_to_f64(&val)?;
    let result = if args.len() == 2 {
        let base_val = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;
        if base_val.is_null() {
            return Ok(Value::Null);
        }
        let base = value_to_f64(&base_val)?;
        f.log(base)
    } else {
        f.ln()
    };
    Ok(Value::Float(result.to_bits()))
}

pub(crate) fn eval_pi(
    args: &[Expr],
) -> Result<Value, DbError> {
    if !args.is_empty() {
        return Err(DbError::Execution("PI expects no arguments".into()));
    }
    Ok(Value::Float(std::f64::consts::PI.to_bits()))
}

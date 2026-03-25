use crate::ast::Expr;
use crate::error::DbError;
use crate::types::Value;

use super::clock::Clock;
use super::context::ExecutionContext;
use super::evaluator::eval_expr;
use super::value_ops::compare_values;
use crate::catalog::Catalog;
use crate::storage::Storage;
pub use super::model::Group;

pub fn is_aggregate_function(name: &str) -> bool {
    matches!(
        name.to_uppercase().as_str(),
        "COUNT" | "SUM" | "AVG" | "MIN" | "MAX"
    )
}

/// Centralized aggregate dispatch. Returns None if the function is not a recognized aggregate.
/// This enables OCP: add new aggregates here without modifying caller code.
pub fn dispatch_aggregate(
    name: &str,
    args: &[Expr],
    group: &Group,
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Option<Result<Value, DbError>> {
    match name.to_uppercase().as_str() {
        "COUNT" => Some(Ok(eval_aggregate_count(args, group, ctx, catalog, storage, clock))),
        "SUM" => Some(eval_aggregate_sum(args, group, ctx, catalog, storage, clock)),
        "AVG" => Some(eval_aggregate_avg(args, group, ctx, catalog, storage, clock)),
        "MIN" => Some(eval_aggregate_min(args, group, ctx, catalog, storage, clock)),
        "MAX" => Some(eval_aggregate_max(args, group, ctx, catalog, storage, clock)),
        _ => None,
    }
}

pub fn collect_group_values<'a>(
    expr: &'a Expr,
    group: &'a Group,
    ctx: &'a mut ExecutionContext,
    catalog: &'a dyn Catalog,
    storage: &'a dyn Storage,
    clock: &'a dyn Clock,
) -> Vec<Value> {
    group
        .rows
        .iter()
        .filter_map(move |row| eval_expr(expr, row, ctx, catalog, storage, clock).ok())
        .filter(|v| !v.is_null())
        .collect()
}

pub fn eval_aggregate_count(
    args: &[Expr],
    group: &Group,
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Value {
    let count = if args.first().is_some_and(|a| matches!(a, Expr::Wildcard)) {
        group.rows.len() as i64
    } else if let Some(expr) = args.first() {
        collect_group_values(expr, group, ctx, catalog, storage, clock).len() as i64
    } else {
        group.rows.len() as i64
    };
    Value::BigInt(count)
}

pub fn eval_aggregate_sum(
    args: &[Expr],
    group: &Group,
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    let expr = args
        .first()
        .ok_or_else(|| DbError::Execution("SUM requires 1 argument".into()))?;
    let mut sum_i64: i64 = 0;
    let mut sum_f64: f64 = 0.0;
    let mut sum_i128: i128 = 0;
    let mut current_scale: u8 = 0;
    let mut has_values = false;
    let mut is_decimal = false;
    let mut is_float = false;
    let mut is_money = false;

    for val in collect_group_values(expr, group, ctx, catalog, storage, clock) {
        if !has_values {
            has_values = true;
            match &val {
                Value::Decimal(_, s) => { is_decimal = true; current_scale = *s; }
                Value::Float(_) => { is_float = true; }
                Value::Money(_) | Value::SmallMoney(_) => { is_money = true; }
                _ => {}
            }
        }

        match &val {
            Value::Float(v) => {
                is_float = true;
                sum_f64 += f64::from_bits(*v);
            }
            Value::Decimal(raw, scale) => {
                is_decimal = true;
                let max_scale = current_scale.max(*scale);
                sum_i128 = super::value_helpers::rescale_raw(sum_i128, current_scale, max_scale)
                         + super::value_helpers::rescale_raw(*raw, *scale, max_scale);
                current_scale = max_scale;
            }
            Value::Money(v) => {
                is_money = true;
                sum_i128 += *v;
            }
            Value::SmallMoney(v) => {
                is_money = true;
                sum_i128 += *v as i128;
            }
            Value::TinyInt(v) => sum_i64 += *v as i64,
            Value::SmallInt(v) => sum_i64 += *v as i64,
            Value::Int(v) => sum_i64 += *v as i64,
            Value::BigInt(v) => sum_i64 += *v,
            _ => return Err(DbError::Execution("SUM requires numeric argument".into())),
        }
    }

    if !has_values {
        return Ok(Value::Null);
    }

    if is_float {
        Ok(Value::Float(sum_f64.to_bits()))
    } else if is_money {
        Ok(Value::Money(sum_i128))
    } else if is_decimal {
        Ok(Value::Decimal(sum_i128, current_scale))
    } else {
        Ok(Value::BigInt(sum_i64))
    }
}

pub fn eval_aggregate_avg(
    args: &[Expr],
    group: &Group,
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    let values = collect_group_values(args.first().unwrap(), group, ctx, catalog, storage, clock);
    if values.is_empty() {
        return Ok(Value::Null);
    }
    let first_val = &values[0];
    let is_float = matches!(first_val, Value::Float(_));
    let is_money = matches!(first_val, Value::Money(_) | Value::SmallMoney(_));

    if is_float {
        let mut sum_f64 = 0.0;
        for val in &values {
            if let Value::Float(v) = val {
                sum_f64 += f64::from_bits(*v);
            }
        }
        return Ok(Value::Float((sum_f64 / values.len() as f64).to_bits()));
    }

    if is_money {
        let mut sum_i128: i128 = 0;
        for val in &values {
            match val {
                Value::Money(v) => sum_i128 += *v,
                Value::SmallMoney(v) => sum_i128 += *v as i128,
                _ => {}
            }
        }
        return Ok(Value::Money(sum_i128 / values.len() as i128));
    }

    let sum = eval_aggregate_sum(args, group, ctx, catalog, storage, clock)?;
    let n = values.len() as i128;
    match sum {
        Value::BigInt(v) => {
            // result = (v * 10^6) / n
            let raw = (v as i128 * 1_000_000) / n;
            Ok(Value::Decimal(raw, 6))
        }
        Value::Decimal(v, s) => {
            // We want result with scale 6.
            // Result raw = (v * 10^6) / (n * 10^s) = (v * 10^(6-s)) / n
            let raw = if s <= 6 {
                (v * 10i128.pow((6 - s) as u32)) / n
            } else {
                v / (n * 10i128.pow((s - 6) as u32))
            };
            Ok(Value::Decimal(raw, 6))
        }
        Value::Float(v) => {
            Ok(Value::Float((f64::from_bits(v) / n as f64).to_bits()))
        }
        _ => Ok(Value::Null),
    }
}

pub fn eval_aggregate_min(
    args: &[Expr],
    group: &Group,
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    let values = collect_group_values(args.first().unwrap(), group, ctx, catalog, storage, clock);
    Ok(values
        .into_iter()
        .min_by(compare_values)
        .unwrap_or(Value::Null))
}

pub fn eval_aggregate_max(
    args: &[Expr],
    group: &Group,
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    let values = collect_group_values(args.first().unwrap(), group, ctx, catalog, storage, clock);
    Ok(values
        .into_iter()
        .max_by(compare_values)
        .unwrap_or(Value::Null))
}

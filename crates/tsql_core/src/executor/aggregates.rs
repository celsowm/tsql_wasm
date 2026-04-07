use crate::ast::Expr;
use crate::error::DbError;
use crate::types::Value;

use super::clock::Clock;
use super::context::ExecutionContext;
use super::evaluator::eval_expr;
pub use super::model::Group;
use super::string_norm::normalize_identifier;
use super::value_ops::compare_values;
use crate::catalog::Catalog;
use crate::storage::Storage;

/// Typed enum for aggregate functions, replacing string-based dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFn {
    Count,
    CountDistinct,
    Sum,
    Avg,
    Min,
    Max,
    StringAgg,
}

impl AggregateFn {
    /// Parse an aggregate function name (case-insensitive) into a typed enum.
    pub fn from_name(name: &str) -> Option<Self> {
        match normalize_identifier(name).as_str() {
            "COUNT" => Some(AggregateFn::Count),
            "COUNT_DISTINCT" => Some(AggregateFn::CountDistinct),
            "SUM" => Some(AggregateFn::Sum),
            "AVG" => Some(AggregateFn::Avg),
            "MIN" => Some(AggregateFn::Min),
            "MAX" => Some(AggregateFn::Max),
            "STRING_AGG" => Some(AggregateFn::StringAgg),
            _ => None,
        }
    }
}

pub fn is_aggregate_function(name: &str) -> bool {
    AggregateFn::from_name(name).is_some()
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
    let agg = AggregateFn::from_name(name)?;
    match agg {
        AggregateFn::Count => Some(Ok(eval_aggregate_count(
            args, group, ctx, catalog, storage, clock,
        ))),
        AggregateFn::CountDistinct => Some(Ok(eval_aggregate_count_distinct(
            args, group, ctx, catalog, storage, clock,
        ))),
        AggregateFn::Sum => Some(eval_aggregate_sum(
            args, group, ctx, catalog, storage, clock,
        )),
        AggregateFn::Avg => Some(eval_aggregate_avg(
            args, group, ctx, catalog, storage, clock,
        )),
        AggregateFn::Min => Some(eval_aggregate_min(
            args, group, ctx, catalog, storage, clock,
        )),
        AggregateFn::Max => Some(eval_aggregate_max(
            args, group, ctx, catalog, storage, clock,
        )),
        AggregateFn::StringAgg => Some(eval_aggregate_string_agg(
            args, group, ctx, catalog, storage, clock,
        )),
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

pub fn eval_aggregate_count_distinct(
    args: &[Expr],
    group: &Group,
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Value {
    if let Some(expr) = args.first() {
        let values = collect_group_values(expr, group, ctx, catalog, storage, clock);
        let mut seen = std::collections::HashSet::new();
        let count = values
            .into_iter()
            .filter(|v| seen.insert(format!("{:?}", v)))
            .count() as i64;
        Value::BigInt(count)
    } else {
        Value::BigInt(group.rows.len() as i64)
    }
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
                Value::Decimal(_, s) => {
                    is_decimal = true;
                    current_scale = *s;
                }
                Value::Float(_) => {
                    is_float = true;
                }
                Value::Money(_) | Value::SmallMoney(_) => {
                    is_money = true;
                }
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
    let expr = args
        .first()
        .ok_or_else(|| DbError::Execution("AVG requires 1 argument".into()))?;
    let values = collect_group_values(expr, group, ctx, catalog, storage, clock);
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
            let res = v / n as i64;
            match first_val {
                Value::TinyInt(_) => Ok(Value::TinyInt(res as u8)),
                Value::SmallInt(_) => Ok(Value::SmallInt(res as i16)),
                Value::Int(_) => Ok(Value::Int(res as i32)),
                _ => Ok(Value::BigInt(res)),
            }
        }
        Value::Decimal(v, s) => Ok(Value::Decimal(v / n, s)),
        Value::Float(v) => Ok(Value::Float((f64::from_bits(v) / n as f64).to_bits())),
        Value::Money(v) => Ok(Value::Money(v / n)),
        Value::SmallMoney(v) => Ok(Value::SmallMoney((v as i128 / n) as i64)),
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
    let expr = args
        .first()
        .ok_or_else(|| DbError::Execution("MIN requires 1 argument".into()))?;
    let values = collect_group_values(expr, group, ctx, catalog, storage, clock);
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
    let expr = args
        .first()
        .ok_or_else(|| DbError::Execution("MAX requires 1 argument".into()))?;
    let values = collect_group_values(expr, group, ctx, catalog, storage, clock);
    Ok(values
        .into_iter()
        .max_by(compare_values)
        .unwrap_or(Value::Null))
}

pub fn eval_aggregate_string_agg(
    args: &[Expr],
    group: &Group,
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() < 2 {
        return Err(DbError::Execution(
            "STRING_AGG requires at least 2 arguments: expression and separator".into(),
        ));
    }

    let expr = &args[0];
    let separator_expr = &args[1];

    let separator = eval_expr(separator_expr, &[], ctx, catalog, storage, clock)?;
    let separator_str = match separator {
        Value::VarChar(s) => s,
        Value::NVarChar(s) => s,
        Value::Char(s) => s,
        Value::NChar(s) => s,
        _ => {
            return Err(DbError::Execution(
                "STRING_AGG separator must be a string".into(),
            ))
        }
    };

    let values = collect_group_values(expr, group, ctx, catalog, storage, clock);
    if values.is_empty() {
        return Ok(Value::Null);
    }

    let string_values: Vec<String> = values.iter().map(|v| v.to_string_value()).collect();

    let result = string_values.join(&separator_str);
    Ok(Value::VarChar(result))
}

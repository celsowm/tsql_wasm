use std::cmp::Ordering;

use crate::ast::{BinaryOp, Expr};
use crate::error::DbError;
use crate::types::{DataType, Value};

use super::clock::Clock;
use super::model::JoinedRow;
use super::type_mapping::data_type_spec_to_runtime;
use super::value_ops::{coerce_value_to_type, compare_values, truthy};

pub(crate) fn eval_expr_to_type_constant(
    expr: &Expr,
    ty: &DataType,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    let value = eval_constant_expr(expr, clock)?;
    coerce_value_to_type(value, ty)
}

pub(crate) fn eval_expr_to_type_in_context(
    expr: &Expr,
    ty: &DataType,
    row: &JoinedRow,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    let value = eval_expr(expr, row, clock)?;
    coerce_value_to_type(value, ty)
}

pub(crate) fn eval_constant_expr(expr: &Expr, clock: &dyn Clock) -> Result<Value, DbError> {
    let ctx: JoinedRow = vec![];
    eval_expr(expr, &ctx, clock)
}

pub(crate) fn eval_expr(expr: &Expr, row: &JoinedRow, clock: &dyn Clock) -> Result<Value, DbError> {
    match expr {
        Expr::Identifier(name) => resolve_identifier(row, name),
        Expr::QualifiedIdentifier(parts) => resolve_qualified_identifier(row, parts),
        Expr::Wildcard => Err(DbError::Execution("wildcard is not a scalar expression".into())),
        Expr::Integer(v) => Ok(if *v >= i32::MIN as i64 && *v <= i32::MAX as i64 {
            Value::Int(*v as i32)
        } else {
            Value::BigInt(*v)
        }),
        Expr::String(v) => Ok(Value::VarChar(v.clone())),
        Expr::UnicodeString(v) => Ok(Value::NVarChar(v.clone())),
        Expr::Null => Ok(Value::Null),
        Expr::FunctionCall { name, args } => eval_function(name, args, row, clock),
        Expr::Binary { left, op, right } => {
            let lv = eval_expr(left, row, clock)?;
            let rv = eval_expr(right, row, clock)?;
            eval_binary(op, lv, rv)
        }
        Expr::IsNull(inner) => Ok(Value::Bit(eval_expr(inner, row, clock)?.is_null())),
        Expr::IsNotNull(inner) => Ok(Value::Bit(!eval_expr(inner, row, clock)?.is_null())),
        Expr::Cast { expr, target } => {
            let value = eval_expr(expr, row, clock)?;
            coerce_value_to_type(value, &data_type_spec_to_runtime(target))
        }
        Expr::Convert { target, expr } => {
            let value = eval_expr(expr, row, clock)?;
            coerce_value_to_type(value, &data_type_spec_to_runtime(target))
        }
    }
}

pub(crate) fn eval_predicate(expr: &Expr, row: &JoinedRow, clock: &dyn Clock) -> Result<bool, DbError> {
    let value = eval_expr(expr, row, clock)?;
    Ok(match value {
        Value::Bit(v) => v,
        Value::Null => false,
        other => truthy(&other),
    })
}

pub(crate) fn contains_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::FunctionCall { name, .. } if name.eq_ignore_ascii_case("COUNT") => true,
        Expr::Binary { left, right, .. } => contains_aggregate(left) || contains_aggregate(right),
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => contains_aggregate(inner),
        Expr::Cast { expr, .. } | Expr::Convert { expr, .. } => contains_aggregate(expr),
        _ => false,
    }
}

fn resolve_identifier(row: &JoinedRow, name: &str) -> Result<Value, DbError> {
    let mut found: Option<Value> = None;
    for binding in row {
        if let Some(idx) = binding
            .table
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(name))
        {
            let value = binding
                .row
                .as_ref()
                .map(|r| r.values[idx].clone())
                .unwrap_or(Value::Null);
            if found.is_some() {
                return Err(DbError::Semantic(format!("ambiguous column '{}'", name)));
            }
            found = Some(value);
        }
    }
    found.ok_or_else(|| DbError::Semantic(format!("column '{}' not found", name)))
}

fn resolve_qualified_identifier(row: &JoinedRow, parts: &[String]) -> Result<Value, DbError> {
    if parts.len() != 2 {
        return Err(DbError::Semantic(
            "only two-part identifiers are supported in this build".into(),
        ));
    }

    let table_name = &parts[0];
    let column_name = &parts[1];
    for binding in row {
        if binding.alias.eq_ignore_ascii_case(table_name)
            || binding.table.name.eq_ignore_ascii_case(table_name)
        {
            let idx = binding
                .table
                .columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(column_name))
                .ok_or_else(|| {
                    DbError::Semantic(format!("column '{}.{}' not found", table_name, column_name))
                })?;
            return Ok(binding
                .row
                .as_ref()
                .map(|r| r.values[idx].clone())
                .unwrap_or(Value::Null));
        }
    }

    Err(DbError::Semantic(format!("table or alias '{}' not found", table_name)))
}

fn eval_function(name: &str, args: &[Expr], row: &JoinedRow, clock: &dyn Clock) -> Result<Value, DbError> {
    if name.eq_ignore_ascii_case("GETDATE") {
        if !args.is_empty() {
            return Err(DbError::Execution("GETDATE expects no arguments".into()));
        }
        Ok(Value::DateTime(clock.now_datetime_literal()))
    } else if name.eq_ignore_ascii_case("ISNULL") {
        if args.len() != 2 {
            return Err(DbError::Execution("ISNULL expects 2 arguments".into()));
        }
        let left = eval_expr(&args[0], row, clock)?;
        if !left.is_null() {
            Ok(left)
        } else {
            eval_expr(&args[1], row, clock)
        }
    } else if name.eq_ignore_ascii_case("COUNT") {
        Err(DbError::Execution("COUNT is only supported in grouped projection".into()))
    } else {
        Err(DbError::Execution(format!("function '{}' not supported", name)))
    }
}

fn eval_binary(op: &BinaryOp, lv: Value, rv: Value) -> Result<Value, DbError> {
    match op {
        BinaryOp::Eq => Ok(compare_bool(lv, rv, |o| o == Ordering::Equal)),
        BinaryOp::NotEq => Ok(compare_bool(lv, rv, |o| o != Ordering::Equal)),
        BinaryOp::Gt => Ok(compare_bool(lv, rv, |o| o == Ordering::Greater)),
        BinaryOp::Lt => Ok(compare_bool(lv, rv, |o| o == Ordering::Less)),
        BinaryOp::Gte => Ok(compare_bool(lv, rv, |o| matches!(o, Ordering::Greater | Ordering::Equal))),
        BinaryOp::Lte => Ok(compare_bool(lv, rv, |o| matches!(o, Ordering::Less | Ordering::Equal))),
        BinaryOp::And => Ok(Value::Bit(truthy(&lv) && truthy(&rv))),
        BinaryOp::Or => Ok(Value::Bit(truthy(&lv) || truthy(&rv))),
    }
}

fn compare_bool<F>(lv: Value, rv: Value, pred: F) -> Value
where
    F: FnOnce(Ordering) -> bool,
{
    if lv.is_null() || rv.is_null() {
        return Value::Null;
    }
    Value::Bit(pred(compare_values(&lv, &rv)))
}

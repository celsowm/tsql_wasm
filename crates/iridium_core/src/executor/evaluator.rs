use crate::ast::Expr;
use crate::catalog::Catalog;
use crate::error::{DbError, StmtOutcome};
use crate::storage::Storage;
use crate::types::{DataType, Value};

use super::clock::Clock;
use super::context::ExecutionContext;
use super::identifier::{resolve_identifier, resolve_qualified_identifier};
use super::model::JoinedRow;
use super::operators::{eval_binary, eval_unary};
use super::predicates::{
    eval_between, eval_case, eval_exists, eval_in_list, eval_in_subquery, eval_like,
    eval_scalar_subquery,
};
use super::scalar::eval_function;
use super::script::ScriptExecutor;
use super::type_mapping::data_type_spec_to_runtime;
use super::value_ops::{coerce_value_to_type_with_dateformat, truthy};

pub(crate) fn eval_expr_to_type_constant(
    expr: &Expr,
    ty: &DataType,
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    let value = eval_constant_expr(expr, ctx, catalog, storage, clock)?;
    coerce_value_to_type_with_dateformat(value, ty, &ctx.options.dateformat)
}

pub(crate) fn eval_expr_to_type_in_context(
    expr: &Expr,
    ty: &DataType,
    row: &[super::model::ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    let mut sub_ctx = ctx.with_outer_row(row.to_vec());
    let value = eval_expr(expr, row, &mut sub_ctx, catalog, storage, clock)?;
    coerce_value_to_type_with_dateformat(value, ty, &ctx.options.dateformat)
}

pub(crate) fn eval_constant_expr(
    expr: &Expr,
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    let row: JoinedRow = vec![];
    eval_expr(expr, &row, ctx, catalog, storage, clock)
}

/// Evaluates a UDF body by executing its statements.
pub(crate) fn eval_udf_body<'a>(
    stmts: &[crate::ast::Statement],
    ctx: &mut ExecutionContext<'_>,
    catalog: &'a dyn Catalog,
    storage: &'a dyn Storage,
    clock: &'a dyn Clock,
) -> Result<Value, DbError> {
    let mut catalog_owned = catalog.clone_boxed();
    let mut storage_owned = storage.clone_boxed();

    let mut executor = ScriptExecutor {
        catalog: catalog_owned.as_mut(),
        storage: storage_owned.as_mut(),
        clock,
    };
    match executor.execute_batch(stmts, ctx) {
        Ok(StmtOutcome::Return(Some(val))) => Ok(val),
        Ok(StmtOutcome::Return(None)) => Ok(Value::Null),
        Ok(StmtOutcome::Ok(_)) => Ok(Value::Null),
        Ok(_) => Ok(Value::Null),
        Err(e) => Err(e),
    }
}

const MAX_RECURSION_DEPTH: usize = 32;

#[inline]
pub fn eval_expr(
    expr: &Expr,
    row: &[super::model::ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if ctx.frame.depth > MAX_RECURSION_DEPTH {
        return Err(DbError::Execution(format!(
            "Maximum recursion depth ({}) exceeded",
            MAX_RECURSION_DEPTH
        )));
    }

    ctx.frame.depth += 1;
    let res = eval_expr_inner(expr, row, ctx, catalog, storage, clock);
    ctx.frame.depth -= 1;
    res
}

#[inline(always)]
fn eval_expr_inner(
    expr: &Expr,
    row: &[super::model::ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    match expr {
        Expr::Identifier(name) => resolve_identifier(row, name, ctx),
        Expr::QualifiedIdentifier(parts) => resolve_qualified_identifier(row, parts, ctx),
        Expr::Wildcard => Err(DbError::Execution(
            "wildcard is not a scalar expression".into(),
        )),
        Expr::QualifiedWildcard(_) => Err(DbError::Execution(
            "qualified wildcard is not a scalar expression".into(),
        )),
        Expr::Integer(v) => Ok(if *v >= i32::MIN as i64 && *v <= i32::MAX as i64 {
            Value::Int(*v as i32)
        } else {
            Value::BigInt(*v)
        }),
        Expr::FloatLiteral(s) => super::value_ops::parse_numeric_literal(s),
        Expr::BinaryLiteral(bytes) => Ok(Value::Binary(bytes.clone())),
        Expr::String(v) => Ok(Value::VarChar(v.clone())),
        Expr::UnicodeString(v) => Ok(Value::NVarChar(v.clone())),
        Expr::Null => Ok(Value::Null),
        Expr::FunctionCall { name, args, .. } => {
            eval_function(name, args, row, ctx, catalog, storage, clock)
        }
        Expr::Binary { left, op, right } => {
            let lv = eval_expr(left, row, ctx, catalog, storage, clock)?;
            let rv = eval_expr(right, row, ctx, catalog, storage, clock)?;
            eval_binary(
                op,
                lv,
                rv,
                ctx.metadata.ansi_nulls,
                ctx.options.concat_null_yields_null,
                ctx.options.arithabort,
                ctx.options.ansi_warnings,
            )
        }
        Expr::Unary { op, expr: inner } => {
            let val = eval_expr(inner, row, ctx, catalog, storage, clock)?;
            eval_unary(op, val)
        }
        Expr::IsNull(inner) => Ok(Value::Bit(
            eval_expr(inner, row, ctx, catalog, storage, clock)?.is_null(),
        )),
        Expr::IsNotNull(inner) => Ok(Value::Bit(
            !eval_expr(inner, row, ctx, catalog, storage, clock)?.is_null(),
        )),
        Expr::Cast { expr, target } => {
            let value = eval_expr(expr, row, ctx, catalog, storage, clock)?;
            coerce_value_to_type_with_dateformat(
                value,
                &data_type_spec_to_runtime(target),
                &ctx.options.dateformat,
            )
        }
        Expr::TryCast { expr, target } => {
            let value = eval_expr(expr, row, ctx, catalog, storage, clock)?;
            match coerce_value_to_type_with_dateformat(
                value,
                &data_type_spec_to_runtime(target),
                &ctx.options.dateformat,
            ) {
                Ok(v) => Ok(v),
                Err(_) => Ok(Value::Null),
            }
        }
        Expr::Convert {
            target,
            expr,
            style,
        } => {
            let value = eval_expr(expr, row, ctx, catalog, storage, clock)?;
            if let Some(style_code) = style {
                super::value_ops::convert_with_style(
                    value,
                    &data_type_spec_to_runtime(target),
                    *style_code,
                    &ctx.options.dateformat,
                )
            } else {
                coerce_value_to_type_with_dateformat(
                    value,
                    &data_type_spec_to_runtime(target),
                    &ctx.options.dateformat,
                )
            }
        }
        Expr::TryConvert {
            target,
            expr,
            style,
        } => {
            let value = eval_expr(expr, row, ctx, catalog, storage, clock)?;
            let result = if let Some(style_code) = style {
                super::value_ops::convert_with_style(
                    value,
                    &data_type_spec_to_runtime(target),
                    *style_code,
                    &ctx.options.dateformat,
                )
            } else {
                coerce_value_to_type_with_dateformat(
                    value,
                    &data_type_spec_to_runtime(target),
                    &ctx.options.dateformat,
                )
            };
            match result {
                Ok(v) => Ok(v),
                Err(_) => Ok(Value::Null),
            }
        }
        Expr::Case {
            operand,
            when_clauses,
            else_result,
        } => eval_case(
            operand.as_deref(),
            when_clauses,
            else_result.as_deref(),
            row,
            ctx,
            catalog,
            storage,
            clock,
        ),
        Expr::InList {
            expr: in_expr,
            list,
            negated,
        } => eval_in_list(in_expr, list, *negated, row, ctx, catalog, storage, clock),
        Expr::Between {
            expr: between_expr,
            low,
            high,
            negated,
        } => eval_between(
            between_expr,
            low,
            high,
            *negated,
            row,
            ctx,
            catalog,
            storage,
            clock,
        ),
        Expr::Like {
            expr: like_expr,
            pattern,
            negated,
        } => eval_like(
            like_expr, pattern, *negated, row, ctx, catalog, storage, clock,
        ),
        Expr::Subquery(stmt) => eval_scalar_subquery(stmt, row, ctx, catalog, storage, clock),
        Expr::Exists { subquery, negated } => {
            eval_exists(subquery, *negated, row, ctx, catalog, storage, clock)
        }
        Expr::InSubquery {
            expr: in_expr,
            subquery,
            negated,
        } => eval_in_subquery(
            in_expr, subquery, *negated, row, ctx, catalog, storage, clock,
        ),
        Expr::WindowFunction { .. } => {
            let key = format!("{:?}", expr);
            if let Some(val) = ctx.get_window_value(&key) {
                Ok(val)
            } else {
                Err(DbError::Execution(
                    "window function value not found in context".into(),
                ))
            }
        }
    }
}

pub(crate) fn eval_predicate(
    expr: &Expr,
    row: &[super::model::ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<bool, DbError> {
    let value = eval_expr(expr, row, ctx, catalog, storage, clock)?;
    let result = match &value {
        Value::Bit(v) => *v,
        Value::Null => false,
        other => truthy(other),
    };
    Ok(result)
}

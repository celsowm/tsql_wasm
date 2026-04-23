pub(crate) mod conversion;
pub(crate) mod literal;
pub(crate) mod predicate;
pub(crate) mod special;
pub(crate) mod subquery;
pub(crate) mod udf;

pub(crate) use udf::eval_udf_body;

use crate::ast::Expr;
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::executor::clock::Clock;
use crate::executor::context::ExecutionContext;
use crate::executor::model::ContextTable;
use crate::executor::operators::{eval_binary, eval_unary};
use crate::executor::scalar::eval_function;
use crate::executor::value_ops::coerce_value_to_type_with_dateformat;
use crate::storage::Storage;
use crate::types::{DataType, Value};

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
    row: &[ContextTable],
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
    let row: Vec<ContextTable> = vec![];
    eval_expr(expr, &row, ctx, catalog, storage, clock)
}

const MAX_RECURSION_DEPTH: usize = 32;

#[inline]
pub fn eval_expr(
    expr: &Expr,
    row: &[ContextTable],
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
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    match expr {
        Expr::Identifier(_)
        | Expr::QualifiedIdentifier(_)
        | Expr::Wildcard
        | Expr::QualifiedWildcard(_)
        | Expr::Integer(_)
        | Expr::FloatLiteral(_)
        | Expr::BinaryLiteral(_)
        | Expr::String(_)
        | Expr::UnicodeString(_)
        | Expr::Null => literal::eval_literal_expr(expr, row, ctx),

        Expr::Cast { .. }
        | Expr::TryCast { .. }
        | Expr::Convert { .. }
        | Expr::TryConvert { .. }
        | Expr::IsNull(_)
        | Expr::IsNotNull(_) => {
            conversion::eval_conversion_expr(expr, row, ctx, catalog, storage, clock)
        }

        Expr::Case { .. }
        | Expr::InList { .. }
        | Expr::Between { .. }
        | Expr::Like { .. } => {
            predicate::eval_predicate_expr(expr, row, ctx, catalog, storage, clock)
        }

        Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } => {
            subquery::eval_subquery_expr(expr, row, ctx, catalog, storage, clock)
        }

        Expr::WindowFunction { .. } | Expr::NextValueFor { .. } => {
            special::eval_special_runtime_expr(expr, ctx)
        }

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
    }
}

pub(crate) fn eval_predicate(
    expr: &Expr,
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<bool, DbError> {
    let value = eval_expr(expr, row, ctx, catalog, storage, clock)?;
    let result = match &value {
        Value::Bit(v) => *v,
        Value::Null => false,
        other => crate::executor::value_ops::truthy(other),
    };
    Ok(result)
}

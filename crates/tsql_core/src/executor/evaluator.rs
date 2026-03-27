use crate::ast::Expr;
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::storage::Storage;
use crate::types::{DataType, Value};

use super::clock::Clock;
use super::context::ExecutionContext;
use super::identifier::{resolve_identifier, resolve_qualified_identifier};
use super::model::JoinedRow;
use super::operators::{eval_binary, eval_unary};
use super::script::ScriptExecutor;
use super::predicates::{
    eval_between, eval_case, eval_exists, eval_in_list, eval_in_subquery, eval_like,
    eval_scalar_subquery,
};
use super::scalar::eval_function;
use super::type_mapping::data_type_spec_to_runtime;
use super::value_ops::{coerce_value_to_type, truthy};

pub(crate) fn eval_expr_to_type_constant(
    expr: &Expr,
    ty: &DataType,
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    let value = eval_constant_expr(expr, ctx, catalog, storage, clock)?;
    coerce_value_to_type(value, ty)
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
    coerce_value_to_type(value, ty)
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

pub(crate) fn eval_udf_body(
    stmts: &[crate::ast::Statement],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    let mut catalog_owned = if let Some(impl_ref) = catalog.as_any().downcast_ref::<crate::catalog::CatalogImpl>() {
        Box::new(impl_ref.clone()) as Box<dyn Catalog>
    } else {
        return Err(DbError::Execution("UDF requires cloneable catalog".into()));
    };

    let mut storage_owned = if let Some(impl_ref) = storage.as_any().downcast_ref::<crate::storage::InMemoryStorage>() {
        Box::new(impl_ref.clone()) as Box<dyn Storage>
    } else if let Some(impl_ref) = storage.as_any().downcast_ref::<crate::storage::RedbStorage>() {
        Box::new(impl_ref.clone()) as Box<dyn Storage>
    } else {
        return Err(DbError::Execution("UDF requires cloneable storage".into()));
    };

    let mut executor = ScriptExecutor {
        catalog: catalog_owned.as_mut(),
        storage: storage_owned.as_mut(),
        clock,
    };
    match executor.execute_batch(stmts, ctx) {
        Err(DbError::Return(Some(val))) => Ok(val),
        Err(DbError::Return(None)) => Ok(Value::Null),
        Ok(_) => Ok(Value::Null),
        Err(e) => Err(e),
    }
}

pub fn eval_expr(
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
        Expr::Integer(v) => Ok(if *v >= i32::MIN as i64 && *v <= i32::MAX as i64 {
            Value::Int(*v as i32)
        } else {
            Value::BigInt(*v)
        }),
        Expr::FloatLiteral(s) => {
            let f: f64 = s
                .parse()
                .map_err(|_| DbError::Execution(format!("invalid float literal '{}'", s)))?;
            Ok(Value::Float(f.to_bits()))
        }
        Expr::BinaryLiteral(bytes) => Ok(Value::Binary(bytes.clone())),
        Expr::String(v) => Ok(Value::VarChar(v.clone())),
        Expr::UnicodeString(v) => Ok(Value::NVarChar(v.clone())),
        Expr::Null => Ok(Value::Null),
        Expr::FunctionCall { name, args } => {
            eval_function(name, args, row, ctx, catalog, storage, clock)
        }
        Expr::Binary { left, op, right } => {
            let lv = eval_expr(left, row, ctx, catalog, storage, clock)?;
            let rv = eval_expr(right, row, ctx, catalog, storage, clock)?;
            eval_binary(op, lv, rv, ctx.ansi_nulls)
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
            coerce_value_to_type(value, &data_type_spec_to_runtime(target))
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
                )
            } else {
                coerce_value_to_type(value, &data_type_spec_to_runtime(target))
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
            if let Some(val) = ctx.get_window_value(expr) {
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

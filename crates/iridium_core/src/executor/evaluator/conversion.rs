use crate::ast::Expr;
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::executor::clock::Clock;
use crate::executor::context::ExecutionContext;
use crate::executor::model::ContextTable;
use crate::executor::type_mapping::data_type_spec_to_runtime;
use crate::executor::value_ops::{coerce_value_to_type_with_dateformat, convert_with_style};
use crate::storage::Storage;
use crate::types::Value;

use super::eval_expr;

pub(crate) fn eval_conversion_expr(
    expr: &Expr,
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    match expr {
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
                convert_with_style(
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
                convert_with_style(
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
        _ => Err(DbError::Execution(
            "eval_conversion_expr called with non-conversion expression".into(),
        )),
    }
}

use std::cmp::Ordering;

use crate::error::DbError;
use crate::types::Value;

use crate::executor::context::ExecutionContext;
use crate::executor::model::JoinedRow;
use crate::executor::projection::compare_projected_rows;
use crate::executor::query::plan::RelationalQuery;
use crate::executor::value_ops;

use super::super::QueryExecutor;
use super::order_validation::validate_projected_order_by;

pub(crate) fn apply_source_ordering(
    executor: &QueryExecutor<'_>,
    query: &RelationalQuery,
    source_rows: &mut [JoinedRow],
    ctx: &mut ExecutionContext,
) -> Result<(), DbError> {
    let order_by = &query.sort.order_by;
    source_rows.sort_by(|a, b| {
        for item in order_by {
            let va = crate::executor::evaluator::eval_expr(
                &item.expr,
                a,
                ctx,
                executor.catalog,
                executor.storage,
                executor.clock,
            )
            .unwrap_or(Value::Null);
            let vb = crate::executor::evaluator::eval_expr(
                &item.expr,
                b,
                ctx,
                executor.catalog,
                executor.storage,
                executor.clock,
            )
            .unwrap_or(Value::Null);
            let ord = value_ops::compare_values(&va, &vb);
            if ord != Ordering::Equal {
                return if item.asc { ord } else { ord.reverse() };
            }
        }
        Ordering::Equal
    });
    Ok(())
}

pub(crate) fn apply_result_ordering(
    _executor: &QueryExecutor<'_>,
    query: &RelationalQuery,
    columns: &[String],
    mut final_rows: Vec<Vec<Value>>,
    _ctx: &mut ExecutionContext,
) -> Result<Vec<Vec<Value>>, DbError> {
    let order_by_refs = &query.sort.order_by;
    validate_projected_order_by(columns, order_by_refs)?;
    final_rows.sort_by(|a, b| {
        compare_projected_rows(a, b, columns, order_by_refs).unwrap_or(Ordering::Equal)
    });
    Ok(final_rows)
}

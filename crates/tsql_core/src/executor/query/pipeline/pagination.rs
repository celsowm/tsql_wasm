use crate::error::DbError;

use crate::executor::context::ExecutionContext;
use crate::executor::projection::eval_top_n;
use crate::executor::query::plan::RelationalQuery;

use super::super::QueryExecutor;

pub(crate) fn apply_pagination(
    executor: &QueryExecutor<'_>,
    query: &RelationalQuery,
    mut final_rows: Vec<Vec<crate::types::Value>>,
    ctx: &mut ExecutionContext,
) -> Result<Vec<Vec<crate::types::Value>>, DbError> {
    if let Some(top) = query.pagination.top.clone() {
        let n = eval_top_n(
            &top,
            ctx,
            executor.catalog,
            executor.storage,
            executor.clock,
        )?;
        if final_rows.len() > n {
            final_rows.truncate(n);
        }
    }

    if let Some(offset_expr) = query.pagination.offset.clone() {
        let offset_val = crate::executor::evaluator::eval_expr(
            &offset_expr,
            &[],
            ctx,
            executor.catalog,
            executor.storage,
            executor.clock,
        )?;
        let offset_n = value_to_usize(offset_val);
        if offset_n < final_rows.len() {
            final_rows = final_rows[offset_n..].to_vec();
        } else {
            final_rows = vec![];
        }
        if let Some(fetch_expr) = query.pagination.fetch.clone() {
            let fetch_val = crate::executor::evaluator::eval_expr(
                &fetch_expr,
                &[],
                ctx,
                executor.catalog,
                executor.storage,
                executor.clock,
            )?;
            let fetch_n = value_to_usize(fetch_val);
            if final_rows.len() > fetch_n {
                final_rows.truncate(fetch_n);
            }
        }
    }

    Ok(final_rows)
}

fn value_to_usize(value: crate::types::Value) -> usize {
    match value {
        crate::types::Value::Int(n) => n.max(0) as usize,
        crate::types::Value::BigInt(n) => n.max(0) as usize,
        crate::types::Value::SmallInt(n) => n.max(0) as usize,
        crate::types::Value::TinyInt(n) => n as usize,
        _ => 0,
    }
}

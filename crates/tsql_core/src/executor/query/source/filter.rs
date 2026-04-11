use crate::error::DbError;

use super::super::from_tree::FromEval;
use super::super::plan::RelationalQuery;
use super::super::QueryExecutor;
use crate::executor::binder::{bind_expr, eval_bound_expr, BoundExpr};

pub(crate) fn execute_where_stage(
    executor: &QueryExecutor<'_>,
    query: &RelationalQuery,
    mut source_eval: FromEval,
    ctx: &mut crate::executor::context::ExecutionContext,
) -> Result<FromEval, DbError> {
    if let Some(where_clause) = &query.filter.selection {
        source_eval.rows = apply_where_filter(
            executor,
            source_eval.rows,
            &source_eval.shape,
            where_clause,
            ctx,
        )?;
    }
    Ok(source_eval)
}

fn apply_where_filter(
    executor: &QueryExecutor<'_>,
    rows: Vec<Vec<crate::executor::model::ContextTable>>,
    shape: &[crate::executor::model::ContextTable],
    where_clause: &crate::ast::Expr,
    ctx: &mut crate::executor::context::ExecutionContext,
) -> Result<Vec<Vec<crate::executor::model::ContextTable>>, DbError> {
    let bound_where = bind_expr(where_clause, shape, ctx)
        .unwrap_or_else(|_| BoundExpr::Dynamic(where_clause.clone()));

    let mut filtered = Vec::new();
    for row in rows {
        if crate::executor::value_ops::truthy(&eval_bound_expr(
            &bound_where,
            &row,
            ctx,
            executor.catalog,
            executor.storage,
            executor.clock,
        )?) {
            filtered.push(row);
        }
    }
    Ok(filtered)
}

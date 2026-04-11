use crate::error::DbError;

use super::super::from_tree::FromEval;
use super::super::plan::RelationalQuery;
use super::super::QueryExecutor;

pub(crate) fn execute_apply_stage(
    executor: &QueryExecutor<'_>,
    query: &RelationalQuery,
    mut source_eval: FromEval,
    ctx: &mut crate::executor::context::ExecutionContext,
) -> Result<FromEval, DbError> {
    if query.applies.is_empty() {
        return Ok(source_eval);
    }

    let mut rows = source_eval.materialize(ctx, executor.catalog, executor.storage, executor.clock)?;
    for apply_clause in &query.applies {
        rows = super::super::transformer::execute_apply(
            rows,
            apply_clause,
            ctx,
            executor,
        )?;
    }
    let shape = rows.first().cloned().unwrap_or_default();
    Ok(FromEval {
        iter: Box::new(super::super::from_tree::ScanIterator::new(rows)),
        shape,
    })
}

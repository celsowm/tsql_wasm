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
    for apply_clause in &query.applies {
        source_eval.rows = super::super::transformer::execute_apply(
            source_eval.rows,
            apply_clause,
            ctx,
            executor,
        )?;
        source_eval.shape = source_eval.rows.first().cloned().unwrap_or_default();
    }
    Ok(source_eval)
}

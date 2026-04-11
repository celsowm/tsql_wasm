mod apply;
mod filter;
mod from;

use crate::error::DbError;

use super::from_tree::FromEval;
use super::plan::RelationalQuery;
use super::QueryExecutor;

pub(crate) fn execute_source(
    executor: &QueryExecutor<'_>,
    query: &RelationalQuery,
    ctx: &mut crate::executor::context::ExecutionContext,
) -> Result<FromEval, DbError> {
    let source_eval = from::execute_from_stage(executor, query, ctx)?;
    let source_eval = apply::execute_apply_stage(executor, query, source_eval, ctx)?;
    filter::execute_where_stage(executor, query, source_eval, ctx)
}

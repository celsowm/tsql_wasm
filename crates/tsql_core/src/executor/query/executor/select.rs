use crate::error::DbError;
use crate::executor::result::QueryResult;

use super::super::from_tree;
use super::super::finalize;
use super::super::plan::RelationalQuery;
use super::super::source;
use super::super::QueryExecutor;
use crate::executor::context::ExecutionContext;

pub(crate) fn execute_select_internal(
    executor: &QueryExecutor<'_>,
    query: &RelationalQuery,
    ctx: &mut ExecutionContext,
) -> Result<QueryResult, DbError> {
    from_tree::enforce_query_governor_cost_limit(query, ctx)?;
    let source_eval = source::execute_source(executor, query, ctx)?;
    finalize::finalize_rows(executor, query, source_eval.rows, ctx)
}

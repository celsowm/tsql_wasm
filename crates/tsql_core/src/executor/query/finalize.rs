use crate::error::DbError;
use crate::executor::context::ExecutionContext;
use crate::executor::model::JoinedRow;
use crate::executor::result;

use super::pipeline;
use super::plan::RelationalQuery;
use super::QueryExecutor;

pub(crate) fn finalize_rows(
    executor: &QueryExecutor<'_>,
    query: &RelationalQuery,
    source_rows: Vec<JoinedRow>,
    ctx: &mut ExecutionContext,
) -> Result<result::QueryResult, DbError> {
    pipeline::execute_rows_to_result(executor, query, source_rows, ctx)
}

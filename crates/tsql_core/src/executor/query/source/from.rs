use crate::error::DbError;

use super::super::from_tree::{self, FromEval};
use super::super::plan::RelationalQuery;
use super::super::QueryExecutor;

pub(crate) fn execute_from_stage(
    executor: &QueryExecutor<'_>,
    query: &RelationalQuery,
    ctx: &mut crate::executor::context::ExecutionContext,
) -> Result<FromEval, DbError> {
    if let Some(from_clause) = query.from_clause.clone() {
        from_tree::execute_from_clause(executor, from_clause, ctx)
    } else {
        Ok(FromEval {
            rows: vec![vec![]],
            shape: vec![],
        })
    }
}

use crate::error::DbError;

use super::super::from_tree::{self, FromEval};
use super::super::plan::RelationalQuery;
use super::super::QueryExecutor;

pub(crate) fn execute_where_stage(
    _executor: &QueryExecutor<'_>,
    query: &RelationalQuery,
    source_eval: FromEval,
    _ctx: &mut crate::executor::context::ExecutionContext,
) -> Result<FromEval, DbError> {
    if let Some(where_clause) = &query.filter.selection {
        let shape = source_eval.shape.clone();
        let iter = Box::new(from_tree::FilterIterator {
            source: source_eval.iter,
            predicate: where_clause.clone(),
        });
        Ok(FromEval { iter, shape })
    } else {
        Ok(source_eval)
    }
}

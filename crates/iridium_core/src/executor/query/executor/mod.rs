mod select;

use crate::error::DbError;
use crate::executor::result::QueryResult;

use super::plan::RelationalQuery;
use super::source;
use super::QueryExecutor;
use crate::executor::context::ExecutionContext;
use crate::executor::model::JoinedRow;

impl<'a> QueryExecutor<'a> {
    pub fn execute_select(
        &self,
        query: RelationalQuery,
        ctx: &mut ExecutionContext,
    ) -> Result<QueryResult, DbError> {
        let set_op = query.set_op.clone();
        let into_table = query.into_table.clone();
        let result = select::execute_select_internal(self, &query, ctx)?;
        let mut result = result;

        if let Some(set_op) = set_op {
            let right = self.execute_select(RelationalQuery::from(set_op.right), ctx)?;
            result = crate::executor::engine::execute_set_op(result, right, set_op.kind)?;
        }

        if ctx.options.rowcount > 0 && result.rows.len() > ctx.options.rowcount as usize {
            result.rows.truncate(ctx.options.rowcount as usize);
        }

        if into_table.is_some() {
            return Err(DbError::Execution(
                "SELECT INTO is handled by ScriptExecutor".into(),
            ));
        }

        Ok(result)
    }

    pub fn execute_to_joined_rows(
        &self,
        query: RelationalQuery,
        ctx: &mut ExecutionContext,
    ) -> Result<Vec<JoinedRow>, DbError> {
        let mut source_eval = source::execute_source(self, &query, ctx)?;
        source_eval.materialize(ctx, self.catalog, self.storage, self.clock)
    }
}

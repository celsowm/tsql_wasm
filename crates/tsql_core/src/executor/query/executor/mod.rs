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
        let into_table = query.into_table.clone();
        let result = select::execute_select_internal(self, &query, ctx)?;
        let mut result = result;

        if ctx.options.rowcount > 0 && result.rows.len() > ctx.options.rowcount as usize {
            result.rows.truncate(ctx.options.rowcount as usize);
        }

        if into_table.is_some() {
            return Err(DbError::Execution("SELECT INTO is handled by ScriptExecutor".into()));
        }

        Ok(result)
    }

    pub fn execute_to_joined_rows(
        &self,
        query: RelationalQuery,
        ctx: &mut ExecutionContext,
    ) -> Result<Vec<JoinedRow>, DbError> {
        source::execute_source(self, &query, ctx).map(|eval| eval.rows)
    }
}

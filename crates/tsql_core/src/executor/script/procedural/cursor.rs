use crate::ast::FetchCursorStmt;
use crate::error::DbError;
use crate::executor::context::ExecutionContext;
use crate::executor::query::QueryExecutor;
use crate::executor::query::plan::RelationalQuery;
use crate::executor::result::QueryResult;
use crate::executor::value_ops::coerce_value_to_type_with_dateformat;
use crate::catalog::Catalog;
use crate::storage::Storage;
use super::super::ScriptExecutor;

impl<'a> ScriptExecutor<'a> {
    pub(crate) fn execute_open_cursor(
        &mut self,
        name: String,
        ctx: &mut ExecutionContext<'_>,
    ) -> Result<Option<QueryResult>, DbError> {
        let mut cursor = ctx.session.cursors.get(&name).cloned().ok_or_else(|| {
            DbError::cursor_not_declared(&name)
        })?;
        let query = cursor.query.clone().ok_or_else(|| {
            DbError::cursor_has_no_query(&name)
        })?;
        let result = QueryExecutor {
            catalog: self.catalog as &dyn Catalog,
            storage: self.storage as &dyn Storage,
            clock: self.clock,
        }
        .execute_select(RelationalQuery::from(query), ctx)?;
        cursor.query_result = result;
        cursor.current_row = -1;
        ctx.session.cursors.insert(name, cursor);
        Ok(None)
    }

    pub(crate) fn execute_close_cursor(
        &mut self,
        name: String,
        ctx: &mut ExecutionContext<'_>,
    ) -> Result<Option<QueryResult>, DbError> {
        let mut cursor = ctx.session.cursors.get(&name).cloned().ok_or_else(|| {
            DbError::cursor_not_declared(&name)
        })?;
        cursor.current_row = -1;
        ctx.session.cursors.insert(name, cursor);
        Ok(None)
    }

    pub(crate) fn execute_deallocate_cursor(
        &mut self,
        name: String,
        ctx: &mut ExecutionContext<'_>,
    ) -> Result<Option<QueryResult>, DbError> {
        ctx.session.cursors.remove(&name);
        Ok(None)
    }

    pub(crate) fn execute_fetch_cursor(
        &mut self,
        stmt: FetchCursorStmt,
        ctx: &mut ExecutionContext<'_>,
    ) -> Result<Option<QueryResult>, DbError> {
        let mut cursor = ctx.session.cursors.get(&stmt.name).cloned().ok_or_else(|| {
            DbError::cursor_not_declared(&stmt.name)
        })?;

        let row_count = cursor.query_result.rows.len() as i64;

        match stmt.direction {
            crate::ast::FetchDirection::Next => {
                cursor.current_row += 1;
            }
            crate::ast::FetchDirection::Prior => {
                cursor.current_row -= 1;
            }
            crate::ast::FetchDirection::First => {
                cursor.current_row = 0;
            }
            crate::ast::FetchDirection::Last => {
                cursor.current_row = row_count - 1;
            }
            crate::ast::FetchDirection::Absolute(expr) => {
                let val = super::super::super::evaluator::eval_expr(&expr, &[], ctx, self.catalog, self.storage, self.clock)?;
                let n = val.to_integer_i64().unwrap_or(0);
                if n > 0 {
                    cursor.current_row = n - 1;
                } else if n < 0 {
                    cursor.current_row = row_count + n;
                } else {
                    cursor.current_row = -1; // Before first
                }
            }
            crate::ast::FetchDirection::Relative(expr) => {
                let val = super::super::super::evaluator::eval_expr(&expr, &[], ctx, self.catalog, self.storage, self.clock)?;
                let n = val.to_integer_i64().unwrap_or(0);
                cursor.current_row += n;
            }
        }

        if cursor.current_row >= 0 && cursor.current_row < row_count {
            *ctx.session.fetch_status = 0;
            if let Some(into_vars) = stmt.into {
                let row = &cursor.query_result.rows[cursor.current_row as usize];
                if into_vars.len() != row.len() {
                    return Err(DbError::Execution(format!(
                        "FETCH INTO expected {} variables, got {}",
                        row.len(),
                        into_vars.len()
                    )));
                }
                for (idx, var_name) in into_vars.iter().enumerate() {
                    if let Some((ty, var)) = ctx.session.variables.get_mut(var_name) {
                        *var = coerce_value_to_type_with_dateformat(
                            row[idx].clone(),
                            ty,
                            &ctx.options.dateformat,
                        )?;
                    } else {
                        return Err(DbError::invalid_identifier(var_name));
                    }
                }
            }
        } else {
            *ctx.session.fetch_status = -1;
            // Adjust current_row to stay just outside boundaries for subsequent relative/next/prior
            if cursor.current_row < 0 {
                cursor.current_row = -1;
            } else if cursor.current_row >= row_count {
                cursor.current_row = row_count;
            }
        }

        ctx.session.cursors.insert(stmt.name, cursor);
        Ok(None)
    }
}

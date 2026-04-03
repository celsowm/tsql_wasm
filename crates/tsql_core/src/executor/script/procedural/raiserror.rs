use crate::ast::RaiserrorStmt;
use crate::error::DbError;
use crate::executor::context::ExecutionContext;
use crate::executor::evaluator::eval_expr;
use super::super::ScriptExecutor;

impl<'a> ScriptExecutor<'a> {
    pub(crate) fn execute_raiserror(
        &mut self,
        stmt: RaiserrorStmt,
        ctx: &mut ExecutionContext<'_>,
    ) -> Result<Option<crate::executor::result::QueryResult>, DbError> {
        let msg_val = eval_expr(
            &stmt.message,
            &[],
            ctx,
            self.catalog,
            self.storage,
            self.clock,
        )?;
        let severity_val = eval_expr(
            &stmt.severity,
            &[],
            ctx,
            self.catalog,
            self.storage,
            self.clock,
        )?;
        let _state_val = eval_expr(
            &stmt.state,
            &[],
            ctx,
            self.catalog,
            self.storage,
            self.clock,
        )?;

        let severity = match severity_val {
            crate::types::Value::Int(v) => v,
            crate::types::Value::TinyInt(v) => v as i32,
            crate::types::Value::SmallInt(v) => v as i32,
            _ => 16,
        };

        let msg = msg_val.to_string_value();

        if severity >= 16 {
            // For now, we'll treat high severity as an execution error
            Err(DbError::Execution(msg))
        } else {
            // Low severity just prints
            ctx.session.print_output.push(msg);
            Ok(None)
        }
    }
}

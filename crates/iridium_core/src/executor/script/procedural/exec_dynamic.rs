use super::super::ScriptExecutor;
use crate::ast::ExecStmt;
use crate::error::StmtResult;
use crate::executor::context::ExecutionContext;
use crate::executor::result::QueryResult;

pub(crate) fn execute_exec_dynamic(
    exec: &mut ScriptExecutor<'_>,
    stmt: ExecStmt,
    ctx: &mut ExecutionContext<'_>,
) -> StmtResult<Option<QueryResult>> {
    let sql_val = crate::executor::evaluator::eval_expr(
        &stmt.sql_expr,
        &[],
        ctx,
        exec.catalog,
        exec.storage,
        exec.clock,
    )?;
    let sql_str = sql_val.to_string_value();
    let batch = crate::parser::parse_batch(&sql_str)?;

    ctx.enter_scope();
    let res = exec.execute_batch(&batch, ctx);
    exec.cleanup_scope_table_vars(ctx)?;
    res
}

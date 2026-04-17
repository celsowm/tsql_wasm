pub(crate) mod assignment;
pub(crate) mod control_flow;
pub(crate) mod cursor;
pub(crate) mod definitions;
pub(crate) mod exec_dynamic;
pub(crate) mod print;
pub(crate) mod procedure;
pub(crate) mod raiserror;
pub(crate) mod routine;
pub(crate) mod throw;
pub(crate) mod shared;
pub(crate) mod sp_executesql;
pub(crate) mod system_procedures;
pub(crate) mod try_catch;
pub(crate) mod variable;

use self::exec_dynamic::execute_exec_dynamic;
use self::sp_executesql::execute_sp_executesql;
use super::ScriptExecutor;
use crate::ast::ProceduralStatement;
use crate::error::DbError;
use crate::executor::context::ExecutionContext;

impl<'a> ScriptExecutor<'a> {
    pub(crate) fn execute_procedural(
        &mut self,
        proc: ProceduralStatement,
        ctx: &mut ExecutionContext<'_>,
    ) -> crate::error::StmtResult<Option<crate::executor::result::QueryResult>> {
        use crate::error::StmtOutcome;

        match proc {
            ProceduralStatement::Declare(stmt) => {
                self.execute_declare(stmt, ctx).map(StmtOutcome::Ok)
            }
            ProceduralStatement::DeclareTableVar(stmt) => self
                .execute_declare_table_var(stmt, ctx)
                .map(StmtOutcome::Ok),
            ProceduralStatement::Set(stmt) => self.execute_set(stmt, ctx).map(StmtOutcome::Ok),
            ProceduralStatement::SetOption(_) => Err(DbError::Execution(
                "SET option statements are handled at engine level".into(),
            )),
            ProceduralStatement::If(stmt) => self.execute_if(stmt, ctx),
            ProceduralStatement::BeginEnd(stmts) => self.execute_batch(&stmts, ctx),
            ProceduralStatement::While(stmt) => self.execute_while(stmt, ctx),
            ProceduralStatement::Break => self.execute_break(ctx),
            ProceduralStatement::Continue => self.execute_continue(ctx),
            ProceduralStatement::Return(expr) => self.execute_return(expr, ctx),
            ProceduralStatement::ExecDynamic(stmt) => execute_exec_dynamic(self, stmt, ctx),
            ProceduralStatement::ExecProcedure(stmt) => {
                self.execute_procedure(stmt, ctx).map(StmtOutcome::Ok)
            }
            ProceduralStatement::SpExecuteSql(stmt) => {
                execute_sp_executesql(self, stmt, ctx).map(StmtOutcome::Ok)
            }
            ProceduralStatement::Print(expr) => self.execute_print(expr, ctx).map(StmtOutcome::Ok),
            ProceduralStatement::Raiserror(stmt) => {
                self.execute_raiserror(stmt, ctx).map(StmtOutcome::Ok)
            }
            ProceduralStatement::Throw(stmt) => {
                self.execute_throw(stmt, ctx).map(StmtOutcome::Ok)
            }
            ProceduralStatement::TryCatch(stmt) => self.execute_try_catch(stmt, ctx),
            ProceduralStatement::DeclareCursor(stmt) => self.execute_declare_cursor(stmt, ctx),
            ProceduralStatement::CreateProcedure(stmt) => {
                self.execute_create_procedure(stmt)?;
                Ok(StmtOutcome::Ok(None))
            }
            ProceduralStatement::CreateFunction(stmt) => {
                self.execute_create_function(stmt)?;
                Ok(StmtOutcome::Ok(None))
            }
            ProceduralStatement::CreateView(stmt) => {
                self.execute_create_view(stmt, ctx)?;
                Ok(StmtOutcome::Ok(None))
            }
            ProceduralStatement::CreateTrigger(stmt) => {
                self.execute_create_trigger(stmt)?;
                Ok(StmtOutcome::Ok(None))
            }
        }
    }

    pub(crate) fn leave_scope_and_cleanup(
        &mut self,
        ctx: &mut ExecutionContext<'_>,
    ) -> Result<(), DbError> {
        self.cleanup_scope_table_vars(ctx)
    }
}

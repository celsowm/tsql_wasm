pub(crate) mod assignment;
pub(crate) mod control_flow;
pub(crate) mod cursor;
pub(crate) mod raiserror;
pub(crate) mod try_catch;
pub(crate) mod routine;
pub(crate) mod print;
pub(crate) mod variable;

use crate::error::DbError;
use crate::executor::context::ExecutionContext;
use crate::ast::ProceduralStatement;
use super::ScriptExecutor;

impl<'a> ScriptExecutor<'a> {
    pub(crate) fn execute_procedural(
        &mut self,
        proc: ProceduralStatement,
        ctx: &mut ExecutionContext<'_>,
    ) -> crate::error::StmtResult<Option<crate::executor::result::QueryResult>> {
        use crate::catalog::{RoutineDef, RoutineKind, TriggerDef};
        use crate::error::StmtOutcome;
        use crate::executor::model::Cursor;
        use crate::executor::result::QueryResult;
        use crate::executor::tooling::{format_routine_definition, format_trigger_definition};

        match proc {
            ProceduralStatement::Declare(stmt) => {
                self.execute_declare(stmt, ctx).map(StmtOutcome::Ok)
            }
            ProceduralStatement::DeclareTableVar(stmt) => {
                self.execute_declare_table_var(stmt, ctx).map(StmtOutcome::Ok)
            }
            ProceduralStatement::Set(stmt) => self.execute_set(stmt, ctx).map(StmtOutcome::Ok),
            ProceduralStatement::SetOption(_) => Err(DbError::Execution(
                "SET option statements are handled at engine level".into(),
            )),
            ProceduralStatement::If(stmt) => self.execute_if(stmt, ctx),
            ProceduralStatement::BeginEnd(stmts) => self.execute_batch(&stmts, ctx),
            ProceduralStatement::While(stmt) => self.execute_while(stmt, ctx),
            ProceduralStatement::Break => {
                if ctx.loop_depth() > 0 {
                    Ok(StmtOutcome::Break)
                } else {
                    Err(DbError::Execution("BREAK outside of WHILE".into()))
                }
            }
            ProceduralStatement::Continue => {
                if ctx.loop_depth() > 0 {
                    Ok(StmtOutcome::Continue)
                } else {
                    Err(DbError::Execution("CONTINUE outside of WHILE".into()))
                }
            }
            ProceduralStatement::Return(expr) => self.execute_return(expr, ctx),
            ProceduralStatement::ExecDynamic(stmt) => {
                let sql_val = crate::executor::evaluator::eval_expr(
                    &stmt.sql_expr,
                    &[],
                    ctx,
                    self.catalog,
                    self.storage,
                    self.clock,
                )?;
                let sql_str = sql_val.to_string_value();
                let batch = crate::parser::parse_batch(&sql_str)?;

                ctx.enter_scope();
                let res = self.execute_batch(&batch, ctx);
                self.cleanup_scope_table_vars(ctx)?;
                res
            }
            ProceduralStatement::ExecProcedure(stmt) => {
                self.execute_procedure(stmt, ctx).map(StmtOutcome::Ok)
            }
            ProceduralStatement::SpExecuteSql(stmt) => {
                self.execute_sp_executesql(stmt, ctx).map(StmtOutcome::Ok)
            }
            ProceduralStatement::Print(expr) => self.execute_print(expr, ctx).map(StmtOutcome::Ok),
            ProceduralStatement::Raiserror(stmt) => {
                self.execute_raiserror(stmt, ctx).map(StmtOutcome::Ok)
            }
            ProceduralStatement::TryCatch(stmt) => self.execute_try_catch(stmt, ctx),
            ProceduralStatement::DeclareCursor(stmt) => {
                ctx.session.cursors.insert(
                    stmt.name.clone(),
                    Cursor {
                        query: Some(stmt.query),
                        query_result: QueryResult::default(),
                        current_row: -1,
                    },
                );
                Ok(StmtOutcome::Ok(None))
            }
            ProceduralStatement::CreateProcedure(stmt) => {
                let schema = stmt.name.schema_or_dbo().to_string();
                let mut routine = RoutineDef {
                    object_id: self.catalog.alloc_object_id(),
                    schema,
                    name: stmt.name.name,
                    params: stmt.params,
                    kind: RoutineKind::Procedure { body: stmt.body },
                    definition_sql: String::new(),
                };
                routine.definition_sql = format_routine_definition(&routine);
                self.catalog.create_routine(routine)?;
                Ok(StmtOutcome::Ok(None))
            }
            ProceduralStatement::CreateFunction(stmt) => {
                let schema = stmt.name.schema_or_dbo().to_string();
                let mut routine = RoutineDef {
                    object_id: self.catalog.alloc_object_id(),
                    schema,
                    name: stmt.name.name,
                    params: stmt.params,
                    kind: RoutineKind::Function {
                        returns: stmt.returns,
                        body: stmt.body,
                    },
                    definition_sql: String::new(),
                };
                routine.definition_sql = format_routine_definition(&routine);
                self.catalog.create_routine(routine)?;
                Ok(StmtOutcome::Ok(None))
            }
            ProceduralStatement::CreateView(stmt) => {
                self.schema().create_view(stmt)?;
                Ok(StmtOutcome::Ok(None))
            }
            ProceduralStatement::CreateTrigger(stmt) => {
                let schema = stmt.name.schema_or_dbo().to_string();
                let mut trigger = TriggerDef {
                    object_id: self.catalog.alloc_object_id(),
                    schema,
                    name: stmt.name.name,
                    table_schema: stmt.table.schema_or_dbo().to_string(),
                    table_name: stmt.table.name,
                    events: stmt.events,
                    is_instead_of: stmt.is_instead_of,
                    body: stmt.body,
                    definition_sql: String::new(),
                };
                trigger.definition_sql = format_trigger_definition(&trigger);
                self.catalog.create_trigger(trigger)?;
                Ok(StmtOutcome::Ok(None))
            }
        }
    }

    pub(crate) fn leave_scope_and_cleanup(&mut self, ctx: &mut ExecutionContext<'_>) -> Result<(), DbError> {
        self.cleanup_scope_table_vars(ctx)
    }
}

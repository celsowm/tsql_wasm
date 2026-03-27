mod control_flow;
mod cte_proxy;
mod ddl_proxy;
mod dml;
mod dml_proxy;
mod procedural;
mod variable;

use super::clock::Clock;
use super::context::ExecutionContext;
use super::result::QueryResult;
use super::model::Cursor;
use super::schema::SchemaExecutor;
use crate::ast::{DropTableStmt, ObjectName, Statement};
use crate::catalog::{Catalog, RoutineDef, RoutineKind};
use crate::error::DbError;
use crate::storage::Storage;

pub struct ScriptExecutor<'a> {
    pub catalog: &'a mut dyn Catalog,
    pub storage: &'a mut dyn Storage,
    pub clock: &'a dyn Clock,
}

impl<'a> ScriptExecutor<'a> {
    pub fn execute(
        &mut self,
        stmt: Statement,
        ctx: &mut ExecutionContext,
    ) -> Result<Option<QueryResult>, DbError> {
        match stmt {
            Statement::BeginTransaction(_)
            | Statement::CommitTransaction
            | Statement::RollbackTransaction(_)
            | Statement::SaveTransaction(_)
            | Statement::SetTransactionIsolationLevel(_) => Err(DbError::Execution(
                "transaction control statements are only supported at top-level execution".into(),
            )),
            Statement::CreateTable(stmt) => self.execute_create_table(stmt, ctx),
            Statement::DropTable(stmt) => self.execute_drop_table(stmt, ctx),
            Statement::CreateIndex(stmt) => {
                SchemaExecutor {
                    catalog: self.catalog,
                    storage: self.storage,
                }
                .create_index(stmt)?;
                Ok(None)
            }
            Statement::DropIndex(stmt) => {
                SchemaExecutor {
                    catalog: self.catalog,
                    storage: self.storage,
                }
                .drop_index(stmt)?;
                Ok(None)
            }
            Statement::CreateSchema(stmt) => {
                SchemaExecutor {
                    catalog: self.catalog,
                    storage: self.storage,
                }
                .create_schema(stmt)?;
                Ok(None)
            }
            Statement::DropSchema(stmt) => {
                SchemaExecutor {
                    catalog: self.catalog,
                    storage: self.storage,
                }
                .drop_schema(stmt)?;
                Ok(None)
            }
            Statement::CreateView(stmt) => {
                SchemaExecutor {
                    catalog: self.catalog,
                    storage: self.storage,
                }
                .create_view(stmt)?;
                Ok(None)
            }
            Statement::DropView(stmt) => {
                SchemaExecutor {
                    catalog: self.catalog,
                    storage: self.storage,
                }
                .drop_view(stmt)?;
                Ok(None)
            }
            Statement::Insert(stmt) => self.execute_insert(stmt, ctx),
            Statement::Select(stmt) => self.execute_select_into(stmt, ctx),
            Statement::Update(stmt) => self.execute_update(stmt, ctx),
            Statement::Delete(stmt) => self.execute_delete(stmt, ctx),
            Statement::TruncateTable(stmt) => self.execute_truncate_table(stmt, ctx),
            Statement::AlterTable(stmt) => self.execute_alter_table(stmt, ctx),
            Statement::Declare(stmt) => self.execute_declare(stmt, ctx),
            Statement::DeclareTableVar(stmt) => self.execute_declare_table_var(stmt, ctx),
            Statement::Set(stmt) => self.execute_set(stmt, ctx),
            Statement::SetOption(_) => Err(DbError::Execution(
                "SET option statements are handled at engine level".into(),
            )),
            Statement::If(stmt) => self.execute_if(stmt, ctx),
            Statement::BeginEnd(stmts) => self.execute_batch(&stmts, ctx),
            Statement::While(stmt) => self.execute_while(stmt, ctx),
            Statement::Break => {
                if ctx.loop_depth > 0 {
                    Err(DbError::Break)
                } else {
                    Err(DbError::Execution("BREAK outside of WHILE".into()))
                }
            }
            Statement::Continue => {
                if ctx.loop_depth > 0 {
                    Err(DbError::Continue)
                } else {
                    Err(DbError::Execution("CONTINUE outside of WHILE".into()))
                }
            }
            Statement::Return(expr) => self.execute_return(expr, ctx),
            Statement::ExecDynamic(stmt) => {
                let sql_val = super::evaluator::eval_expr(
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
            Statement::ExecProcedure(stmt) => self.execute_procedure(stmt, ctx),
            Statement::SpExecuteSql(stmt) => self.execute_sp_executesql(stmt, ctx),
            Statement::SelectAssign(stmt) => self.execute_select_assign(stmt, ctx),
            Statement::CreateProcedure(stmt) => {
                let schema = stmt.name.schema_or_dbo().to_string();
                self.catalog.create_routine(RoutineDef {
                    schema,
                    name: stmt.name.name,
                    params: stmt.params,
                    kind: RoutineKind::Procedure { body: stmt.body },
                })?;
                Ok(None)
            }
            Statement::DropProcedure(stmt) => {
                let schema = stmt.name.schema_or_dbo().to_string();
                self.catalog.drop_routine(&schema, &stmt.name.name, false)?;
                Ok(None)
            }
            Statement::CreateFunction(stmt) => {
                let schema = stmt.name.schema_or_dbo().to_string();
                self.catalog.create_routine(RoutineDef {
                    schema,
                    name: stmt.name.name,
                    params: stmt.params,
                    kind: RoutineKind::Function {
                        returns: stmt.returns,
                        body: stmt.body,
                    },
                })?;
                Ok(None)
            }
            Statement::DropFunction(stmt) => {
                let schema = stmt.name.schema_or_dbo().to_string();
                self.catalog.drop_routine(&schema, &stmt.name.name, true)?;
                Ok(None)
            }
            Statement::WithCte(stmt) => self.execute_with_cte(stmt, ctx),
            Statement::SetOp(stmt) => {
                let left_result = self.execute(*stmt.left, ctx)?;
                let right_result = self.execute(*stmt.right, ctx)?;

                match (left_result, right_result) {
                    (Some(left), Some(right)) => {
                        let result = super::engine::execute_set_op(left, right, stmt.op)?;
                        Ok(Some(result))
                    }
                    _ => Err(DbError::Execution(
                        "set operations require both sides to return results".into(),
                    )),
                }
            }
            Statement::Merge(stmt) => self.execute_merge(stmt, ctx),
            Statement::Print(expr) => self.execute_print(expr, ctx),
            Statement::Raiserror(stmt) => self.execute_raiserror(stmt, ctx),
            Statement::TryCatch(stmt) => self.execute_try_catch(stmt, ctx),
            Statement::DeclareCursor(stmt) => {
                ctx.cursors.insert(stmt.name.clone(), Cursor {
                    query: Some(stmt.query),
                    query_result: QueryResult::default(),
                    current_row: -1,
                });
                Ok(None)
            }
            Statement::OpenCursor(name) => self.execute_open_cursor(name, ctx),
            Statement::FetchCursor(stmt) => self.execute_fetch_cursor(stmt, ctx),
            Statement::CloseCursor(name) => self.execute_close_cursor(name, ctx),
            Statement::DeallocateCursor(name) => self.execute_deallocate_cursor(name, ctx),
            Statement::CreateTrigger(stmt) => {
                let schema = stmt.name.schema_or_dbo().to_string();
                self.catalog.create_trigger(crate::catalog::TriggerDef {
                    schema,
                    name: stmt.name.name,
                    table_schema: stmt.table.schema_or_dbo().to_string(),
                    table_name: stmt.table.name,
                    events: stmt.events,
                    is_instead_of: stmt.is_instead_of,
                    body: stmt.body,
                })?;
                Ok(None)
            }
            Statement::DropTrigger(stmt) => {
                let schema = stmt.name.schema_or_dbo().to_string();
                self.catalog.drop_trigger(&schema, &stmt.name.name)?;
                Ok(None)
            }
        }
    }

    pub fn execute_batch(
        &mut self,
        stmts: &[Statement],
        ctx: &mut ExecutionContext,
    ) -> Result<Option<QueryResult>, DbError> {
        let mut last_result = Ok(None);
        let mut early_err: Option<DbError> = None;
        for stmt in stmts {
            match self.execute(stmt.clone(), ctx) {
                Ok(r) => {
                    last_result = Ok(r);
                }
                Err(DbError::Return(v)) => {
                    early_err = Some(DbError::Return(v));
                    break;
                }
                Err(e) => {
                    early_err = Some(e);
                    break;
                }
            };
        }
        if let Some(err) = early_err {
            Err(err)
        } else {
            last_result
        }
    }

    fn cleanup_scope_table_vars(&mut self, ctx: &mut ExecutionContext) -> Result<(), DbError> {
        let dropped_physical = ctx.leave_scope_collect_table_vars();
        for physical in dropped_physical {
            if self.catalog.find_table("dbo", &physical).is_none() {
                continue;
            }
            SchemaExecutor {
                catalog: self.catalog,
                storage: self.storage,
            }
            .drop_table(DropTableStmt {
                name: ObjectName {
                    schema: Some("dbo".to_string()),
                    name: physical,
                },
            })?;
        }
        Ok(())
    }
}

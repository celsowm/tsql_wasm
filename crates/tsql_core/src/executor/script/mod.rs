mod ddl;
mod dml;
mod procedural;

use super::clock::Clock;
use super::context::ExecutionContext;
use super::model::Cursor;
use super::result::QueryResult;
use super::schema::SchemaExecutor;
use super::tooling::{format_routine_definition, format_trigger_definition};
use crate::ast::{DropTableStmt, ObjectName, Statement, DmlStatement, DdlStatement, ProceduralStatement, TransactionStatement, CursorStatement, SessionStatement};
use crate::catalog::{Catalog, RoutineDef, RoutineKind, TriggerDef};
use crate::error::{DbError, StmtOutcome, StmtResult};
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
    ) -> StmtResult<Option<QueryResult>> {
        match stmt {
            Statement::Transaction(txn) => match txn {
                TransactionStatement::Begin(_)
                | TransactionStatement::Commit(_)
                | TransactionStatement::Rollback(_)
                | TransactionStatement::Save(_)
                | TransactionStatement::SetTransactionIsolationLevel(_) => Err(DbError::Execution(
                    "transaction control statements are only supported at top-level execution".into(),
                )),
            },
            Statement::Ddl(ddl) => match ddl {
                DdlStatement::CreateTable(stmt) => self.execute_create_table(stmt, ctx).map(StmtOutcome::Ok),
                DdlStatement::DropTable(stmt) => self.execute_drop_table(stmt, ctx).map(StmtOutcome::Ok),
                DdlStatement::CreateType(stmt) => {
                    SchemaExecutor {
                        catalog: self.catalog,
                        storage: self.storage,
                    }
                    .create_type(stmt)?;
                    Ok(StmtOutcome::Ok(None))
                }
                DdlStatement::DropType(stmt) => {
                    SchemaExecutor {
                        catalog: self.catalog,
                        storage: self.storage,
                    }
                    .drop_type(stmt)?;
                    Ok(StmtOutcome::Ok(None))
                }
                DdlStatement::CreateIndex(stmt) => {
                    SchemaExecutor {
                        catalog: self.catalog,
                        storage: self.storage,
                    }
                    .create_index(stmt)?;
                    Ok(StmtOutcome::Ok(None))
                }
                DdlStatement::DropIndex(stmt) => {
                    SchemaExecutor {
                        catalog: self.catalog,
                        storage: self.storage,
                    }
                    .drop_index(stmt)?;
                    Ok(StmtOutcome::Ok(None))
                }
                DdlStatement::CreateSchema(stmt) => {
                    SchemaExecutor {
                        catalog: self.catalog,
                        storage: self.storage,
                    }
                    .create_schema(stmt)?;
                    Ok(StmtOutcome::Ok(None))
                }
                DdlStatement::DropSchema(stmt) => {
                    SchemaExecutor {
                        catalog: self.catalog,
                        storage: self.storage,
                    }
                    .drop_schema(stmt)?;
                    Ok(StmtOutcome::Ok(None))
                }
                DdlStatement::TruncateTable(stmt) => self.execute_truncate_table(stmt, ctx).map(StmtOutcome::Ok),
                DdlStatement::AlterTable(stmt) => self.execute_alter_table(stmt, ctx).map(StmtOutcome::Ok),
                DdlStatement::CreateTable(_) | DdlStatement::CreateView(_)
                | DdlStatement::DropView(_) => unreachable!("handled separately"),
            },
            Statement::Dml(dml) => match dml {
                DmlStatement::Insert(stmt) => self.execute_insert(stmt, ctx).map(StmtOutcome::Ok),
                DmlStatement::Select(stmt) => self.execute_select_into(stmt, ctx).map(StmtOutcome::Ok),
                DmlStatement::Update(stmt) => self.execute_update(stmt, ctx).map(StmtOutcome::Ok),
                DmlStatement::Delete(stmt) => self.execute_delete(stmt, ctx).map(StmtOutcome::Ok),
                DmlStatement::Merge(stmt) => self.execute_merge(stmt, ctx).map(StmtOutcome::Ok),
                DmlStatement::SelectAssign(stmt) => self.execute_select_assign(stmt, ctx).map(StmtOutcome::Ok),
                DmlStatement::SetOp(stmt) => {
                    let left_outcome = self.execute(*stmt.left, ctx)?;
                    let right_outcome = self.execute(*stmt.right, ctx)?;

                    match (left_outcome, right_outcome) {
                        (StmtOutcome::Ok(Some(left)), StmtOutcome::Ok(Some(right))) => {
                            let result = super::engine::execute_set_op(left, right, stmt.op)?;
                            Ok(StmtOutcome::Ok(Some(result)))
                        }
                        (StmtOutcome::Break, _) | (_, StmtOutcome::Break) => Ok(StmtOutcome::Break),
                        (StmtOutcome::Continue, _) | (_, StmtOutcome::Continue) => Ok(StmtOutcome::Continue),
                        (StmtOutcome::Return(v), _) | (_, StmtOutcome::Return(v)) => Ok(StmtOutcome::Return(v)),
                        _ => Err(DbError::Execution(
                            "set operations require both sides to return results".into(),
                        )),
                    }
                }
            },
            Statement::Procedural(proc) => match proc {
                ProceduralStatement::Declare(stmt) => self.execute_declare(stmt, ctx).map(StmtOutcome::Ok),
                ProceduralStatement::DeclareTableVar(stmt) => self.execute_declare_table_var(stmt, ctx).map(StmtOutcome::Ok),
                ProceduralStatement::Set(stmt) => self.execute_set(stmt, ctx).map(StmtOutcome::Ok),
                ProceduralStatement::SetOption(_) => Err(DbError::Execution(
                    "SET option statements are handled at engine level".into(),
                )),
                ProceduralStatement::If(stmt) => self.execute_if(stmt, ctx),
                ProceduralStatement::BeginEnd(stmts) => self.execute_batch(&stmts, ctx),
                ProceduralStatement::While(stmt) => self.execute_while(stmt, ctx),
                ProceduralStatement::Break => {
                    if ctx.loop_depth > 0 {
                        Ok(StmtOutcome::Break)
                    } else {
                        Err(DbError::Execution("BREAK outside of WHILE".into()))
                    }
                }
                ProceduralStatement::Continue => {
                    if ctx.loop_depth > 0 {
                        Ok(StmtOutcome::Continue)
                    } else {
                        Err(DbError::Execution("CONTINUE outside of WHILE".into()))
                    }
                }
                ProceduralStatement::Return(expr) => self.execute_return(expr, ctx),
                ProceduralStatement::ExecDynamic(stmt) => {
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
                ProceduralStatement::ExecProcedure(stmt) => self.execute_procedure(stmt, ctx).map(StmtOutcome::Ok),
                ProceduralStatement::SpExecuteSql(stmt) => self.execute_sp_executesql(stmt, ctx).map(StmtOutcome::Ok),
                ProceduralStatement::Print(expr) => self.execute_print(expr, ctx).map(StmtOutcome::Ok),
                ProceduralStatement::Raiserror(stmt) => self.execute_raiserror(stmt, ctx).map(StmtOutcome::Ok),
                ProceduralStatement::TryCatch(stmt) => self.execute_try_catch(stmt, ctx),
                ProceduralStatement::DeclareCursor(stmt) => {
                    ctx.cursors.insert(
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
                ProceduralStatement::DropProcedure(stmt) => {
                    let schema = stmt.name.schema_or_dbo().to_string();
                    self.catalog.drop_routine(&schema, &stmt.name.name, false)?;
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
                ProceduralStatement::DropFunction(stmt) => {
                    let schema = stmt.name.schema_or_dbo().to_string();
                    self.catalog.drop_routine(&schema, &stmt.name.name, true)?;
                    Ok(StmtOutcome::Ok(None))
                }
                ProceduralStatement::CreateView(stmt) => {
                    SchemaExecutor {
                        catalog: self.catalog,
                        storage: self.storage,
                    }
                    .create_view(stmt)?;
                    Ok(StmtOutcome::Ok(None))
                }
                ProceduralStatement::DropView(stmt) => {
                    SchemaExecutor {
                        catalog: self.catalog,
                        storage: self.storage,
                    }
                    .drop_view(stmt)?;
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
                ProceduralStatement::DropTrigger(stmt) => {
                    let schema = stmt.name.schema_or_dbo().to_string();
                    self.catalog.drop_trigger(&schema, &stmt.name.name)?;
                    Ok(StmtOutcome::Ok(None))
                }
            },
            Statement::Cursor(cursor) => match cursor {
                CursorStatement::OpenCursor(name) => self.execute_open_cursor(name, ctx).map(StmtOutcome::Ok),
                CursorStatement::FetchCursor(stmt) => self.execute_fetch_cursor(stmt, ctx).map(StmtOutcome::Ok),
                CursorStatement::CloseCursor(name) => self.execute_close_cursor(name, ctx).map(StmtOutcome::Ok),
                CursorStatement::DeallocateCursor(name) => self.execute_deallocate_cursor(name, ctx).map(StmtOutcome::Ok),
            },
            Statement::Session(session) => match session {
                SessionStatement::SetIdentityInsert(_) => Err(DbError::Execution(
                    "SET IDENTITY_INSERT is handled at engine level".into(),
                )),
                SessionStatement::SetTransactionIsolationLevel(_)
                | SessionStatement::SetOption(_) => Err(DbError::Execution(
                    "session option statements are handled at engine level".into(),
                )),
            },
            Statement::WithCte(stmt) => self.execute_with_cte(stmt, ctx).map(StmtOutcome::Ok),
        }
    }

    pub fn execute_batch(
        &mut self,
        stmts: &[Statement],
        ctx: &mut ExecutionContext,
    ) -> StmtResult<Option<QueryResult>> {
        let mut last_result = StmtOutcome::Ok(None);
        for stmt in stmts {
            match self.execute(stmt.clone(), ctx) {
                Ok(r) => {
                    if r.is_control_flow() {
                        return Ok(r);
                    }
                    last_result = r;
                }
                Err(e) => return Err(e),
            };
        }
        Ok(last_result)
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

    pub(crate) fn push_dirty_insert(
        &self,
        ctx: &mut ExecutionContext,
        table_name: &str,
        row: &crate::storage::StoredRow,
    ) {
        if let Some(db) = &ctx.dirty_buffer {
            db.lock().push_op(
                ctx.session_id,
                table_name.to_string(),
                super::dirty_buffer::DirtyOp::Insert { row: row.clone() },
            );
        }
    }



}

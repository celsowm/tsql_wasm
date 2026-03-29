use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::ast::{DropTableStmt, IsolationLevel, ObjectName, Statement};
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::parser::{parse_batch_with_quoted_ident, parse_sql};
use crate::storage::Storage;

use super::super::clock::Clock;
use super::super::context::ExecutionContext;
use super::super::journal::{Journal, JournalEvent};
use super::super::locks::{SessionId, TxWorkspace};
use super::super::result::QueryResult;
use super::super::schema::SchemaExecutor;
use super::super::script::ScriptExecutor;
use super::super::session::{SessionRuntime, SharedState};
use super::super::table_util::{collect_write_tables, is_transaction_statement};
use super::super::tooling::{
    analyze_sql_batch, apply_set_option,
    collect_read_tables as collect_read_tables_tooling,
    collect_write_tables as collect_write_tables_tooling, explain_statement,
    split_sql_statements, statement_compat_warnings, CompatibilityReport, ExecutionTrace,
    ExplainPlan, SessionOptions, TraceStatementEvent,
};
use super::super::dirty_buffer;
use super::super::transaction::TransactionManager;
use super::super::transaction_exec;
use super::persistence::DatabaseInner;
use super::{RandomSeed, SqlAnalyzer, StatementExecutor};

impl<C, S> StatementExecutor for DatabaseInner<C, S>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static + Default,
    S: Storage + Serialize + DeserializeOwned + Clone + 'static + Default,
{
    fn execute_session(
        &self,
        session_id: SessionId,
        stmt: Statement,
    ) -> Result<Option<QueryResult>, DbError> {
        let mut guard = self.inner.lock().expect("database mutex poisoned");
        guard.with_session_mut(session_id, |state, session| {
            execute_single_statement(state, session_id, session, stmt)
        })
    }

    fn execute_session_batch(
        &self,
        session_id: SessionId,
        stmts: Vec<Statement>,
    ) -> Result<Option<QueryResult>, DbError> {
        let mut guard = self.inner.lock().expect("database mutex poisoned");
        guard.with_session_mut(session_id, |state, session| {
            execute_batch_statements(state, session_id, session, stmts)
        })
    }

    fn execute_session_batch_sql(
        &self,
        session_id: SessionId,
        sql: &str,
    ) -> Result<Option<QueryResult>, DbError> {
        let mut guard = self.inner.lock().expect("database mutex poisoned");
        let quoted_ident = guard.with_session_mut(session_id, |_state, session| {
            Ok(session.options.quoted_identifier)
        })?;
        drop(guard);
        
        let stmts = parse_batch_with_quoted_ident(sql, quoted_ident)?;
        self.execute_session_batch(session_id, stmts)
    }

    fn execute_session_batch_sql_multi(
        &self,
        session_id: SessionId,
        sql: &str) -> Result<Vec<Option<QueryResult>>, DbError> {
        let mut guard = self.inner.lock().expect("database mutex poisoned");
        let quoted_ident = guard.with_session_mut(session_id, |_state, session| {
            Ok(session.options.quoted_identifier)
        })?;
        drop(guard);
        
        let stmts = parse_batch_with_quoted_ident(sql, quoted_ident)?;
        let mut guard = self.inner.lock().expect("database mutex poisoned");
        guard.with_session_mut(session_id, |state, session| {
            execute_batch_statements_multi(state, session_id, session, stmts)
        })
    }
}

impl<C, S> SqlAnalyzer for DatabaseInner<C, S>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static + Default,
    S: Storage + Serialize + DeserializeOwned + Clone + 'static + Default,
{
    fn session_isolation_level(&self, session_id: SessionId) -> Result<IsolationLevel, DbError> {
        let guard = self.inner.lock().expect("database mutex poisoned");
        let session = guard
            .sessions
            .get(&session_id)
            .ok_or_else(|| DbError::Execution(format!("session {} not found", session_id)))?;
        Ok(session.tx_manager.session_isolation_level)
    }

    fn transaction_is_active(&self, session_id: SessionId) -> Result<bool, DbError> {
        let guard = self.inner.lock().expect("database mutex poisoned");
        let session = guard
            .sessions
            .get(&session_id)
            .ok_or_else(|| DbError::Execution(format!("session {} not found", session_id)))?;
        Ok(session.tx_manager.active.is_some())
    }

    fn session_options(&self, session_id: SessionId) -> Result<SessionOptions, DbError> {
        let guard = self.inner.lock().expect("database mutex poisoned");
        let session = guard
            .sessions
            .get(&session_id)
            .ok_or_else(|| DbError::Execution(format!("session {} not found", session_id)))?;
        Ok(session.options.clone())
    }

    fn analyze_sql_batch(&self, sql: &str) -> CompatibilityReport {
        analyze_sql_batch(sql)
    }

    fn explain_sql(&self, sql: &str) -> Result<ExplainPlan, DbError> {
        let stmt = parse_sql(sql)?;
        Ok(explain_statement(&stmt))
    }

    fn trace_execute_session_sql(
        &self,
        session_id: SessionId,
        sql: &str,
    ) -> Result<ExecutionTrace, DbError> {
        let slices = split_sql_statements(sql);
        let mut events = Vec::with_capacity(slices.len());
        let mut stopped_on_error = false;

        for slice in slices {
            match parse_sql(&slice.sql) {
                Ok(stmt) => {
                    let mut warnings = statement_compat_warnings(&stmt);
                    let mut read_tables: Vec<String> =
                        collect_read_tables_tooling(&stmt).into_iter().collect();
                    let mut write_tables: Vec<String> =
                        collect_write_tables_tooling(&stmt).into_iter().collect();
                    read_tables.sort();
                    write_tables.sort();

                    match self.execute_session(session_id, stmt) {
                        Ok(result) => {
                            let options = self.session_options(session_id)?;
                            let row_count = if options.nocount {
                                None
                            } else {
                                result.as_ref().map(|r| r.rows.len())
                            };
                            events.push(TraceStatementEvent {
                                index: slice.index,
                                sql: slice.sql,
                                normalized_sql: slice.normalized_sql,
                                span: slice.span,
                                status: "ok".to_string(),
                                warnings: std::mem::take(&mut warnings),
                                error: None,
                                row_count,
                                read_tables,
                                write_tables,
                            });
                        }
                        Err(err) => {
                            events.push(TraceStatementEvent {
                                index: slice.index,
                                sql: slice.sql,
                                normalized_sql: slice.normalized_sql,
                                span: slice.span,
                                status: "error".to_string(),
                                warnings,
                                error: Some(err.to_string()),
                                row_count: None,
                                read_tables,
                                write_tables,
                            });
                            stopped_on_error = true;
                            break;
                        }
                    }
                }
                Err(err) => {
                    let err_str: String = err.to_string();
                    events.push(TraceStatementEvent {
                        index: slice.index,
                        sql: slice.sql,
                        normalized_sql: slice.normalized_sql,
                        span: slice.span,
                        status: "unsupported".to_string(),
                        warnings: Vec::new(),
                        error: Some(err_str),
                        row_count: None,
                        read_tables: Vec::new(),
                        write_tables: Vec::new(),
                    });
                    stopped_on_error = true;
                    break;
                }
            }
        }
        Ok(ExecutionTrace {
            events,
            stopped_on_error,
        })
    }
}

impl<C, S> RandomSeed for DatabaseInner<C, S>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static + Default,
    S: Storage + Serialize + DeserializeOwned + Clone + 'static + Default,
{
    fn set_session_seed(&self, session_id: SessionId, seed: u64) -> Result<(), DbError> {
        let mut guard = self.inner.lock().expect("database mutex poisoned");
        guard.with_session_mut(session_id, |_, session| {
            session.random_state = seed;
            Ok(())
        })
    }
}

pub(crate) fn execute_batch_statements<C, S>(
    state: &mut SharedState<C, S>,
    session_id: SessionId,
    session: &mut SessionRuntime<C, S>,
    stmts: Vec<Statement>,
) -> Result<Option<QueryResult>, DbError>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static + Default,
    S: Storage + Serialize + DeserializeOwned + Clone + 'static + Default,
{
    let mut out = Ok(None);
    let mut ctx = ExecutionContext::new(
        &mut session.variables,
        &mut session.session_last_identity,
        &mut session.scope_identity_stack,
        &mut session.temp_table_map,
        &mut session.table_var_map,
        &mut session.table_var_counter,
        session.options.ansi_nulls,
        session.options.datefirst,
        &mut session.random_state,
        &mut session.cursors,
        &mut session.fetch_status,
        &mut session.print_output,
        if session.tx_manager.active.is_some() { Some(state.dirty_buffer.clone()) } else { None },
        session_id,
    );
    ctx.enter_scope();

    for stmt in stmts {
        ctx.trancount = session.tx_manager.depth;
        ctx.identity_insert = session.options.identity_insert.clone();
        if is_transaction_statement(&stmt) {
            match transaction_exec::execute_transaction_statement(
                state,
                session_id,
                &mut session.tx_manager,
                &mut session.journal,
                &mut session.workspace,
                stmt,
            ) {
                Ok(r) => out = Ok(r),
                Err(e) => {
                    out = Err(e);
                    break;
                }
            }
        } else {
            match execute_non_transaction_statement(
                state,
                session_id,
                &mut session.tx_manager,
                session.journal.as_mut(),
                &mut session.workspace,
                session.clock.as_ref(),
                &mut session.options,
                stmt,
                &mut ctx,
            ) {
                Ok(r) => out = Ok(r),
                Err(DbError::Return(_)) => {
                    out = Ok(None);
                    break;
                }
                Err(e) => {
                    out = Err(e);
                    break;
                }
            }
        }
    }

    if session.tx_manager.active.is_some() {
        if let Some(workspace) = session.workspace.as_mut() {
            cleanup_scope_table_vars(&mut workspace.catalog, &mut workspace.storage, &mut ctx)?;
        } else {
            let _ = ctx.leave_scope_collect_table_vars();
        }
    } else {
        cleanup_scope_table_vars(&mut state.catalog, &mut state.storage, &mut ctx)?;
    }
    out
}

pub(crate) fn execute_batch_statements_multi<C, S>(
    state: &mut SharedState<C, S>,
    session_id: SessionId,
    session: &mut SessionRuntime<C, S>,
    stmts: Vec<Statement>,
) -> Result<Vec<Option<QueryResult>>, DbError>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static + Default,
    S: Storage + Serialize + DeserializeOwned + Clone + 'static + Default,
{
    let mut results: Vec<Option<QueryResult>> = Vec::new();
    let mut ctx = ExecutionContext::new(
        &mut session.variables,
        &mut session.session_last_identity,
        &mut session.scope_identity_stack,
        &mut session.temp_table_map,
        &mut session.table_var_map,
        &mut session.table_var_counter,
        session.options.ansi_nulls,
        session.options.datefirst,
        &mut session.random_state,
        &mut session.cursors,
        &mut session.fetch_status,
        &mut session.print_output,
        if session.tx_manager.active.is_some() { Some(state.dirty_buffer.clone()) } else { None },
        session_id,
    );
    ctx.enter_scope();

    for stmt in stmts {
        ctx.trancount = session.tx_manager.depth;
        ctx.identity_insert = session.options.identity_insert.clone();
        if is_transaction_statement(&stmt) {
            match transaction_exec::execute_transaction_statement(
                state,
                session_id,
                &mut session.tx_manager,
                &mut session.journal,
                &mut session.workspace,
                stmt,
            ) {
                Ok(r) => results.push(r),
                Err(e) => {
                    let _ = cleanup_scope_table_vars(&mut state.catalog, &mut state.storage, &mut ctx);
                    return Err(e);
                }
            }
        } else {
            match execute_non_transaction_statement(
                state,
                session_id,
                &mut session.tx_manager,
                session.journal.as_mut(),
                &mut session.workspace,
                session.clock.as_ref(),
                &mut session.options,
                stmt,
                &mut ctx,
            ) {
                Ok(r) => results.push(r),
                Err(DbError::Return(_)) => {
                    results.push(None);
                    break;
                }
                Err(e) => {
                    let _ = cleanup_scope_table_vars(&mut state.catalog, &mut state.storage, &mut ctx);
                    return Err(e);
                }
            }
        }
    }

    if session.tx_manager.active.is_some() {
        if let Some(workspace) = session.workspace.as_mut() {
            cleanup_scope_table_vars(&mut workspace.catalog, &mut workspace.storage, &mut ctx)?;
        } else {
            let _ = ctx.leave_scope_collect_table_vars();
        }
    } else {
        cleanup_scope_table_vars(&mut state.catalog, &mut state.storage, &mut ctx)?;
    }

    Ok(results)
}

pub(crate) fn execute_single_statement<C, S>(
    state: &mut SharedState<C, S>,
    session_id: SessionId,
    session: &mut SessionRuntime<C, S>,
    stmt: Statement,
) -> Result<Option<QueryResult>, DbError>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static + Default,
    S: Storage + Serialize + DeserializeOwned + Clone + 'static + Default,
{
    if is_transaction_statement(&stmt) {
        return transaction_exec::execute_transaction_statement(
            state,
            session_id,
            &mut session.tx_manager,
            &mut session.journal,
            &mut session.workspace,
            stmt,
        );
    }

    let mut ctx = ExecutionContext::new(
        &mut session.variables,
        &mut session.session_last_identity,
        &mut session.scope_identity_stack,
        &mut session.temp_table_map,
        &mut session.table_var_map,
        &mut session.table_var_counter,
        session.options.ansi_nulls,
        session.options.datefirst,
        &mut session.random_state,
        &mut session.cursors,
        &mut session.fetch_status,
        &mut session.print_output,
        if session.tx_manager.active.is_some() { Some(state.dirty_buffer.clone()) } else { None },
        session_id,
    );
    ctx.trancount = session.tx_manager.depth;
    ctx.identity_insert = session.options.identity_insert.clone();

    match execute_non_transaction_statement(
        state,
        session_id,
        &mut session.tx_manager,
        session.journal.as_mut(),
        &mut session.workspace,
        session.clock.as_ref(),
        &mut session.options,
        stmt,
        &mut ctx,
    ) {
        Err(DbError::Return(_)) => Ok(None),
        other => other,
    }
}

pub(crate) fn execute_non_transaction_statement<C, S>(
    state: &mut SharedState<C, S>,
    session_id: SessionId,
    tx_manager: &mut TransactionManager<C, S>,
    journal: &mut dyn Journal,
    workspace_slot: &mut Option<TxWorkspace<C, S>>,
    clock: &dyn Clock,
    session_options: &mut SessionOptions,
    stmt: Statement,
    ctx: &mut ExecutionContext,
) -> Result<Option<QueryResult>, DbError>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static + Default,
    S: Storage + Serialize + DeserializeOwned + Clone + 'static + Default,
{
    if let Statement::SetOption(opt) = &stmt {
        let apply = apply_set_option(opt, session_options)?;
        ctx.ansi_nulls = session_options.ansi_nulls;
        ctx.datefirst = session_options.datefirst;
        for warn in apply.warnings {
            journal.record(JournalEvent::Info { message: warn });
        }
        return Ok(None);
    }

    if let Statement::SetIdentityInsert(ref id_stmt) = stmt {
        let table_name = id_stmt.table.name.to_uppercase();
        if id_stmt.on {
            session_options.identity_insert.insert(table_name);
        } else {
            session_options.identity_insert.remove(&table_name);
        }
        return Ok(None);
    }

    let isolation_level = tx_manager
        .active
        .as_ref()
        .map(|tx| tx.isolation_level)
        .unwrap_or(tx_manager.session_isolation_level);
    let is_select = matches!(stmt, Statement::Select(_));
    let read_uncommitted_dirty =
        isolation_level == IsolationLevel::ReadUncommitted && is_select;

    if tx_manager.active.is_some() {
        let read_committed_from_shared =
            isolation_level == IsolationLevel::ReadCommitted && is_select;

        state.table_locks.acquire_statement_locks(
            session_id,
            tx_manager,
            workspace_slot,
            &stmt,
        )?;

        let out = if read_uncommitted_dirty {
            // READ UNCOMMITTED: build a merged view with dirty writes from all sessions
            let (mut dirty_catalog, mut dirty_storage) =
                dirty_buffer::build_dirty_read_storage(state, session_id, workspace_slot);
            let mut script = ScriptExecutor {
                catalog: &mut dirty_catalog,
                storage: &mut dirty_storage,
                clock,
            };
            script.execute(stmt.clone(), ctx)
        } else if read_committed_from_shared {
            let mut script = ScriptExecutor {
                catalog: &mut state.catalog,
                storage: &mut state.storage,
                clock,
            };
            script.execute(stmt.clone(), ctx)
        } else {
            let workspace = workspace_slot.as_mut().ok_or_else(|| {
                DbError::Execution("internal error: missing transaction workspace".into())
            })?;
            let mut script = ScriptExecutor {
                catalog: &mut workspace.catalog,
                storage: &mut workspace.storage,
                clock,
            };
            script.execute(stmt.clone(), ctx)
        };
        if out.is_ok() {
            transaction_exec::register_read_tables(workspace_slot, &stmt);
            transaction_exec::register_workspace_write_tables(workspace_slot, &stmt);
            transaction_exec::register_write_intent(tx_manager, journal, &stmt);
        } else if session_options.xact_abort
            && !matches!(out, Err(DbError::Break | DbError::Continue | DbError::Return(_)))
        {
            transaction_exec::force_xact_abort(
                state,
                session_id,
                tx_manager,
                journal,
                workspace_slot,
            );
        }
        out
    } else {
        if read_uncommitted_dirty {
            let (mut dirty_catalog, mut dirty_storage) =
                dirty_buffer::build_dirty_read_storage(state, session_id, workspace_slot);
            let mut script = ScriptExecutor {
                catalog: &mut dirty_catalog,
                storage: &mut dirty_storage,
                clock,
            };
            return script.execute(stmt, ctx);
        }

        let written_tables = collect_write_tables(&stmt);
        if written_tables.is_empty() {
            let mut script = ScriptExecutor {
                catalog: &mut state.catalog,
                storage: &mut state.storage,
                clock,
            };
            return script.execute(stmt, ctx);
        }
        let before_catalog = state.catalog.clone();
        let before_storage = state.storage.clone();
        let before_versions = state.table_versions.clone();
        let before_commit_ts = state.commit_ts;
        let mut script = ScriptExecutor {
            catalog: &mut state.catalog,
            storage: &mut state.storage,
            clock,
        };
        let out = script.execute(stmt, ctx);
        if out.is_ok() {
            state.commit_ts += 1;
            for table in &written_tables {
                state.table_versions.insert(table.clone(), state.commit_ts);
            }
            let checkpoint = state.to_checkpoint();
            if let Err(e) = state.durability.persist_checkpoint(&checkpoint) {
                state.catalog = before_catalog;
                state.storage = before_storage;
                state.table_versions = before_versions;
                state.commit_ts = before_commit_ts;
                return Err(e);
            }
        }
        out
    }
}

pub(crate) fn cleanup_scope_table_vars(
    catalog: &mut dyn Catalog,
    storage: &mut dyn Storage,
    ctx: &mut ExecutionContext,
) -> Result<(), DbError> {
    let dropped_physical = ctx.leave_scope_collect_table_vars();
    for physical in dropped_physical {
        if catalog.find_table("dbo", &physical).is_none() {
            continue;
        }
        let mut schema = SchemaExecutor { catalog, storage };
        schema.drop_table(DropTableStmt {
            name: ObjectName {
                schema: Some("dbo".to_string()),
                name: physical,
            },
        })?;
    }
    Ok(())
}

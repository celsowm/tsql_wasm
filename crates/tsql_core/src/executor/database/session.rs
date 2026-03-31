use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::ast::{DropTableStmt, IsolationLevel, ObjectName, Statement};
use crate::catalog::Catalog;
use crate::error::{DbError, StmtOutcome, StmtResult};
use crate::parser::{parse_batch_with_quoted_ident, parse_sql};
use crate::storage::Storage;

use super::super::clock::Clock;
use super::super::context::ExecutionContext;
use super::super::dirty_buffer;
use super::super::journal::{Journal, JournalEvent};
use super::super::locks::{LockTable, SessionId, TxWorkspace};
use super::super::result::QueryResult;
use super::super::schema::SchemaExecutor;
use super::super::script::ScriptExecutor;
use super::super::session::{SessionRuntime, SessionSnapshot, SharedState};
use super::super::table_util::{collect_write_tables, is_transaction_statement};
use super::super::tooling::{
    analyze_sql_batch, apply_set_option, collect_read_tables as collect_read_tables_tooling,
    collect_write_tables as collect_write_tables_tooling, explain_statement, split_sql_statements,
    statement_compat_warnings, CompatibilityReport, ExecutionTrace, ExplainPlan, SessionOptions,
    TraceStatementEvent,
};
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
        let session_mutex = self.inner.sessions.get(&session_id)
            .ok_or_else(|| DbError::Execution(format!("session {} not found", session_id)))?;
        let mut session = session_mutex.lock();
        execute_single_statement(&self.inner, session_id, &mut session, stmt)
    }

    fn execute_session_batch(
        &self,
        session_id: SessionId,
        stmts: Vec<Statement>,
    ) -> Result<Option<QueryResult>, DbError> {
        let session_mutex = self.inner.sessions.get(&session_id)
            .ok_or_else(|| DbError::Execution(format!("session {} not found", session_id)))?;
        let mut session = session_mutex.lock();
        execute_batch_statements(&self.inner, session_id, &mut session, stmts)
    }

    fn execute_session_batch_sql(
        &self,
        session_id: SessionId,
        sql: &str,
    ) -> Result<Option<QueryResult>, DbError> {
        let quoted_ident = {
            let session_mutex = self.inner.sessions.get(&session_id)
                .ok_or_else(|| DbError::Execution(format!("session {} not found", session_id)))?;
            let session = session_mutex.lock();
            session.options.quoted_identifier
        };

        let stmts = parse_batch_with_quoted_ident(sql, quoted_ident)?;
        self.execute_session_batch(session_id, stmts)
    }

    fn execute_session_batch_sql_multi(
        &self,
        session_id: SessionId,
        sql: &str,
    ) -> Result<Vec<Option<QueryResult>>, DbError> {
        let quoted_ident = {
            let session_mutex = self.inner.sessions.get(&session_id)
                .ok_or_else(|| DbError::Execution(format!("session {} not found", session_id)))?;
            let session = session_mutex.lock();
            session.options.quoted_identifier
        };

        let stmts = parse_batch_with_quoted_ident(sql, quoted_ident)?;
        let session_mutex = self.inner.sessions.get(&session_id)
            .ok_or_else(|| DbError::Execution(format!("session {} not found", session_id)))?;
        let mut session = session_mutex.lock();
        execute_batch_statements_multi(&self.inner, session_id, &mut session, stmts)
    }
}

impl<C, S> SqlAnalyzer for DatabaseInner<C, S>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static + Default,
    S: Storage + Serialize + DeserializeOwned + Clone + 'static + Default,
{
    fn session_isolation_level(&self, session_id: SessionId) -> Result<IsolationLevel, DbError> {
        let session_mutex = self.inner.sessions.get(&session_id)
            .ok_or_else(|| DbError::Execution(format!("session {} not found", session_id)))?;
        let session = session_mutex.lock();
        Ok(session.tx_manager.session_isolation_level)
    }

    fn transaction_is_active(&self, session_id: SessionId) -> Result<bool, DbError> {
        let session_mutex = self.inner.sessions.get(&session_id)
            .ok_or_else(|| DbError::Execution(format!("session {} not found", session_id)))?;
        let session = session_mutex.lock();
        Ok(session.tx_manager.active.is_some())
    }

    fn session_options(&self, session_id: SessionId) -> Result<SessionOptions, DbError> {
        let session_mutex = self.inner.sessions.get(&session_id)
            .ok_or_else(|| DbError::Execution(format!("session {} not found", session_id)))?;
        let session = session_mutex.lock();
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
        let session_mutex = self.inner.sessions.get(&session_id)
            .ok_or_else(|| DbError::Execution(format!("session {} not found", session_id)))?;
        let mut session = session_mutex.lock();
        session.random_state = seed;
        Ok(())
    }
}

pub(crate) fn execute_batch_statements<C, S>(
    state: &SharedState<C, S>,
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
        &mut session.identities.last_identity,
        &mut session.identities.scope_stack,
        &mut session.tables.temp_map,
        &mut session.tables.var_map,
        &mut session.tables.var_counter,
        session.options.ansi_nulls,
        session.options.datefirst,
        &mut session.random_state,
        &mut session.cursors.map,
        &mut session.cursors.fetch_status,
        &mut session.diagnostics.print_output,
        if session.tx_manager.active.is_some() {
            Some(state.dirty_buffer.clone())
        } else {
            None
        },
        session_id,
    );
    ctx.enter_scope();

    for stmt in stmts {
        ctx.trancount = session.tx_manager.depth;
        ctx.xact_state = session.tx_manager.xact_state;
        ctx.identity_insert = session.options.identity_insert.clone();
        if is_transaction_statement(&stmt) {
            match transaction_exec::execute_transaction_statement(
                state,
                session_id,
                &mut session.tx_manager,
                &mut session.journal,
                &mut session.workspace,
                &mut ctx,
                &mut session.options,
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
                Ok(r) => {
                    match r {
                        StmtOutcome::Return(_) => {
                            out = Ok(None);
                            break;
                        }
                        StmtOutcome::Break | StmtOutcome::Continue => {
                            // Control flow should propagate as-is through batch
                            // (shouldn't normally reach here outside loops)
                            out = r.into_result();
                            break;
                        }
                        StmtOutcome::Ok(v) => out = Ok(v),
                    }
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
        let mut storage_guard = state.storage.write();
        let (cat, stor) = storage_guard.get_mut_refs();
        cleanup_scope_table_vars(
            cat,
            stor,
            &mut ctx,
        )?;
    }
    out
}

pub(crate) fn execute_batch_statements_multi<C, S>(
    state: &SharedState<C, S>,
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
        &mut session.identities.last_identity,
        &mut session.identities.scope_stack,
        &mut session.tables.temp_map,
        &mut session.tables.var_map,
        &mut session.tables.var_counter,
        session.options.ansi_nulls,
        session.options.datefirst,
        &mut session.random_state,
        &mut session.cursors.map,
        &mut session.cursors.fetch_status,
        &mut session.diagnostics.print_output,
        if session.tx_manager.active.is_some() {
            Some(state.dirty_buffer.clone())
        } else {
            None
        },
        session_id,
    );
    ctx.enter_scope();

    for stmt in stmts {
        ctx.trancount = session.tx_manager.depth;
        ctx.xact_state = session.tx_manager.xact_state;
        ctx.identity_insert = session.options.identity_insert.clone();
        if is_transaction_statement(&stmt) {
            match transaction_exec::execute_transaction_statement(
                state,
                session_id,
                &mut session.tx_manager,
                &mut session.journal,
                &mut session.workspace,
                &mut ctx,
                &mut session.options,
                stmt,
            ) {
                Ok(r) => results.push(r),
                Err(e) => {
                    let mut storage_guard = state.storage.write();
                    let (cat, stor) = storage_guard.get_mut_refs();
                    let _ = cleanup_scope_table_vars(
                        cat,
                        stor,
                        &mut ctx,
                    );
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
                Ok(r) => {
                    match r {
                        StmtOutcome::Return(_) => {
                            results.push(None);
                            break;
                        }
                        StmtOutcome::Break | StmtOutcome::Continue => {
                            // BREAK/CONTINUE outside loops is an error
                            return Err(DbError::Execution(
                                if matches!(r, StmtOutcome::Break) {
                                    "BREAK outside of WHILE".into()
                                } else {
                                    "CONTINUE outside of WHILE".into()
                                }
                            ));
                        }
                        StmtOutcome::Ok(v) => results.push(v),
                    }
                }
                Err(e) => {
                    let mut storage_guard = state.storage.write();
                    let (cat, stor) = storage_guard.get_mut_refs();
                    let _ = cleanup_scope_table_vars(
                        cat,
                        stor,
                        &mut ctx,
                    );
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
        let mut storage_guard = state.storage.write();
        let (cat, stor) = storage_guard.get_mut_refs();
        cleanup_scope_table_vars(
            cat,
            stor,
            &mut ctx,
        )?;
    }

    Ok(results)
}

pub(crate) fn execute_single_statement<C, S>(
    state: &SharedState<C, S>,
    session_id: SessionId,
    session: &mut SessionRuntime<C, S>,
    stmt: Statement,
) -> Result<Option<QueryResult>, DbError>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static + Default,
    S: Storage + Serialize + DeserializeOwned + Clone + 'static + Default,
{
    let mut ctx = ExecutionContext::new(
        &mut session.variables,
        &mut session.identities.last_identity,
        &mut session.identities.scope_stack,
        &mut session.tables.temp_map,
        &mut session.tables.var_map,
        &mut session.tables.var_counter,
        session.options.ansi_nulls,
        session.options.datefirst,
        &mut session.random_state,
        &mut session.cursors.map,
        &mut session.cursors.fetch_status,
        &mut session.diagnostics.print_output,
        if session.tx_manager.active.is_some() {
            Some(state.dirty_buffer.clone())
        } else {
            None
        },
        session_id,
    );
    ctx.trancount = session.tx_manager.depth;
    ctx.xact_state = session.tx_manager.xact_state;
    ctx.identity_insert = session.options.identity_insert.clone();

    if is_transaction_statement(&stmt) {
        return transaction_exec::execute_transaction_statement(
            state,
            session_id,
            &mut session.tx_manager,
            &mut session.journal,
            &mut session.workspace,
            &mut ctx,
            &mut session.options,
            stmt,
        );
    }

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
        // Swallow RETURN at the top level; BREAK/CONTINUE outside loops are errors
        Ok(outcome) => outcome.into_result_swallow_return(),
        Err(e) => Err(e),
    }
}

pub(crate) fn execute_non_transaction_statement<C, S>(
    state: &SharedState<C, S>,
    session_id: SessionId,
    tx_manager: &mut TransactionManager<C, S, SessionSnapshot>,
    journal: &mut dyn Journal,
    workspace_slot: &mut Option<TxWorkspace<C, S>>,
    clock: &dyn Clock,
    session_options: &mut SessionOptions,
    stmt: Statement,
    ctx: &mut ExecutionContext,
) -> StmtResult<Option<QueryResult>>
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
        return Ok(StmtOutcome::Ok(None));
    }

    if let Statement::SetIdentityInsert(ref id_stmt) = stmt {
        let table_name = id_stmt.table.name.to_uppercase();
        if id_stmt.on {
            session_options.identity_insert.insert(table_name);
        } else {
            session_options.identity_insert.remove(&table_name);
        }
        return Ok(StmtOutcome::Ok(None));
    }

    let isolation_level = tx_manager
        .active
        .as_ref()
        .map(|tx| tx.isolation_level)
        .unwrap_or(tx_manager.session_isolation_level);
    let is_select = matches!(stmt, Statement::Select(_));
    let read_uncommitted_dirty = isolation_level == IsolationLevel::ReadUncommitted && is_select;
    let read_committed_select = isolation_level == IsolationLevel::ReadCommitted && is_select;

    if tx_manager.active.is_some() {
        LockTable::acquire_statement_locks(
            &state.table_locks,
            session_id,
            tx_manager,
            workspace_slot,
            &stmt,
            session_options.lock_timeout_ms,
        )?;

        let out = if read_uncommitted_dirty {
            let (mut dirty_catalog, mut dirty_storage) =
                dirty_buffer::build_dirty_read_storage(state, session_id, workspace_slot);
            let mut script = ScriptExecutor {
                catalog: &mut dirty_catalog,
                storage: &mut dirty_storage,
                clock,
            };
            script.execute(stmt.clone(), ctx)
        } else if read_committed_select {
            let workspace = workspace_slot.as_mut().ok_or_else(|| {
                DbError::Execution("internal error: missing transaction workspace".into())
            })?;

            {
                let storage_guard = state.storage.read();
                for table_def in storage_guard.catalog.get_tables() {
                    let tname = table_def.name.to_uppercase();
                    if workspace.write_tables.contains(&tname) {
                        continue;
                    }
                    let tid = table_def.id;
                    if let Ok(committed_rows) = storage_guard.storage.get_rows(tid) {
                        let _ = workspace.storage.update_rows(tid, committed_rows);
                    }
                }
                for table_def in storage_guard.catalog.get_tables() {
                    let tname = table_def.name.to_uppercase();
                    if workspace.write_tables.contains(&tname) {
                        continue;
                    }
                    if workspace.catalog.find_table(table_def.schema_or_dbo(), &table_def.name).is_none() {
                        workspace.catalog.get_tables_mut().push(table_def.clone());
                    }
                }
            }

            let mut script = ScriptExecutor {
                catalog: &mut workspace.catalog,
                storage: &mut workspace.storage,
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
        // Control flow signals (Break/Continue/Return) should not affect transaction state
        let is_control_flow = out.as_ref().map_or(false, |o| o.is_control_flow());
        if out.is_ok() && !is_control_flow {
            transaction_exec::register_read_tables(workspace_slot, &stmt);
            transaction_exec::register_workspace_write_tables(workspace_slot, &stmt);
            transaction_exec::register_write_intent(tx_manager, journal, &stmt);
            if tx_manager.xact_state != -1 {
                tx_manager.xact_state = 1;
            }
        } else if out.is_err() && session_options.xact_abort {
            transaction_exec::force_xact_abort(
                state,
                session_id,
                tx_manager,
                journal,
                workspace_slot,
                ctx,
                session_options,
            );
        } else if out.is_err() {
            tx_manager.xact_state = -1;
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
            LockTable::acquire_statement_locks(
                &state.table_locks,
                session_id,
                tx_manager,
                workspace_slot,
                &stmt,
                session_options.lock_timeout_ms,
            )?;

            let mut storage_guard = state.storage.write();
            let (cat, stor) = storage_guard.get_mut_refs();
            let mut script = ScriptExecutor {
                catalog: cat,
                storage: stor,
                clock,
            };
            let out = script.execute(stmt, ctx);
            state.table_locks.lock().release_all_for_session(session_id);
            return out;
        }

        LockTable::acquire_statement_locks(
            &state.table_locks,
            session_id,
            tx_manager,
            workspace_slot,
            &stmt,
            session_options.lock_timeout_ms,
        )?;

        let mut storage_guard = state.storage.write();
        let before_catalog = storage_guard.catalog.clone();
        let before_storage = storage_guard.storage.clone();
        let before_versions = storage_guard.table_versions.clone();
        let before_commit_ts = storage_guard.commit_ts;
        let (cat, stor) = storage_guard.get_mut_refs();
        let mut script = ScriptExecutor {
            catalog: cat,
            storage: stor,
            clock,
        };
        let out = script.execute(stmt, ctx);
        let is_control_flow = out.as_ref().map_or(false, |o| o.is_control_flow());
        if out.is_ok() && !is_control_flow {
            storage_guard.commit_ts += 1;
            for table in &written_tables {
                let ts = storage_guard.commit_ts;
                storage_guard.table_versions.insert(table.clone(), ts);
            }
            let checkpoint = state.to_checkpoint_internal(&storage_guard);
            if let Err(e) = state.durability.lock().persist_checkpoint(&checkpoint) {
                storage_guard.catalog = before_catalog;
                storage_guard.storage = before_storage;
                storage_guard.table_versions = before_versions;
                storage_guard.commit_ts = before_commit_ts;
                state.table_locks.lock().release_all_for_session(session_id);
                return Err(e);
            }
        }
        state.table_locks.lock().release_all_for_session(session_id);
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

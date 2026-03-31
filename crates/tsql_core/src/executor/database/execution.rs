use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::ast::Statement;
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::parser::parse_batch_with_quoted_ident;
use crate::storage::Storage;

use super::super::context::ExecutionContext;
use super::super::locks::SessionId;
use super::super::result::QueryResult;
use super::super::session::{SessionRuntime, SharedState};
use super::super::table_util::is_transaction_statement;
use super::super::transaction_exec;
use super::persistence::DatabaseInner;
use super::StatementExecutor;

use super::dispatch::{execute_non_transaction_statement, cleanup_scope_table_vars};

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
            use crate::error::StmtOutcome;
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
            use crate::error::StmtOutcome;
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

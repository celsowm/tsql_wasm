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
use super::StatementExecutor;

use super::dispatch::execute_non_transaction_statement;

impl<C, S> StatementExecutor for super::StatementExecutorService<C, S>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static + Default,
    S: Storage + crate::storage::CheckpointableStorage + Serialize + DeserializeOwned + Clone + 'static + Default,
{
    fn execute_session(
        &self,
        session_id: SessionId,
        stmt: Statement,
    ) -> Result<Option<QueryResult>, DbError> {
        let session_mutex = self.state.sessions.get(&session_id)
            .ok_or_else(|| DbError::Execution(format!("session {} not found", session_id)))?;
        let mut session = session_mutex.lock();
        execute_single_statement(&self.state, session_id, &mut session, stmt)
    }

    fn execute_session_batch(
        &self,
        session_id: SessionId,
        stmts: Vec<Statement>,
    ) -> Result<Option<QueryResult>, DbError> {
        let session_mutex = self.state.sessions.get(&session_id)
            .ok_or_else(|| DbError::Execution(format!("session {} not found", session_id)))?;
        let mut session = session_mutex.lock();
        execute_batch_statements(&self.state, session_id, &mut session, stmts)
    }

    fn execute_session_batch_sql(
        &self,
        session_id: SessionId,
        sql: &str,
    ) -> Result<Option<QueryResult>, DbError> {
        let quoted_ident = {
            let session_mutex = self.state.sessions.get(&session_id)
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
            let session_mutex = self.state.sessions.get(&session_id)
                .ok_or_else(|| DbError::Execution(format!("session {} not found", session_id)))?;
            let session = session_mutex.lock();
            session.options.quoted_identifier
        };

        let stmts = parse_batch_with_quoted_ident(sql, quoted_ident)?;
        let session_mutex = self.state.sessions.get(&session_id)
            .ok_or_else(|| DbError::Execution(format!("session {} not found", session_id)))?;
        let mut session = session_mutex.lock();
        execute_batch_statements_multi(&self.state, session_id, &mut session, stmts)
    }

    fn set_session_metadata(
        &self,
        session_id: SessionId,
        user: Option<String>,
        app_name: Option<String>,
        host_name: Option<String>,
    ) -> Result<(), DbError> {
        let session_mutex = self.state.sessions.get(&session_id)
            .ok_or_else(|| DbError::Execution(format!("session {} not found", session_id)))?;
        let mut session = session_mutex.lock();
        session.user = user;
        session.app_name = app_name;
        session.host_name = host_name;
        Ok(())
    }
}


fn build_execution_context<'a, C, S>(
    session_id: SessionId,
    session: &'a mut SessionRuntime<C, S>,
    state: &SharedState<C, S>,
) -> ExecutionContext<'a>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static + Default,
    S: Storage + crate::storage::CheckpointableStorage + Serialize + DeserializeOwned + Clone + 'static + Default,
{
    let ctx = ExecutionContext::new(
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
        session.original_database.clone(),
        session.user.clone(),
        session.app_name.clone(),
        session.host_name.clone(),
    );

    ctx
}

fn execute_stmt_loop<C, S, F>(
    state: &SharedState<C, S>,
    session_id: SessionId,
    tx_manager: &mut crate::executor::transaction::TransactionManager<C, S, crate::executor::session::SessionSnapshot>,
    journal: &mut Box<dyn crate::executor::journal::Journal>,
    workspace: &mut Option<crate::executor::locks::TxWorkspace<C, S>>,
    clock: &dyn crate::executor::clock::Clock,
    options: &mut crate::executor::tooling::SessionOptions,
    ctx: &mut ExecutionContext,
    stmts: Vec<Statement>,
    mut on_result: F,
) -> Result<(), DbError>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static + Default,
    S: Storage + crate::storage::CheckpointableStorage + Serialize + DeserializeOwned + Clone + 'static + Default,
    F: FnMut(Option<QueryResult>),
{
    for stmt in stmts {
        ctx.frame.trancount = tx_manager.depth;
        ctx.frame.xact_state = tx_manager.xact_state;
        ctx.identity_insert = options.identity_insert.clone();
        if is_transaction_statement(&stmt) {
            match transaction_exec::execute_transaction_statement(
                state,
                session_id,
                tx_manager,
                journal,
                workspace,
                ctx,
                options,
                stmt,
            ) {
                Ok(r) => on_result(r),
                Err(e) => {
                    if matches!(e, DbError::Deadlock(_)) {
                        transaction_exec::force_xact_abort(
                            state,
                            session_id,
                            tx_manager,
                            journal.as_mut(),
                            workspace,
                            ctx,
                            options,
                        );
                    }
                    return Err(e);
                }
            }
        } else {
            use crate::error::StmtOutcome;
            match execute_non_transaction_statement(
                state,
                session_id,
                tx_manager,
                journal.as_mut(),
                workspace,
                clock,
                options,
                stmt,
                ctx,
            ) {
                Ok(r) => match r {
                    StmtOutcome::Return(_) => {
                        on_result(None);
                        break;
                    }
                    StmtOutcome::Break | StmtOutcome::Continue => {
                        return Err(DbError::Execution(if matches!(r, StmtOutcome::Break) {
                            "BREAK outside of WHILE".into()
                        } else {
                            "CONTINUE outside of WHILE".into()
                        }));
                    }
                    StmtOutcome::Ok(v) => on_result(v),
                },
                Err(e) => {
                    if matches!(e, DbError::Deadlock(_)) {
                        transaction_exec::force_xact_abort(
                            state,
                            session_id,
                            tx_manager,
                            journal.as_mut(),
                            workspace,
                            ctx,
                            options,
                        );
                    }
                    return Err(e);
                }
            }
        }
    }
    Ok(())
}

pub(crate) fn execute_batch_statements<C, S>(
    state: &SharedState<C, S>,
    session_id: SessionId,
    session: &mut SessionRuntime<C, S>,
    stmts: Vec<Statement>,
) -> Result<Option<QueryResult>, DbError>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static + Default,
    S: Storage + crate::storage::CheckpointableStorage + Serialize + DeserializeOwned + Clone + 'static + Default,
{
    let mut last_res = None;
    let session_ptr = session as *mut SessionRuntime<C, S>;
    let mut ctx = unsafe {
        let session_ref = &mut *session_ptr;
        build_execution_context(session_id, session_ref, state)
    };
    ctx.enter_scope();
    let exec_res = unsafe {
        let session_ref = &mut *session_ptr;
        let tx_manager = &mut session_ref.tx_manager;
        let journal = &mut session_ref.journal;
        let workspace = &mut session_ref.workspace;
        let clock = session_ref.clock.as_ref();
        let options = &mut session_ref.options;
        execute_stmt_loop(
            state,
            session_id,
            tx_manager,
            journal,
            workspace,
            clock,
            options,
            &mut ctx,
            stmts,
            |r| {
                last_res = r;
            },
        )
    };
    let dropped_physical = ctx.leave_scope_collect_table_vars();
    drop(ctx);
    unsafe {
        cleanup_scope_tables(state, &mut *session_ptr, dropped_physical);
    }
    exec_res?;
    Ok(last_res)
}

pub(crate) fn execute_batch_statements_multi<C, S>(
    state: &SharedState<C, S>,
    session_id: SessionId,
    session: &mut SessionRuntime<C, S>,
    stmts: Vec<Statement>,
) -> Result<Vec<Option<QueryResult>>, DbError>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static + Default,
    S: Storage + crate::storage::CheckpointableStorage + Serialize + DeserializeOwned + Clone + 'static + Default,
{
    let mut results = Vec::new();
    let session_ptr = session as *mut SessionRuntime<C, S>;
    let mut ctx = unsafe {
        let session_ref = &mut *session_ptr;
        build_execution_context(session_id, session_ref, state)
    };
    ctx.enter_scope();
    let exec_res = unsafe {
        let session_ref = &mut *session_ptr;
        let tx_manager = &mut session_ref.tx_manager;
        let journal = &mut session_ref.journal;
        let workspace = &mut session_ref.workspace;
        let clock = session_ref.clock.as_ref();
        let options = &mut session_ref.options;
        execute_stmt_loop(
            state,
            session_id,
            tx_manager,
            journal,
            workspace,
            clock,
            options,
            &mut ctx,
            stmts,
            |r| {
                results.push(r);
            },
        )
    };
    let dropped_physical = ctx.leave_scope_collect_table_vars();
    drop(ctx);
    unsafe {
        cleanup_scope_tables(state, &mut *session_ptr, dropped_physical);
    }
    exec_res?;
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
    S: Storage + crate::storage::CheckpointableStorage + Serialize + DeserializeOwned + Clone + 'static + Default,
{
    let session_ptr = session as *mut SessionRuntime<C, S>;
    let mut ctx = unsafe {
        let session_ref = &mut *session_ptr;
        build_execution_context(session_id, session_ref, state)
    };
    let mut res = None;
    let exec_res = unsafe {
        let session_ref = &mut *session_ptr;
        let tx_manager = &mut session_ref.tx_manager;
        let journal = &mut session_ref.journal;
        let workspace = &mut session_ref.workspace;
        let clock = session_ref.clock.as_ref();
        let options = &mut session_ref.options;
        execute_stmt_loop(
            state,
            session_id,
            tx_manager,
            journal,
            workspace,
            clock,
            options,
            &mut ctx,
            vec![stmt],
            |r| {
                res = r;
            },
        )
    };
    drop(ctx);
    exec_res?;
    Ok(res)
}

fn cleanup_scope_tables<C, S>(
    state: &SharedState<C, S>,
    session: &mut SessionRuntime<C, S>,
    dropped_physical: Vec<String>,
)
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static + Default,
    S: Storage + crate::storage::CheckpointableStorage + Serialize + DeserializeOwned + Clone + 'static + Default,
{
    fn drop_physical_table(
        catalog: &mut dyn Catalog,
        storage: &mut dyn Storage,
        physical: &str,
    ) -> Result<(), DbError> {
        let Some(table) = catalog
            .get_tables()
            .iter()
            .find(|table| table.name.eq_ignore_ascii_case(physical))
            .cloned()
        else {
            return Ok(());
        };

        let schema_name = table.schema_name.clone();
        let table_name = table.name.clone();
        let table_id = table.id;
        catalog.drop_table(&schema_name, &table_name)?;
        storage.remove_table(table_id);
        Ok(())
    }

    if session.tx_manager.active.is_some() {
        if let Some(workspace) = session.workspace.as_mut() {
            for physical in dropped_physical {
                let _ = drop_physical_table(&mut workspace.catalog, &mut workspace.storage, &physical);
            }
        }
    } else {
        let mut storage_guard = state.storage.write();
        let (cat, stor) = storage_guard.get_mut_refs();
        for physical in dropped_physical {
            let _ = drop_physical_table(cat, stor, &physical);
        }
    }
}

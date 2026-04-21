#![allow(dead_code)]

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
use super::{CursorFetchResult, StatementExecutor};
use super::{EngineCatalog, EngineStorage};
use super::execution_support;
use super::cursor_rpc;

use super::dispatch::execute_non_transaction_statement;

impl<C, S> StatementExecutor for super::StatementExecutorService<C, S>
where
    C: EngineCatalog,
    S: EngineStorage,
{
    fn execute_session(
        &self,
        session_id: SessionId,
        stmt: Statement,
    ) -> Result<Option<QueryResult>, DbError> {
        execution_support::with_session(&self.state, session_id, |session| {
            execution_support::execute_single_statement(&self.state, session_id, session, stmt)
        })
    }

    fn execute_session_batch(
        &self,
        session_id: SessionId,
        stmts: Vec<Statement>,
    ) -> Result<Option<QueryResult>, DbError> {
        execution_support::with_session(&self.state, session_id, |session| {
            execution_support::execute_batch_statements(&self.state, session_id, session, stmts)
        })
    }

    fn execute_session_batch_sql(
        &self,
        session_id: SessionId,
        sql: &str,
    ) -> Result<Option<QueryResult>, DbError> {
        let quoted_ident = execution_support::with_session(&self.state, session_id, |session| {
            Ok(session.options.quoted_identifier)
        })?;

        let stmts = parse_batch_with_quoted_ident(sql, quoted_ident)?;
        execution_support::with_session(&self.state, session_id, |session| {
            execution_support::execute_batch_statements(&self.state, session_id, session, stmts)
        })
    }

    fn execute_session_batch_sql_multi(
        &self,
        session_id: SessionId,
        sql: &str,
    ) -> Result<Vec<Option<QueryResult>>, DbError> {
        let quoted_ident = execution_support::with_session(&self.state, session_id, |session| {
            Ok(session.options.quoted_identifier)
        })?;

        let stmts = parse_batch_with_quoted_ident(sql, quoted_ident)?;
        execution_support::with_session(&self.state, session_id, |session| {
            execution_support::execute_batch_statements_multi(
                &self.state,
                session_id,
                session,
                stmts,
            )
        })
    }

    fn set_session_metadata(
        &self,
        session_id: SessionId,
        user: Option<String>,
        app_name: Option<String>,
        host_name: Option<String>,
        database: Option<String>,
    ) -> Result<(), DbError> {
        execution_support::with_session(&self.state, session_id, |session| {
            session.user = user;
            session.app_name = app_name;
            session.host_name = host_name;
            if let Some(database) = database {
                session.current_database = database.clone();
                session.original_database = database;
            }
            Ok(())
        })
    }

    fn set_session_database(&self, session_id: SessionId, database: String) -> Result<(), DbError> {
        execution_support::with_session(&self.state, session_id, |session| {
            session.current_database = database;
            Ok(())
        })
    }

    // ── Cursor RPC operations ──────────────────────────────────────

    fn cursor_rpc_open(
        &self,
        session_id: SessionId,
        sql: &str,
        _scroll_opt: i32,
    ) -> Result<(i32, QueryResult), DbError> {
        cursor_rpc::cursor_rpc_open(&self.state, session_id, sql, _scroll_opt)
    }

    fn cursor_rpc_fetch(
        &self,
        session_id: SessionId,
        handle: i32,
        fetch_type: i32,
        row_num: i32,
        n_rows: i32,
    ) -> Result<CursorFetchResult, DbError> {
        cursor_rpc::cursor_rpc_fetch(&self.state, session_id, handle, fetch_type, row_num, n_rows)
    }

    fn cursor_rpc_close(&self, session_id: SessionId, handle: i32) -> Result<(), DbError> {
        cursor_rpc::cursor_rpc_close(&self.state, session_id, handle)
    }

    fn cursor_rpc_deallocate(&self, session_id: SessionId, handle: i32) -> Result<(), DbError> {
        cursor_rpc::cursor_rpc_deallocate(&self.state, session_id, handle)
    }

    fn set_bulk_load_active(
        &self,
        session_id: SessionId,
        active: bool,
        table: crate::ast::ObjectName,
        columns: Vec<crate::ast::statements::ddl::ColumnSpec>,
        received_metadata: bool,
    ) -> Result<(), DbError> {
        cursor_rpc::set_bulk_load_active(
            &self.state,
            session_id,
            active,
            table,
            columns,
            received_metadata,
        )
    }

    fn get_bulk_load_state(
        &self,
        session_id: SessionId,
    ) -> (
        bool,
        Option<crate::ast::ObjectName>,
        Option<Vec<crate::ast::statements::ddl::ColumnSpec>>,
        bool,
    ) {
        cursor_rpc::get_bulk_load_state(&self.state, session_id)
    }
}

#[allow(deprecated)]
#[allow(clippy::type_complexity)]
fn build_execution_context<'a, C, S>(
    session_id: SessionId,
    session: &'a mut SessionRuntime<C, S>,
    state: &SharedState<C, S>,
) -> (
    ExecutionContext<'a>,
    &'a mut crate::executor::transaction::TransactionManager<
        C,
        S,
        crate::executor::session::SessionSnapshot,
    >,
    &'a mut Box<dyn crate::executor::journal::Journal>,
    &'a mut Option<crate::executor::locks::TxWorkspace<C, S>>,
    &'a mut Box<dyn crate::executor::clock::Clock>,
    &'a mut crate::executor::tooling::SessionOptions,
)
where
    C: EngineCatalog,
    S: EngineStorage,
{
    let dirty_buffer = if session.tx_manager.active.is_some() {
        Some(state.dirty_buffer.clone())
    } else {
        None
    };
    let (
        clock,
        tx_manager,
        journal,
        variables,
        identities,
        tables,
        cursors,
        diagnostics,
        workspace,
        options,
        random_state,
        current_database,
        original_database,
        user,
        app_name,
        host_name,
    ) = (
        &mut session.clock,
        &mut session.tx_manager,
        &mut session.journal,
        &mut session.variables,
        &mut session.identities,
        &mut session.tables,
        &mut session.cursors,
        &mut session.diagnostics,
        &mut session.workspace,
        &mut session.options,
        &mut session.random_state,
        &mut session.current_database,
        &mut session.original_database,
        &mut session.user,
        &mut session.app_name,
        &mut session.host_name,
    );

    let mut ctx = ExecutionContext::new(
        variables,
        &mut session.bulk_load_active,
        &mut session.bulk_load_table,
        &mut session.bulk_load_columns,
        &mut session.bulk_load_received_metadata,
        &mut identities.last_identity,
        &mut identities.scope_stack,
        &mut tables.temp_map,
        &mut tables.var_map,
        &mut tables.var_counter,
        options.ansi_nulls,
        options.datefirst,
        random_state,
        &mut cursors.map,
        &mut cursors.fetch_status,
        &mut cursors.next_cursor_handle,
        &mut cursors.handle_map,
        &mut diagnostics.print_output,
        &mut session.context_info,
        &mut session.session_context,
        dirty_buffer,
        session_id,
        current_database.clone(),
        original_database.clone(),
        user.clone(),
        app_name.clone(),
        host_name.clone(),
    );
    ctx.options = options.clone();
    (ctx, tx_manager, journal, workspace, clock, options)
}

#[allow(clippy::too_many_arguments)]
fn execute_stmt_loop<C, S, F>(
    state: &SharedState<C, S>,
    session_id: SessionId,
    tx_manager: &mut crate::executor::transaction::TransactionManager<
        C,
        S,
        crate::executor::session::SessionSnapshot,
    >,
    journal: &mut Box<dyn crate::executor::journal::Journal>,
    workspace: &mut Option<crate::executor::locks::TxWorkspace<C, S>>,
    clock: &dyn crate::executor::clock::Clock,
    options: &mut crate::executor::tooling::SessionOptions,
    ctx: &mut ExecutionContext,
    stmts: Vec<Statement>,
    mut on_result: F,
) -> Result<(), DbError>
where
    C: EngineCatalog,
    S: EngineStorage,
    F: FnMut(Option<QueryResult>),
{
    for stmt in stmts {
        ctx.frame.trancount = tx_manager.depth;
        ctx.frame.xact_state = tx_manager.xact_state;
        ctx.session.identity_insert = options.identity_insert.clone();
        if is_transaction_statement(&stmt) {
            match transaction_exec::execute_transaction_statement(
                state,
                session_id,
                tx_manager,
                journal.as_mut(),
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

fn execute_batch_core_inner<C, S, F>(
    state: &SharedState<C, S>,
    session_id: SessionId,
    session: &mut SessionRuntime<C, S>,
    body: F,
) -> Result<(), DbError>
where
    C: EngineCatalog,
    S: EngineStorage,
    F: FnOnce(
        &mut ExecutionContext,
        &SharedState<C, S>,
        SessionId,
        &mut crate::executor::transaction::TransactionManager<
            C,
            S,
            crate::executor::session::SessionSnapshot,
        >,
        &mut Box<dyn crate::executor::journal::Journal>,
        &mut Option<crate::executor::locks::TxWorkspace<C, S>>,
        &dyn crate::executor::clock::Clock,
        &mut crate::executor::tooling::SessionOptions,
    ) -> Result<(), DbError>,
{
    let (mut ctx, tx_manager, journal, workspace, clock, options) =
        build_execution_context(session_id, session, state);

    ctx.enter_scope();

    let exec_res = body(
        &mut ctx,
        state,
        session_id,
        tx_manager,
        journal,
        workspace,
        clock.as_ref(),
        options,
    );
    let current_database = ctx.metadata.database.clone();

    // Scope cleanup always runs before error propagation — guarantees no leak
    // even when the body returns Err (BREAK/CONTINUE/deadlock/etc).
    let dropped_physical = ctx.leave_scope_collect_table_vars();
    let tx_active = tx_manager.active.is_some();
    let workspace = workspace.as_mut();
    drop(ctx);
    cleanup_scope_tables(state, tx_active, workspace, dropped_physical)?;
    if let Some(current_database) = current_database {
        session.current_database = current_database;
    }

    exec_res
}

pub(crate) fn execute_batch_statements<C, S>(
    state: &SharedState<C, S>,
    session_id: SessionId,
    session: &mut SessionRuntime<C, S>,
    stmts: Vec<Statement>,
) -> Result<Option<QueryResult>, DbError>
where
    C: EngineCatalog,
    S: EngineStorage,
{
    let mut last_res = None;
    let exec_res = execute_batch_core_inner(
        state,
        session_id,
        session,
        |ctx, state, session_id, tx_manager, journal, workspace, clock, options| {
            execute_stmt_loop(
                state,
                session_id,
                tx_manager,
                journal,
                workspace,
                clock,
                options,
                ctx,
                stmts,
                |r| {
                    last_res = r;
                },
            )
        },
    );
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
    C: EngineCatalog,
    S: EngineStorage,
{
    let mut results = Vec::new();
    let exec_res = execute_batch_core_inner(
        state,
        session_id,
        session,
        |ctx, state, session_id, tx_manager, journal, workspace, clock, options| {
            execute_stmt_loop(
                state,
                session_id,
                tx_manager,
                journal,
                workspace,
                clock,
                options,
                ctx,
                stmts,
                |r| {
                    results.push(r);
                },
            )
        },
    );
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
    C: EngineCatalog,
    S: EngineStorage,
{
    let mut res = None;
    let exec_res = {
        let (mut ctx, tx_manager, journal, workspace, clock, options) =
            build_execution_context(session_id, session, state);
        let exec_res = execute_stmt_loop(
            state,
            session_id,
            tx_manager,
            journal,
            workspace,
            clock.as_ref(),
            options,
            &mut ctx,
            vec![stmt],
            |r| {
                res = r;
            },
        );
        let current_database = ctx.metadata.database.clone();
        drop(ctx);
        if let Some(current_database) = current_database {
            session.current_database = current_database;
        }
        exec_res
    };
    exec_res?;
    Ok(res)
}

fn cleanup_scope_tables<C, S>(
    state: &SharedState<C, S>,
    tx_active: bool,
    workspace: Option<&mut crate::executor::locks::TxWorkspace<C, S>>,
    dropped_physical: Vec<String>,
) -> Result<(), DbError>
where
    C: EngineCatalog,
    S: EngineStorage,
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
        storage.remove_table(table_id)?;
        Ok(())
    }

    if tx_active {
        if let Some(workspace) = workspace {
            for physical in dropped_physical {
                drop_physical_table(&mut workspace.catalog, &mut workspace.storage, &physical)?;
            }
        }
    } else {
        let mut storage_guard = state.storage.write();
        let (cat, stor) = storage_guard.get_mut_refs();
        for physical in dropped_physical {
            drop_physical_table(cat, stor, &physical)?;
        }
    }

    Ok(())
}

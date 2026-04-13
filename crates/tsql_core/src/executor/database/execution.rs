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
use super::{EngineCatalog, EngineStorage};

use super::dispatch::execute_non_transaction_statement;

fn with_session<C, S, R, F>(
    state: &SharedState<C, S>,
    session_id: SessionId,
    f: F,
) -> Result<R, DbError>
where
    C: EngineCatalog,
    S: EngineStorage,
    F: FnOnce(&mut SessionRuntime<C, S>) -> Result<R, DbError>,
{
    let session_mutex = state
        .sessions
        .get(&session_id)
        .ok_or_else(|| DbError::Execution(format!("session {} not found", session_id)))?;
    let mut session = session_mutex.lock();
    f(&mut session)
}

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
        with_session(&self.state, session_id, |session| {
            execute_single_statement(&self.state, session_id, session, stmt)
        })
    }

    fn execute_session_batch(
        &self,
        session_id: SessionId,
        stmts: Vec<Statement>,
    ) -> Result<Option<QueryResult>, DbError> {
        with_session(&self.state, session_id, |session| {
            execute_batch_statements(&self.state, session_id, session, stmts)
        })
    }

    fn execute_session_batch_sql(
        &self,
        session_id: SessionId,
        sql: &str,
    ) -> Result<Option<QueryResult>, DbError> {
        let quoted_ident = with_session(&self.state, session_id, |session| {
            Ok(session.options.quoted_identifier)
        })?;

        let stmts = parse_batch_with_quoted_ident(sql, quoted_ident)?;
        with_session(&self.state, session_id, |session| {
            execute_batch_statements(&self.state, session_id, session, stmts)
        })
    }

    fn execute_session_batch_sql_multi(
        &self,
        session_id: SessionId,
        sql: &str,
    ) -> Result<Vec<Option<QueryResult>>, DbError> {
        let quoted_ident = with_session(&self.state, session_id, |session| {
            Ok(session.options.quoted_identifier)
        })?;

        let stmts = parse_batch_with_quoted_ident(sql, quoted_ident)?;
        with_session(&self.state, session_id, |session| {
            execute_batch_statements_multi(&self.state, session_id, session, stmts)
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
        with_session(&self.state, session_id, |session| {
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
        with_session(&self.state, session_id, |session| {
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
        with_session(&self.state, session_id, |session| {
            let quoted_ident = session.options.quoted_identifier;
            let stmts = crate::parser::parse_batch_with_quoted_ident(sql, quoted_ident)?;
            let first_stmt = stmts
                .into_iter()
                .next()
                .ok_or_else(|| DbError::Execution("cursor RPC open: empty SQL".to_string()))?;

            let select_ast = match first_stmt {
                crate::ast::Statement::Dml(crate::ast::DmlStatement::Select(s)) => s,
                _ => {
                    return Err(DbError::Execution(
                        "cursor RPC open: expected SELECT statement".to_string(),
                    ))
                }
            };

            // Generate cursor handle and name
            let handle = session.cursors.next_cursor_handle;
            session.cursors.next_cursor_handle += 1;
            let cursor_name = format!("#rpc_cursor_{}", handle);

            // Create cursor
            session.cursors.map.insert(
                cursor_name.clone(),
                crate::executor::model::Cursor {
                    query: Some(select_ast.clone()),
                    query_result: super::super::result::QueryResult::default(),
                    current_row: -1,
                },
            );
            session
                .cursors
                .handle_map
                .insert(handle, cursor_name.clone());

            // Execute the query
            let dirty_buffer = if session.tx_manager.active.is_some() {
                Some(self.state.dirty_buffer.clone())
            } else {
                None
            };

            let mut ctx = ExecutionContext::from_session(session, session_id, dirty_buffer);

            let query_result = {
                let storage_guard = self.state.storage.read();
                let (cat, stor) = storage_guard.get_refs();
                super::super::query::QueryExecutor {
                    catalog: cat,
                    storage: stor,
                    clock: &SystemClock,
                }
                .execute_select(
                    super::super::query::plan::RelationalQuery::from(select_ast),
                    &mut ctx,
                )?
            };

            if let Some(mut cursor) = ctx.session.cursors.get(&cursor_name).cloned() {
                cursor.query_result = query_result.clone();
                cursor.current_row = -1;
                ctx.session.cursors.insert(cursor_name, cursor);
            }

            Ok((handle, query_result))
        })
    }

    fn cursor_rpc_fetch(
        &self,
        session_id: SessionId,
        handle: i32,
        fetch_type: i32,
        row_num: i32,
        n_rows: i32,
    ) -> Result<CursorFetchResult, DbError> {
        with_session(&self.state, session_id, |session| {
            let cursor_name = session
                .cursors
                .handle_map
                .get(&handle)
                .cloned()
                .ok_or_else(|| DbError::Execution(format!("cursor handle {} not found", handle)))?;

            let mut cursor = session
                .cursors
                .map
                .get(&cursor_name)
                .cloned()
                .ok_or_else(|| DbError::cursor_not_declared(&cursor_name))?;

            let row_count = cursor.query_result.rows.len() as i64;

            // Map fetch_type to direction
            // 0x0001 = FIRST, 0x0002 = NEXT, 0x0004 = PREV, 0x0008 = LAST
            // 0x0010 = ABSOLUTE, 0x0020 = RELATIVE
            match fetch_type & 0xFF {
                0x01 => cursor.current_row = 0,
                0x02 => cursor.current_row += 1,
                0x04 => cursor.current_row -= 1,
                0x08 => cursor.current_row = row_count - 1,
                0x10 => {
                    if row_num > 0 {
                        cursor.current_row = row_num as i64 - 1;
                    } else if row_num < 0 {
                        cursor.current_row = row_count + row_num as i64;
                    } else {
                        cursor.current_row = -1;
                    }
                }
                0x20 => {
                    cursor.current_row += row_num as i64;
                }
                _ => cursor.current_row += 1,
            }

            let mut fetched_rows = Vec::new();
            let fetch_status;
            let n = if n_rows <= 0 { 1 } else { n_rows as usize };

            if cursor.current_row >= 0 && cursor.current_row < row_count {
                for i in 0..n {
                    let idx = cursor.current_row + i as i64;
                    if idx >= 0 && idx < row_count {
                        fetched_rows.push(cursor.query_result.rows[idx as usize].clone());
                    }
                }
                if fetched_rows.is_empty() {
                    fetch_status = -1;
                } else {
                    fetch_status = 0;
                    if fetch_type & 0x02 != 0 && n_rows > 1 {
                        cursor.current_row += n_rows as i64 - 1;
                    }
                }
            } else {
                fetch_status = -1;
                if cursor.current_row < 0 {
                    cursor.current_row = -1;
                } else if cursor.current_row >= row_count {
                    cursor.current_row = row_count;
                }
            }

            let columns = cursor.query_result.columns.clone();
            let column_types = cursor.query_result.column_types.clone();
            let column_nullabilities = cursor.query_result.column_nullabilities.clone();

            session.cursors.fetch_status = fetch_status;
            session.cursors.map.insert(cursor_name, cursor);

            Ok(CursorFetchResult {
                handle,
                rows: fetched_rows,
                columns,
                column_types,
                column_nullabilities,
                fetch_status,
            })
        })
    }

    fn cursor_rpc_close(&self, session_id: SessionId, handle: i32) -> Result<(), DbError> {
        with_session(&self.state, session_id, |session| {
            let cursor_name = session
                .cursors
                .handle_map
                .get(&handle)
                .cloned()
                .ok_or_else(|| DbError::Execution(format!("cursor handle {} not found", handle)))?;

            if let Some(mut cursor) = session.cursors.map.get(&cursor_name).cloned() {
                cursor.current_row = -1;
                session.cursors.map.insert(cursor_name, cursor);
            }
            Ok(())
        })
    }

    fn cursor_rpc_deallocate(&self, session_id: SessionId, handle: i32) -> Result<(), DbError> {
        with_session(&self.state, session_id, |session| {
            let cursor_name =
                session.cursors.handle_map.remove(&handle).ok_or_else(|| {
                    DbError::Execution(format!("cursor handle {} not found", handle))
                })?;
            session.cursors.map.remove(&cursor_name);
            Ok(())
        })
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
        &mut session.original_database,
        &mut session.user,
        &mut session.app_name,
        &mut session.host_name,
    );

    let mut ctx = ExecutionContext::new(
        variables,
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
        dirty_buffer,
        session_id,
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

    // Scope cleanup always runs before error propagation — guarantees no leak
    // even when the body returns Err (BREAK/CONTINUE/deadlock/etc).
    let dropped_physical = ctx.leave_scope_collect_table_vars();
    let tx_active = tx_manager.active.is_some();
    let workspace = workspace.as_mut();
    drop(ctx);
    cleanup_scope_tables(state, tx_active, workspace, dropped_physical)?;

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
        drop(ctx);
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

// ── Cursor RPC helpers ──────────────────────────────────────────────

use super::super::clock::SystemClock;
use super::CursorFetchResult;

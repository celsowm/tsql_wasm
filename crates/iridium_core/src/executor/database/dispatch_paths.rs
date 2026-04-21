use crate::ast::{DmlStatement, IsolationLevel, Statement};
use crate::error::{DbError, StmtOutcome, StmtResult};

use super::super::clock::{Clock, SystemClock};
use super::super::context::ExecutionContext;
use super::super::dirty_buffer;
use super::super::journal::Journal;
use super::super::locks::{LockTable, SessionId, TxWorkspace};
use super::super::result::QueryResult;
use super::super::script::ScriptExecutor;
use super::super::session::{SessionSnapshot, SharedState};
use super::super::table_util::{collect_write_tables, is_transaction_statement};
use super::super::tooling::SessionOptions;
use super::super::transaction::TransactionManager;
use super::super::transaction_exec;
use super::dispatch_helpers;
use super::{EngineCatalog, EngineStorage};
use crate::catalog::Catalog;
use crate::storage::Storage;

fn create_script_executor<'a>(
    catalog: &'a mut dyn Catalog,
    storage: &'a mut dyn Storage,
    clock: &'a dyn Clock,
) -> ScriptExecutor<'a> {
    ScriptExecutor {
        catalog,
        storage,
        clock,
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_write_without_transaction<C, S>(
    state: &SharedState<C, S>,
    session_id: SessionId,
    tx_manager: &mut TransactionManager<C, S, SessionSnapshot>,
    workspace_slot: &mut Option<TxWorkspace<C, S>>,
    session_options: &SessionOptions,
    stmt: Statement,
    ctx: &mut ExecutionContext,
    clock: &dyn Clock,
) -> StmtResult<Option<QueryResult>>
where
    C: EngineCatalog,
    S: EngineStorage,
{
    LockTable::acquire_statement_locks(
        &state.table_locks,
        session_id,
        tx_manager,
        workspace_slot,
        &stmt,
        session_options.lock_timeout_ms,
        session_options.deadlock_priority,
        &|sid| dispatch_helpers::lookup_session_deadlock_priority(state, sid),
    )?;

    let mut storage_guard = state.storage.write();
    let before_catalog = storage_guard.catalog.clone();
    let before_storage = storage_guard.storage.clone();
    let before_versions = storage_guard.table_versions.clone();
    let before_commit_ts = storage_guard.commit_ts;
    let (cat, stor) = storage_guard.get_mut_refs();
    let mut script = create_script_executor(cat, stor, clock);
    let out = script.execute(stmt.clone(), ctx);
    let is_control_flow = out.as_ref().is_ok_and(|o| o.is_control_flow());
    if out.is_ok() && !is_control_flow {
        storage_guard.commit_ts += 1;
        let written_tables = collect_write_tables(&stmt);
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
        let tx_id = state.allocate_tx_id();
        state.wal_auto_commit(tx_id);
    }
    state.table_locks.lock().release_all_for_session(session_id);
    out
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_read_without_transaction<C, S>(
    state: &SharedState<C, S>,
    session_id: SessionId,
    tx_manager: &mut TransactionManager<C, S, SessionSnapshot>,
    workspace_slot: &mut Option<TxWorkspace<C, S>>,
    session_options: &SessionOptions,
    stmt: Statement,
    ctx: &mut ExecutionContext,
    clock: &dyn Clock,
) -> StmtResult<Option<QueryResult>>
where
    C: EngineCatalog,
    S: EngineStorage,
{
    LockTable::acquire_statement_locks(
        &state.table_locks,
        session_id,
        tx_manager,
        workspace_slot,
        &stmt,
        session_options.lock_timeout_ms,
        session_options.deadlock_priority,
        &|sid| dispatch_helpers::lookup_session_deadlock_priority(state, sid),
    )?;

    if let Statement::Dml(DmlStatement::Select(select_stmt)) = stmt {
        let storage_guard = state.storage.read();
        let (cat, stor) = storage_guard.get_refs();
        let qe = super::super::query::QueryExecutor {
            catalog: cat,
            storage: stor,
            clock,
        };
        let result = qe.execute_select(
            super::super::query::plan::RelationalQuery::from(select_stmt),
            ctx,
        )?;
        state.table_locks.lock().release_all_for_session(session_id);
        return Ok(StmtOutcome::Ok(Some(result)));
    }

    let mut storage_guard = state.storage.write();
    let (cat, stor) = storage_guard.get_mut_refs();
    let mut script = create_script_executor(cat, stor, clock);
    let out = script.execute(stmt, ctx);
    state.table_locks.lock().release_all_for_session(session_id);
    out
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_dirty_read_without_transaction<C, S>(
    state: &SharedState<C, S>,
    session_id: SessionId,
    workspace_slot: &mut Option<TxWorkspace<C, S>>,
    stmt: Statement,
    ctx: &mut ExecutionContext,
    clock: &dyn Clock,
) -> StmtResult<Option<QueryResult>>
where
    C: EngineCatalog,
    S: EngineStorage,
{
    let (mut dirty_catalog, mut dirty_storage) =
        dirty_buffer::build_dirty_read_storage(state, session_id, workspace_slot)?;
    let mut script = create_script_executor(&mut dirty_catalog, &mut dirty_storage, clock);
    script.execute(stmt, ctx)
}

#[allow(clippy::too_many_arguments)]
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
    C: EngineCatalog,
    S: EngineStorage,
{
    if let Some(result) = dispatch_helpers::handle_session_statement(
        state,
        session_id,
        &stmt,
        session_options,
        ctx,
        journal,
    ) {
        return result;
    }

    if session_options.fmtonly {
        if let Statement::Dml(DmlStatement::Select(select_stmt)) = &stmt {
            return execute_fmt_only_select(state, select_stmt, ctx);
        }
    }

    if session_options.noexec {
        if let Statement::Dml(DmlStatement::Select(select_stmt)) = &stmt {
            return execute_fmt_only_select(state, select_stmt, ctx);
        }
    }

    if tx_manager.active.is_none()
        && session_options.implicit_transactions
        && dispatch_helpers::should_start_implicit_transaction(&stmt)
        && !is_transaction_statement(&stmt)
    {
        transaction_exec::execute_transaction_statement(
            state,
            session_id,
            tx_manager,
            journal,
            workspace_slot,
            ctx,
            session_options,
            Statement::Transaction(crate::ast::TransactionStatement::Begin(None)),
        )?;
    }

    let isolation_level = tx_manager
        .active
        .as_ref()
        .map(|tx| tx.isolation_level)
        .unwrap_or(tx_manager.session_isolation_level);
    let is_select = matches!(stmt, Statement::Dml(DmlStatement::Select(_)));
    let read_uncommitted_dirty = isolation_level == IsolationLevel::ReadUncommitted && is_select;

    if tx_manager.active.is_some() {
        execute_in_transaction(
            state,
            session_id,
            tx_manager,
            journal,
            workspace_slot,
            clock,
            session_options,
            stmt,
            ctx,
        )
    } else if read_uncommitted_dirty {
        execute_dirty_read_without_transaction(state, session_id, workspace_slot, stmt, ctx, clock)
    } else {
        let written_tables = collect_write_tables(&stmt);
        if written_tables.is_empty() {
            execute_read_without_transaction(
                state,
                session_id,
                tx_manager,
                workspace_slot,
                session_options,
                stmt,
                ctx,
                clock,
            )
        } else {
            execute_write_without_transaction(
                state,
                session_id,
                tx_manager,
                workspace_slot,
                session_options,
                stmt,
                ctx,
                clock,
            )
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn execute_in_transaction<C, S>(
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
    C: EngineCatalog,
    S: EngineStorage,
{
    LockTable::acquire_statement_locks(
        &state.table_locks,
        session_id,
        tx_manager,
        workspace_slot,
        &stmt,
        session_options.lock_timeout_ms,
        session_options.deadlock_priority,
        &|sid| dispatch_helpers::lookup_session_deadlock_priority(state, sid),
    )?;

    ctx.options = session_options.clone();
    ctx.metadata.ansi_nulls = session_options.ansi_nulls;
    ctx.metadata.datefirst = session_options.datefirst;

    let isolation_level = tx_manager
        .active
        .as_ref()
        .map(|tx| tx.isolation_level)
        .unwrap_or(tx_manager.session_isolation_level);
    let is_select = matches!(stmt, Statement::Dml(DmlStatement::Select(_)));
    let read_uncommitted_dirty = isolation_level == IsolationLevel::ReadUncommitted && is_select;
    let read_committed_select = isolation_level == IsolationLevel::ReadCommitted && is_select;

    if tx_manager.active.is_none()
        && session_options.implicit_transactions
        && dispatch_helpers::should_start_implicit_transaction(&stmt)
        && !is_transaction_statement(&stmt)
    {
        transaction_exec::execute_transaction_statement(
            state,
            session_id,
            tx_manager,
            journal,
            workspace_slot,
            ctx,
            session_options,
            Statement::Transaction(crate::ast::TransactionStatement::Begin(None)),
        )?;
    }

    let out = if read_uncommitted_dirty {
        let (mut dirty_catalog, mut dirty_storage) =
            dirty_buffer::build_dirty_read_storage(state, session_id, workspace_slot)?;
        let mut script = create_script_executor(&mut dirty_catalog, &mut dirty_storage, clock);
        script.execute(stmt.clone(), ctx)
    } else if read_committed_select {
        let workspace = workspace_slot.as_mut().ok_or_else(|| {
            DbError::Execution("internal error: missing transaction workspace".into())
        })?;
        dispatch_helpers::refresh_workspace_for_read_committed(state, workspace, &stmt)?;
        let mut script =
            create_script_executor(&mut workspace.catalog, &mut workspace.storage, clock);
        script.execute(stmt.clone(), ctx)
    } else {
        let workspace = workspace_slot.as_mut().ok_or_else(|| {
            DbError::Execution("internal error: missing transaction workspace".into())
        })?;
        let mut script =
            create_script_executor(&mut workspace.catalog, &mut workspace.storage, clock);
        script.execute(stmt.clone(), ctx)
    };

    dispatch_helpers::update_transaction_state(
        &out,
        tx_manager,
        state,
        session_id,
        journal,
        workspace_slot,
        ctx,
        session_options,
        &stmt,
    );
    out
}

fn execute_fmt_only_select<C, S>(
    state: &SharedState<C, S>,
    select_stmt: &crate::ast::SelectStmt,
    ctx: &mut ExecutionContext,
) -> StmtResult<Option<QueryResult>>
where
    C: EngineCatalog,
    S: EngineStorage,
{
    let storage_guard = state.storage.read();
    let (catalog, storage) = storage_guard.get_refs();

    let qe = super::super::query::QueryExecutor {
        catalog,
        storage,
        clock: &SystemClock,
    };

    let query_plan = super::super::query::plan::RelationalQuery::from(select_stmt.clone());
    let fake_rows = vec![];

    let result = match super::super::query::pipeline::execute_rows_to_result(
        &qe,
        &query_plan,
        fake_rows,
        ctx,
    ) {
        Ok(mut r) => {
            r.rows = vec![];
            r
        }
        Err(_e) => {
            return Ok(StmtOutcome::Ok(Some(QueryResult {
                columns: vec![],
                column_types: vec![],
                column_nullabilities: vec![],
                rows: vec![],
                return_status: None,
                is_procedure: false,
            })));
        }
    };

    Ok(StmtOutcome::Ok(Some(result)))
}

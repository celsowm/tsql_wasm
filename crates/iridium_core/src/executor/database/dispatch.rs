use crate::ast::{DmlStatement, IsolationLevel, SessionStatement, Statement};
use crate::catalog::Catalog;
use crate::error::{DbError, StmtOutcome, StmtResult};
use crate::storage::Storage;
use crate::types::Value;

use super::super::clock::Clock;
use super::super::clock::SystemClock;
use super::super::context::ExecutionContext;
use super::super::database_catalog::{database_id_for_name, database_name_for_id};
use super::super::dirty_buffer;
use super::super::journal::{Journal, JournalEvent};
use super::super::locks::{LockTable, SessionId, TxWorkspace};
use super::super::result::QueryResult;
use super::super::script::ScriptExecutor;
use super::super::session::{SessionSnapshot, SharedState};
use super::super::string_norm::normalize_identifier;
use super::super::table_util::{
    collect_read_tables, collect_write_tables, is_transaction_statement,
};
use super::super::tooling::{apply_set_option, SessionOptions};
use super::super::transaction::TransactionManager;
use super::super::transaction_exec;
use super::{EngineCatalog, EngineStorage};

/// D4: Factory function to create a ScriptExecutor, eliminating
/// 6× inline constructions scattered across dispatch.
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

fn should_start_implicit_transaction(stmt: &Statement) -> bool {
    matches!(
        stmt,
        Statement::Dml(DmlStatement::Insert(_))
            | Statement::Dml(DmlStatement::Update(_))
            | Statement::Dml(DmlStatement::Delete(_))
            | Statement::Dml(DmlStatement::Merge(_))
            | Statement::Dml(DmlStatement::SelectAssign(_))
            | Statement::Ddl(_)
    )
}

fn lookup_session_deadlock_priority<C, S>(state: &SharedState<C, S>, session_id: SessionId) -> i32
where
    C: EngineCatalog,
    S: EngineStorage,
{
    state
        .deadlock_priorities
        .get(&session_id)
        .map(|priority| *priority)
        .unwrap_or(0)
}

/// S3: Extract session option handling from the main dispatch function.
fn handle_session_statement<C: Catalog, S: Storage>(
    state: &SharedState<C, S>,
    session_id: SessionId,
    stmt: &Statement,
    session_options: &mut SessionOptions,
    ctx: &mut ExecutionContext,
    journal: &mut dyn Journal,
) -> Option<StmtResult<Option<QueryResult>>> {
    if let Statement::Session(SessionStatement::UseDatabase(database)) = stmt {
        match database_id_for_name(database) {
            Some(database_id) => {
                let canonical_name = database_name_for_id(database_id)
                    .unwrap_or(database.as_str())
                    .to_string();
                ctx.metadata.database = Some(canonical_name);
                Some(Ok(StmtOutcome::Ok(None)))
            }
            None => Some(Err(DbError::Execution(format!(
                "Cannot open database '{}' requested by the login. The login failed.",
                database
            )))),
        }
    } else if let Statement::Session(SessionStatement::SetOption(opt)) = stmt {
        match apply_set_option(opt, session_options) {
            Ok(apply) => {
                ctx.options = session_options.clone();
                ctx.metadata.ansi_nulls = session_options.ansi_nulls;
                ctx.metadata.datefirst = session_options.datefirst;
                state
                    .deadlock_priorities
                    .insert(session_id, session_options.deadlock_priority);
                for warn in apply.warnings {
                    journal.record(JournalEvent::Info { message: warn });
                }
                Some(Ok(StmtOutcome::Ok(None)))
            }
            Err(e) => Some(Err(e)),
        }
    } else if let Statement::Session(SessionStatement::SetIdentityInsert(ref id_stmt)) = stmt {
        let storage_guard = state.storage.read();
        let (catalog, _) = storage_guard.get_refs();
        let schema = id_stmt.table.schema_or_dbo();
        if catalog.find_table(schema, &id_stmt.table.name).is_none() {
            return Some(Err(DbError::table_not_found(schema, &id_stmt.table.name)));
        }

        let table_name = normalize_identifier(&id_stmt.table.name);
        if id_stmt.on {
            // SQL Server only allows one table to have IDENTITY_INSERT ON at a time in a session.
            session_options.identity_insert.clear();
            session_options.identity_insert.insert(table_name);
        } else {
            session_options.identity_insert.remove(&table_name);
        }
        ctx.options.identity_insert = session_options.identity_insert.clone();
        Some(Ok(StmtOutcome::Ok(None)))
    } else if let Statement::Session(SessionStatement::SetContextInfo(ref expr)) = stmt {
        let storage_guard = state.storage.read();
        let (catalog, storage) = storage_guard.get_refs();
        match crate::executor::evaluator::eval_expr(
            expr,
            &[],
            ctx,
            catalog as &dyn Catalog,
            storage as &dyn Storage,
            &SystemClock,
        ) {
            Ok(val) => {
                let bytes = match val {
                    Value::Null => vec![0u8; 128],
                    Value::Binary(mut b) | Value::VarBinary(mut b) => {
                        if b.len() > 128 {
                            b.truncate(128);
                        } else {
                            b.resize(128, 0);
                        }
                        b
                    }
                    _ => {
                        let mut b = val.to_string_value().into_bytes();
                        if b.len() > 128 {
                            b.truncate(128);
                        } else {
                            b.resize(128, 0);
                        }
                        b
                    }
                };
                *ctx.session.context_info = bytes;
                Some(Ok(StmtOutcome::Ok(None)))
            }
            Err(e) => Some(Err(e)),
        }
    } else {
        None
    }
}

/// S3: Extract workspace refresh for READ COMMITTED isolation.
/// Only refreshes tables that the SELECT actually references (P1 #21).
fn refresh_workspace_for_read_committed<C, S>(
    state: &SharedState<C, S>,
    workspace: &mut TxWorkspace<C, S>,
    stmt: &Statement,
) -> Result<(), DbError>
where
    C: EngineCatalog,
    S: EngineStorage,
{
    let storage_guard = state.storage.read();
    let read_tables = collect_read_tables(stmt);
    for table_def in storage_guard.catalog.get_tables() {
        let tname = normalize_identifier(&table_def.name);
        if workspace.write_tables.contains(&tname) {
            continue;
        }
        if !read_tables.is_empty() && !read_tables.contains(&tname) {
            continue;
        }
        let tid = table_def.id;
        if let Ok(rows) = storage_guard.storage.scan_rows(tid) {
            if let Ok(committed_rows) = rows.collect::<Result<Vec<_>, DbError>>() {
                workspace.storage.replace_table(tid, committed_rows)?;
            }
        }
    }
    for table_def in storage_guard.catalog.get_tables() {
        let tname = normalize_identifier(&table_def.name);
        if workspace.write_tables.contains(&tname) {
            continue;
        }
        if !read_tables.is_empty() && !read_tables.contains(&tname) {
            continue;
        }
        if workspace
            .catalog
            .find_table(table_def.schema_or_dbo(), &table_def.name)
            .is_none()
        {
            workspace.catalog.register_table(table_def.clone());
        }
    }
    Ok(())
}

/// S3: Extract transaction state update logic after statement execution.
#[allow(clippy::too_many_arguments)]
fn update_transaction_state<C, S>(
    out: &StmtResult<Option<QueryResult>>,
    tx_manager: &mut TransactionManager<C, S, SessionSnapshot>,
    state: &SharedState<C, S>,
    session_id: SessionId,
    journal: &mut dyn Journal,
    workspace_slot: &mut Option<TxWorkspace<C, S>>,
    ctx: &mut ExecutionContext,
    session_options: &mut SessionOptions,
    stmt: &Statement,
) where
    C: EngineCatalog,
    S: EngineStorage,
{
    let is_control_flow = out.as_ref().is_ok_and(|o| o.is_control_flow());
    if out.is_ok() && !is_control_flow {
        transaction_exec::register_read_tables(workspace_slot, stmt);
        transaction_exec::register_workspace_write_tables(workspace_slot, stmt);
        transaction_exec::register_write_intent::<C, S>(tx_manager, journal, stmt);
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
}

/// S3: Execute a statement within an active transaction.
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
        &|sid| lookup_session_deadlock_priority(state, sid),
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
        && should_start_implicit_transaction(&stmt)
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
        refresh_workspace_for_read_committed(state, workspace, &stmt)?;
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

    update_transaction_state(
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

/// S3: Execute a write statement without an active transaction.
/// Uses clone-and-rollback for durability safety.
#[allow(clippy::too_many_arguments)]
fn execute_write_without_transaction<C, S>(
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
        &|sid| lookup_session_deadlock_priority(state, sid),
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

/// S3: Execute a read-only statement without an active transaction.
/// P1 #20: Uses `state.storage.read()` instead of `state.storage.write()`
/// for plain SELECT statements. Complex read paths (CTEs, SELECT ASSIGN)
/// fall back to the write path until a read-only ScriptExecutor is available.
#[allow(clippy::too_many_arguments)]
fn execute_read_without_transaction<C, S>(
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
        &|sid| lookup_session_deadlock_priority(state, sid),
    )?;

    // P1 #20: Use read lock for plain SELECT statements
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

    // Fall back to write path for CTEs, SELECT ASSIGN, etc.
    let mut storage_guard = state.storage.write();
    let (cat, stor) = storage_guard.get_mut_refs();
    let mut script = create_script_executor(cat, stor, clock);
    let out = script.execute(stmt, ctx);
    state.table_locks.lock().release_all_for_session(session_id);
    out
}

/// S3: Execute a dirty-read (READ UNCOMMITTED) statement without an active transaction.
#[allow(clippy::too_many_arguments)]
fn execute_dirty_read_without_transaction<C, S>(
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

/// Main dispatch entry point for non-transaction statement execution.
/// Delegates to focused sub-functions for each execution path (S3).
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
    // Handle session-level statements early (SetOption, SetIdentityInsert)
    if let Some(result) =
        handle_session_statement(state, session_id, &stmt, session_options, ctx, journal)
    {
        return result;
    }

    // P1 #23: Handle FMTONLY - return metadata without executing
    if session_options.fmtonly {
        if let Statement::Dml(DmlStatement::Select(select_stmt)) = &stmt {
            return execute_fmt_only_select(state, select_stmt, ctx);
        }
    }

    // P1 #23: Handle NOEXEC - compile query but don't execute (returns metadata like FMTONLY)
    if session_options.noexec {
        if let Statement::Dml(DmlStatement::Select(select_stmt)) = &stmt {
            return execute_fmt_only_select(state, select_stmt, ctx);
        }
    }

    if tx_manager.active.is_none()
        && session_options.implicit_transactions
        && should_start_implicit_transaction(&stmt)
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

/// P1 #23: Execute a SELECT statement under SET FMTONLY ON.
/// Returns metadata (column names, types) without actually running the query.
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

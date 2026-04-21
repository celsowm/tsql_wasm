use crate::ast::{DmlStatement, Statement};
use crate::error::DbError;

use super::super::journal::Journal;
use super::super::locks::{SessionId, TxWorkspace};
use super::super::result::QueryResult;
use super::super::session::{SessionSnapshot, SharedState};
use super::super::string_norm::normalize_identifier;
use super::super::table_util::collect_read_tables;
use super::super::tooling::SessionOptions;
use super::super::transaction::TransactionManager;
use super::{EngineCatalog, EngineStorage};
use super::super::context::ExecutionContext;

pub(crate) fn should_start_implicit_transaction(stmt: &Statement) -> bool {
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

pub(crate) fn lookup_session_deadlock_priority<C, S>(state: &SharedState<C, S>, session_id: SessionId) -> i32
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

pub(crate) fn refresh_workspace_for_read_committed<C, S>(
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

pub(crate) fn update_transaction_state<C, S>(
    out: &crate::error::StmtResult<Option<QueryResult>>,
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
        super::super::transaction_exec::register_read_tables(workspace_slot, stmt);
        super::super::transaction_exec::register_workspace_write_tables(workspace_slot, stmt);
        super::super::transaction_exec::register_write_intent::<C, S>(tx_manager, journal, stmt);
        if tx_manager.xact_state != -1 {
            tx_manager.xact_state = 1;
        }
    } else if out.is_err() && session_options.xact_abort {
        super::super::transaction_exec::force_xact_abort(
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

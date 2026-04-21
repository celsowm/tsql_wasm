use crate::ast::{DmlStatement, SessionStatement, Statement};
use crate::catalog::Catalog;
use crate::error::{DbError, StmtOutcome, StmtResult};

use super::super::clock::SystemClock;
use super::super::context::ExecutionContext;
use super::super::journal::{Journal, JournalEvent};
use super::super::locks::{SessionId, TxWorkspace};
use super::super::result::QueryResult;
use super::super::session::{SessionSnapshot, SharedState};
use super::super::string_norm::normalize_identifier;
use super::super::table_util::collect_read_tables;
use super::super::tooling::SessionOptions;
use super::super::tooling::apply_set_option;
use super::super::transaction::TransactionManager;
use super::{EngineCatalog, EngineStorage};
use crate::storage::Storage;
use crate::types::Value;

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

pub(crate) fn handle_session_statement<C, S>(
    state: &SharedState<C, S>,
    session_id: SessionId,
    stmt: &Statement,
    session_options: &mut SessionOptions,
    ctx: &mut ExecutionContext,
    journal: &mut dyn Journal,
) -> Option<StmtResult<Option<QueryResult>>>
where
    C: EngineCatalog,
    S: EngineStorage,
{
    if let Statement::Session(SessionStatement::UseDatabase(database)) = stmt {
        match super::super::database_catalog::database_id_for_name(database) {
            Some(database_id) => {
                let canonical_name = super::super::database_catalog::database_name_for_id(database_id)
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

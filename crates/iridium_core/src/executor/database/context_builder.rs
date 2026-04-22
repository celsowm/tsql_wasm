use super::super::context::ExecutionContext;
use super::super::locks::SessionId;
use super::super::session::{SessionRuntime, SharedState};
use super::{EngineCatalog, EngineStorage};

#[allow(deprecated)]
#[allow(clippy::type_complexity)]
pub(crate) fn build_execution_context<'a, C, S>(
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

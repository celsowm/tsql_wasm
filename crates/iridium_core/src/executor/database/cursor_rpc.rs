use crate::error::DbError;
use crate::parser::parse_batch_with_quoted_ident;

use super::super::clock::SystemClock;
use super::super::context::ExecutionContext;
use super::super::locks::SessionId;
use super::super::model::Cursor;
use super::super::result::QueryResult;
use super::super::session::SharedState;
use super::{CursorFetchResult, EngineCatalog, EngineStorage};

pub(crate) fn cursor_rpc_open<C, S>(
    state: &SharedState<C, S>,
    session_id: SessionId,
    sql: &str,
    _scroll_opt: i32,
) -> Result<(i32, QueryResult), DbError>
where
    C: EngineCatalog,
    S: EngineStorage,
{
    super::execution_support::with_session(state, session_id, |session| {
        let quoted_ident = session.options.quoted_identifier;
        let stmts = parse_batch_with_quoted_ident(sql, quoted_ident)?;
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

        let handle = session.cursors.next_cursor_handle;
        session.cursors.next_cursor_handle += 1;
        let cursor_name = format!("#rpc_cursor_{}", handle);

        session.cursors.map.insert(
            cursor_name.clone(),
            Cursor {
                query: Some(select_ast.clone()),
                query_result: QueryResult::default(),
                current_row: -1,
            },
        );
        session
            .cursors
            .handle_map
            .insert(handle, cursor_name.clone());

        let dirty_buffer = if session.tx_manager.active.is_some() {
            Some(state.dirty_buffer.clone())
        } else {
            None
        };

        let mut ctx = ExecutionContext::from_session(session, session_id, dirty_buffer);

        let query_result = {
            let storage_guard = state.storage.read();
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

pub(crate) fn cursor_rpc_fetch<C, S>(
    state: &SharedState<C, S>,
    session_id: SessionId,
    handle: i32,
    fetch_type: i32,
    row_num: i32,
    n_rows: i32,
) -> Result<CursorFetchResult, DbError>
where
    C: EngineCatalog,
    S: EngineStorage,
{
    super::execution_support::with_session(state, session_id, |session| {
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

pub(crate) fn cursor_rpc_close<C, S>(
    state: &SharedState<C, S>,
    session_id: SessionId,
    handle: i32,
) -> Result<(), DbError>
where
    C: EngineCatalog,
    S: EngineStorage,
{
    super::execution_support::with_session(state, session_id, |session| {
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

pub(crate) fn cursor_rpc_deallocate<C, S>(
    state: &SharedState<C, S>,
    session_id: SessionId,
    handle: i32,
) -> Result<(), DbError>
where
    C: EngineCatalog,
    S: EngineStorage,
{
    super::execution_support::with_session(state, session_id, |session| {
        let cursor_name = session
            .cursors
            .handle_map
            .remove(&handle)
            .ok_or_else(|| DbError::Execution(format!("cursor handle {} not found", handle)))?;
        session.cursors.map.remove(&cursor_name);
        Ok(())
    })
}

pub(crate) fn set_bulk_load_active<C, S>(
    state: &SharedState<C, S>,
    session_id: SessionId,
    active: bool,
    table: crate::ast::ObjectName,
    columns: Vec<crate::ast::statements::ddl::ColumnSpec>,
    received_metadata: bool,
) -> Result<(), DbError>
where
    C: EngineCatalog,
    S: EngineStorage,
{
    super::execution_support::with_session(state, session_id, |session| {
        session.bulk_load_active = active;
        session.bulk_load_table = Some(table);
        session.bulk_load_columns = Some(columns);
        session.bulk_load_received_metadata = received_metadata;
        Ok(())
    })
}

pub(crate) fn get_bulk_load_state<C, S>(
    state: &SharedState<C, S>,
    session_id: SessionId,
) -> (
    bool,
    Option<crate::ast::ObjectName>,
    Option<Vec<crate::ast::statements::ddl::ColumnSpec>>,
    bool,
)
where
    C: EngineCatalog,
    S: EngineStorage,
{
    super::execution_support::with_session(state, session_id, |session| {
        Ok((
            session.bulk_load_active,
            session.bulk_load_table.clone(),
            session.bulk_load_columns.clone(),
            session.bulk_load_received_metadata,
        ))
    })
    .unwrap_or((false, None, None, false))
}

use crate::ast::Statement;
use crate::error::DbError;
use crate::parser::parse_batch_with_quoted_ident;

use super::super::locks::SessionId;
use super::super::result::QueryResult;
use super::{CursorFetchResult, StatementExecutor};
use super::{EngineCatalog, EngineStorage};
use super::execution_support;
use super::cursor_rpc;

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

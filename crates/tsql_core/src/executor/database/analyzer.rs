use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::ast::IsolationLevel;
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::parser::parse_sql;
use crate::storage::Storage;

use super::super::locks::{SessionId};
use super::super::tooling::{
    analyze_sql_batch, collect_read_tables as collect_read_tables_tooling,
    collect_write_tables as collect_write_tables_tooling, explain_statement, split_sql_statements,
    statement_compat_warnings, CompatibilityReport, ExecutionTrace, ExplainPlan, SessionOptions,
    TraceStatementEvent,
};
use super::{RandomSeed, SqlAnalyzer, StatementExecutor};

impl<C, S> SqlAnalyzer for super::SqlAnalyzerService<C, S>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static + Default,
    S: Storage + crate::storage::CheckpointableStorage + Serialize + DeserializeOwned + Clone + 'static + Default,
{
    fn session_isolation_level(&self, session_id: SessionId) -> Result<IsolationLevel, DbError> {
        let session_mutex = self.state.sessions.get(&session_id)
            .ok_or_else(|| DbError::Execution(format!("session {} not found", session_id)))?;
        let session = session_mutex.lock();
        Ok(session.tx_manager.session_isolation_level)
    }

    fn transaction_is_active(&self, session_id: SessionId) -> Result<bool, DbError> {
        let session_mutex = self.state.sessions.get(&session_id)
            .ok_or_else(|| DbError::Execution(format!("session {} not found", session_id)))?;
        let session = session_mutex.lock();
        Ok(session.tx_manager.active.is_some())
    }

    fn session_options(&self, session_id: SessionId) -> Result<SessionOptions, DbError> {
        let session_mutex = self.state.sessions.get(&session_id)
            .ok_or_else(|| DbError::Execution(format!("session {} not found", session_id)))?;
        let session = session_mutex.lock();
        Ok(session.options.clone())
    }

    fn analyze_sql_batch(&self, sql: &str) -> CompatibilityReport {
        analyze_sql_batch(sql)
    }

    fn explain_sql(&self, sql: &str) -> Result<ExplainPlan, DbError> {
        let stmt = parse_sql(sql)?;
        Ok(explain_statement(&stmt))
    }

    fn trace_execute_session_sql(
        &self,
        session_id: SessionId,
        sql: &str,
    ) -> Result<ExecutionTrace, DbError> {
        let slices = split_sql_statements(sql);
        let mut events = Vec::with_capacity(slices.len());
        let mut stopped_on_error = false;

        for slice in slices {
            match parse_sql(&slice.sql) {
                Ok(stmt) => {
                    let mut warnings = statement_compat_warnings(&stmt);
                    let mut read_tables: Vec<String> =
                        collect_read_tables_tooling(&stmt).into_iter().collect();
                    let mut write_tables: Vec<String> =
                        collect_write_tables_tooling(&stmt).into_iter().collect();
                    read_tables.sort();
                    write_tables.sort();

                    let executor = super::StatementExecutorService { state: self.state.clone() };
                    match executor.execute_session(session_id, stmt) {
                        Ok(result) => {
                            let options = self.session_options(session_id)?;
                            let row_count = if options.nocount {
                                None
                            } else {
                                result.as_ref().map(|r| r.rows.len())
                            };
                            events.push(TraceStatementEvent {
                                index: slice.index,
                                sql: slice.sql,
                                normalized_sql: slice.normalized_sql,
                                span: slice.span,
                                status: "ok".to_string(),
                                warnings: std::mem::take(&mut warnings),
                                error: None,
                                row_count,
                                read_tables,
                                write_tables,
                            });
                        }
                        Err(err) => {
                            events.push(TraceStatementEvent {
                                index: slice.index,
                                sql: slice.sql,
                                normalized_sql: slice.normalized_sql,
                                span: slice.span,
                                status: "error".to_string(),
                                warnings,
                                error: Some(err.to_string()),
                                row_count: None,
                                read_tables,
                                write_tables,
                            });
                            stopped_on_error = true;
                            break;
                        }
                    }
                }
                Err(err) => {
                    let err_str: String = err.to_string();
                    events.push(TraceStatementEvent {
                        index: slice.index,
                        sql: slice.sql,
                        normalized_sql: slice.normalized_sql,
                        span: slice.span,
                        status: "unsupported".to_string(),
                        warnings: Vec::new(),
                        error: Some(err_str),
                        row_count: None,
                        read_tables: Vec::new(),
                        write_tables: Vec::new(),
                    });
                    stopped_on_error = true;
                    break;
                }
            }
        }
        Ok(ExecutionTrace {
            events,
            stopped_on_error,
        })
    }
}


impl<C, S> RandomSeed for super::SqlAnalyzerService<C, S>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static + Default,
    S: Storage + crate::storage::CheckpointableStorage + Serialize + DeserializeOwned + Clone + 'static + Default,
{
    fn set_session_seed(&self, session_id: SessionId, seed: u64) -> Result<(), DbError> {
        let session_mutex = self.state.sessions.get(&session_id)
            .ok_or_else(|| DbError::Execution(format!("session {} not found", session_id)))?;
        let mut session = session_mutex.lock();
        session.random_state = seed;
        Ok(())
    }
}

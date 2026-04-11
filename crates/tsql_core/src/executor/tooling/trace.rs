use super::slicing::SourceSpan;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceStatementEvent {
    pub index: usize,
    pub sql: String,
    pub normalized_sql: String,
    pub span: SourceSpan,
    pub status: String,
    pub warnings: Vec<String>,
    pub error: Option<String>,
    pub row_count: Option<usize>,
    pub read_tables: Vec<String>,
    pub write_tables: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionTrace {
    pub events: Vec<TraceStatementEvent>,
    pub stopped_on_error: bool,
}

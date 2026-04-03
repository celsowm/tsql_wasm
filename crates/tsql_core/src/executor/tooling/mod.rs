pub mod session_options;
pub mod compatibility;
pub mod explain;
pub mod trace;
pub mod formatting;
pub mod formatting_kind;
pub mod slicing;

pub use session_options::{SessionOptions, SetOptionApply, apply_set_option};
pub use compatibility::{CompatibilityReport, CompatibilityEntry, CompatibilityIssue, SupportStatus, analyze_sql_batch, collect_read_tables, collect_write_tables, statement_compat_warnings};
pub use explain::{ExplainPlan, ExplainOperator, explain_statement};
pub use trace::{ExecutionTrace, TraceStatementEvent};
pub use slicing::{StatementSlice, SourceSpan, split_sql_statements};
pub use formatting::{format_routine_definition, format_trigger_definition, format_view_definition};

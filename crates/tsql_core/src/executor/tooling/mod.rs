//! SQL execution tooling and utilities.
//!
//! This module groups cross-cutting execution utilities into three cohesive concerns:
//!
//! ## Formatting & Display
//! - [`formatting`] — SQL expression/statement pretty-printing (routine/trigger/view definitions)
//! - [`formatting_kind`] — Statement kind classification (SELECT, INSERT, etc.)
//! - [`object_name`] — Object name normalization and table reference helpers
//!
//! ## Diagnostics & Analysis
//! - [`explain`] — Query execution plan generation
//! - [`trace`] — Statement-level execution tracing
//! - [`slicing`] — SQL text slicing and source span tracking
//! - [`table_usage`] — Read/write table dependency collection
//!
//! ## Session Configuration
//! - [`session_options`] — SET option handling (ANSI_NULLS, QUOTED_IDENTIFIER, etc.)

// -- Formatting & Display --
pub mod formatting;
pub mod formatting_kind;
mod object_name;

// -- Diagnostics & Analysis --
pub mod explain;
pub mod trace;
pub mod slicing;
mod table_usage;

// -- Session Configuration --
pub mod session_options;

pub use session_options::{SessionOptions, SetOptionApply, apply_set_option};
pub(crate) use object_name::{normalize_object_name, normalize_table_ref, select_from_name};
pub(crate) use session_options::statement_option_warnings;
pub(crate) use table_usage::{collect_read_tables, collect_write_tables};
pub use explain::{ExplainPlan, ExplainOperator, explain_statement};
pub use trace::{ExecutionTrace, TraceStatementEvent};
pub use slicing::{StatementSlice, SourceSpan, split_sql_statements};
pub use formatting::{format_routine_definition, format_trigger_definition, format_view_definition};

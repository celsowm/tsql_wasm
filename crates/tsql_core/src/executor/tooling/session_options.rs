use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::ast::{SessionOption, SessionOptionValue, SetOptionStmt, Statement};
use crate::error::DbError;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionOptions {
    pub ansi_nulls: bool,
    pub quoted_identifier: bool,
    pub nocount: bool,
    pub xact_abort: bool,
    pub ansi_null_dflt_on: bool,
    pub ansi_padding: bool,
    pub ansi_warnings: bool,
    pub arithabort: bool,
    pub concat_null_yields_null: bool,
    pub cursor_close_on_commit: bool,
    pub implicit_transactions: bool,
    pub datefirst: i32,
    pub language: String,
    pub dateformat: String,
    pub rowcount: u64,
    pub textsize: i32,
    pub query_governor_cost_limit: i64,
    pub deadlock_priority: i32,
    pub lock_timeout_ms: i64,
    pub statistics_io: bool,
    pub statistics_time: bool,
    pub showplan_all: bool,
    #[serde(skip)]
    pub identity_insert: HashSet<String>,
}

impl Default for SessionOptions {
    fn default() -> Self {
        Self {
            ansi_nulls: true,
            quoted_identifier: true,
            nocount: false,
            xact_abort: false,
            ansi_null_dflt_on: true,
            ansi_padding: true,
            ansi_warnings: true,
            arithabort: true,
            concat_null_yields_null: true,
            cursor_close_on_commit: false,
            implicit_transactions: false,
            datefirst: 7,
            language: "us_english".to_string(),
            dateformat: "mdy".to_string(),
            rowcount: 0,
            textsize: 4096,
            query_governor_cost_limit: 0,
            deadlock_priority: 0,
            lock_timeout_ms: 0,
            statistics_io: false,
            statistics_time: false,
            showplan_all: false,
            identity_insert: HashSet::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SetOptionApply {
    pub warnings: Vec<String>,
}

pub fn apply_set_option(
    stmt: &SetOptionStmt,
    options: &mut SessionOptions,
) -> Result<SetOptionApply, DbError> {
    let mut warnings = Vec::new();
    match (&stmt.option, &stmt.value) {
        (SessionOption::AnsiNulls, SessionOptionValue::Bool(v)) => {
            options.ansi_nulls = *v;
        }
        (SessionOption::QuotedIdentifier, SessionOptionValue::Bool(v)) => {
            options.quoted_identifier = *v;
        }
        (SessionOption::NoCount, SessionOptionValue::Bool(v)) => {
            options.nocount = *v;
        }
        (SessionOption::XactAbort, SessionOptionValue::Bool(v)) => {
            options.xact_abort = *v;
        }
        (SessionOption::AnsiNullDfltOn, SessionOptionValue::Bool(v)) => {
            options.ansi_null_dflt_on = *v;
        }
        (SessionOption::AnsiPadding, SessionOptionValue::Bool(v)) => {
            options.ansi_padding = *v;
        }
        (SessionOption::AnsiWarnings, SessionOptionValue::Bool(v)) => {
            options.ansi_warnings = *v;
        }
        (SessionOption::ArithAbort, SessionOptionValue::Bool(v)) => {
            options.arithabort = *v;
        }
        (SessionOption::ConcatNullYieldsNull, SessionOptionValue::Bool(v)) => {
            options.concat_null_yields_null = *v;
        }
        (SessionOption::CursorCloseOnCommit, SessionOptionValue::Bool(v)) => {
            options.cursor_close_on_commit = *v;
        }
        (SessionOption::ImplicitTransactions, SessionOptionValue::Bool(v)) => {
            options.implicit_transactions = *v;
        }
        (SessionOption::DateFirst, SessionOptionValue::Int(v)) => {
            if !(1..=7).contains(v) {
                return Err(DbError::Execution(format!(
                    "The DATEFIRST value {} is outside the range of allowed values (1-7).",
                    v
                )));
            }
            options.datefirst = *v as i32;
        }
        (SessionOption::Language, SessionOptionValue::Text(v)) => {
            let profile = language_profile(v)
                .ok_or_else(|| DbError::Execution(format!("unsupported language '{}'", v)))?;
            options.language = profile.name.to_string();
            options.datefirst = profile.datefirst;
            options.dateformat = profile.dateformat.to_string();
        }
        (SessionOption::DateFormat, SessionOptionValue::Text(v)) => {
            options.dateformat = v.to_lowercase();
        }
        (SessionOption::LockTimeout, SessionOptionValue::Int(v)) => {
            options.lock_timeout_ms = *v;
        }
        (SessionOption::RowCount, SessionOptionValue::Int(v)) => {
            options.rowcount = if *v <= 0 { 0 } else { *v as u64 };
        }
        (SessionOption::TextSize, SessionOptionValue::Int(v)) => {
            options.textsize = if *v < 0 {
                0
            } else {
                (*v).min(i32::MAX as i64) as i32
            };
        }
        (SessionOption::QueryGovernorCostLimit, SessionOptionValue::Int(v)) => {
            options.query_governor_cost_limit = *v;
        }
        (SessionOption::DeadlockPriority, SessionOptionValue::Int(v)) => {
            options.deadlock_priority = (*v).clamp(-10, 10) as i32;
        }
        (SessionOption::DeadlockPriority, SessionOptionValue::Text(v)) => {
            let upper = v.to_ascii_uppercase();
            options.deadlock_priority = match upper.as_str() {
                "LOW" => -5,
                "NORMAL" => 0,
                "HIGH" => 5,
                _ => {
                    return Err(DbError::Execution(format!(
                        "unsupported deadlock priority '{}'",
                        v
                    )));
                }
            };
        }
        (SessionOption::StatisticsIo, SessionOptionValue::Bool(v)) => {
            options.statistics_io = *v;
        }
        (SessionOption::StatisticsTime, SessionOptionValue::Bool(v)) => {
            options.statistics_time = *v;
        }
        (SessionOption::ShowplanAll, SessionOptionValue::Bool(v)) => {
            options.showplan_all = *v;
        }
        (SessionOption::Unsupported(name), _) => {
            warnings.push(format!(
                "SET {} is accepted but not modeled; no session state change applied",
                name
            ));
        }
        _ => {
            warnings.push(
                "SET option value type mismatch; statement accepted with no state change"
                    .to_string(),
            );
        }
    }
    Ok(SetOptionApply { warnings })
}

pub fn statement_option_warnings(stmt: &Statement) -> Vec<String> {
    if let Statement::Session(crate::ast::SessionStatement::SetOption(opt)) = stmt {
        match (&opt.option, &opt.value) {
            (SessionOption::DateFirst, SessionOptionValue::Int(v)) if !(1..=7).contains(v) => {
                return vec![format!(
                    "DATEFIRST {} is outside the supported range 1..7",
                    v
                )]
            }
            _ => {}
        }
    }
    Vec::new()
}

#[derive(Debug, Clone, Copy)]
struct LanguageProfile {
    name: &'static str,
    datefirst: i32,
    dateformat: &'static str,
}

fn language_profile(name: &str) -> Option<LanguageProfile> {
    let lower = name.to_ascii_lowercase();
    match lower.as_str() {
        "us_english" => Some(LanguageProfile {
            name: "us_english",
            datefirst: 7,
            dateformat: "mdy",
        }),
        "english" => Some(LanguageProfile {
            name: "us_english",
            datefirst: 7,
            dateformat: "mdy",
        }),
        "british" => Some(LanguageProfile {
            name: "british",
            datefirst: 1,
            dateformat: "dmy",
        }),
        "french" => Some(LanguageProfile {
            name: "french",
            datefirst: 1,
            dateformat: "dmy",
        }),
        "german" => Some(LanguageProfile {
            name: "german",
            datefirst: 1,
            dateformat: "dmy",
        }),
        "italian" => Some(LanguageProfile {
            name: "italian",
            datefirst: 1,
            dateformat: "dmy",
        }),
        "spanish" => Some(LanguageProfile {
            name: "spanish",
            datefirst: 1,
            dateformat: "dmy",
        }),
        "portuguese" => Some(LanguageProfile {
            name: "portuguese",
            datefirst: 2,
            dateformat: "dmy",
        }),
        _ => None,
    }
}

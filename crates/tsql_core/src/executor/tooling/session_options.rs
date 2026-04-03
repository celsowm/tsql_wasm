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
    pub datefirst: i32,
    pub language: String,
    pub dateformat: String,
    pub lock_timeout_ms: i64,
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
            datefirst: 7,
            language: "us_english".to_string(),
            dateformat: "mdy".to_string(),
            lock_timeout_ms: 0,
            identity_insert: HashSet::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SetOptionApply {
    pub warnings: Vec<String>,
}

pub fn apply_set_option(stmt: &SetOptionStmt, options: &mut SessionOptions) -> Result<SetOptionApply, DbError> {
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
        (SessionOption::DateFirst, SessionOptionValue::Int(v)) => {
            if !(1..=7).contains(v) {
                return Err(DbError::Execution(
                    format!("The DATEFIRST value {} is outside the range of allowed values (1-7).", v)
                ));
            }
            options.datefirst = *v;
        }
        (SessionOption::Language, SessionOptionValue::Text(v)) => {
            options.language = v.clone();
            if !v.eq_ignore_ascii_case("us_english") {
                warnings.push(format!(
                    "SET LANGUAGE '{}' is accepted but only us_english behavior is modeled",
                    v
                ));
            }
        }
        (SessionOption::DateFormat, SessionOptionValue::Text(v)) => {
            options.dateformat = v.to_lowercase();
        }
        (SessionOption::LockTimeout, SessionOptionValue::Int(v)) => {
            options.lock_timeout_ms = *v as i64;
        }
        _ => {
            warnings.push("SET option value type mismatch; statement accepted with no state change".to_string());
        }
    }
    Ok(SetOptionApply { warnings })
}

pub fn statement_option_warnings(stmt: &Statement) -> Vec<String> {
    if let Statement::Session(crate::ast::SessionStatement::SetOption(opt)) = stmt {
        match (&opt.option, &opt.value) {
            (SessionOption::DateFirst, SessionOptionValue::Int(v))
                if !(1..=7).contains(v) =>
            {
                return vec![format!(
                    "DATEFIRST {} is outside the supported range 1..7",
                    v
                )]
            }
            (SessionOption::Language, SessionOptionValue::Text(v))
                if !v.eq_ignore_ascii_case("us_english") =>
            {
                return vec![format!(
                    "LANGUAGE '{}' is accepted, but only us_english behavior is modeled",
                    v
                )]
            }
            _ => {}
        }
    }
    Vec::new()
}

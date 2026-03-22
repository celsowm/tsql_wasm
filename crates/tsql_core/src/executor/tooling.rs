use serde::{Deserialize, Serialize};

use crate::ast::{ObjectName, SelectStmt, SessionOption, SessionOptionValue, SetOptionStmt, Statement, TableRef};
use crate::parser::parse_sql;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionOptions {
    pub ansi_nulls: bool,
    pub quoted_identifier: bool,
    pub nocount: bool,
    pub xact_abort: bool,
    pub datefirst: i32,
    pub language: String,
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
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceSpan {
    pub start_line: usize,
    pub start_col: usize,
    pub end_line: usize,
    pub end_col: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementSlice {
    pub index: usize,
    pub sql: String,
    pub normalized_sql: String,
    pub span: SourceSpan,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SupportStatus {
    Supported,
    Partial,
    Unsupported,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompatibilityIssue {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompatibilityEntry {
    pub index: usize,
    pub sql: String,
    pub normalized_sql: String,
    pub span: SourceSpan,
    pub status: SupportStatus,
    pub feature_tags: Vec<String>,
    pub issues: Vec<CompatibilityIssue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompatibilityReport {
    pub entries: Vec<CompatibilityEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExplainOperator {
    pub op: String,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExplainPlan {
    pub statement_kind: String,
    pub operators: Vec<ExplainOperator>,
    pub read_tables: Vec<String>,
    pub write_tables: Vec<String>,
}

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

#[derive(Debug, Clone)]
pub struct SetOptionApply {
    pub warnings: Vec<String>,
}

pub fn apply_set_option(stmt: &SetOptionStmt, options: &mut SessionOptions) -> SetOptionApply {
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
            options.datefirst = *v;
            if !(1..=7).contains(v) {
                warnings.push(format!(
                    "SET DATEFIRST {} is outside SQL Server range 1..7",
                    v
                ));
            }
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
        _ => {
            warnings.push("SET option value type mismatch; statement accepted with no state change".to_string());
        }
    }
    SetOptionApply { warnings }
}

pub fn analyze_sql_batch(sql: &str) -> CompatibilityReport {
    let slices = split_sql_statements(sql);
    let mut entries = Vec::with_capacity(slices.len());
    for slice in slices {
        match parse_sql(&slice.sql) {
            Ok(stmt) => {
                let mut status = SupportStatus::Supported;
                let mut issues = Vec::new();
                for warn in statement_compat_warnings(&stmt) {
                    status = SupportStatus::Partial;
                    issues.push(CompatibilityIssue {
                        code: "WARN_PARTIAL_MODEL".to_string(),
                        message: warn,
                    });
                }
                entries.push(CompatibilityEntry {
                    index: slice.index,
                    sql: slice.sql,
                    normalized_sql: slice.normalized_sql,
                    span: slice.span,
                    status,
                    feature_tags: feature_tags_for_statement(&stmt),
                    issues,
                });
            }
            Err(err) => entries.push(CompatibilityEntry {
                index: slice.index,
                sql: slice.sql,
                normalized_sql: slice.normalized_sql,
                span: slice.span,
                status: SupportStatus::Unsupported,
                feature_tags: vec!["unsupported".to_string()],
                issues: vec![CompatibilityIssue {
                    code: "ERR_UNSUPPORTED_STATEMENT".to_string(),
                    message: err.to_string(),
                }],
            }),
        }
    }
    CompatibilityReport { entries }
}

pub fn explain_statement(stmt: &Statement) -> ExplainPlan {
    let mut operators = Vec::new();
    let statement_kind = statement_kind(stmt).to_string();
    match stmt {
        Statement::Select(s) => {
            operators.push(ExplainOperator {
                op: "Scan".to_string(),
                detail: format!("from {}", select_from_name(s)),
            });
            if !s.joins.is_empty() {
                operators.push(ExplainOperator {
                    op: "Join".to_string(),
                    detail: format!("{} join(s)", s.joins.len()),
                });
            }
            if s.selection.is_some() {
                operators.push(ExplainOperator {
                    op: "Filter".to_string(),
                    detail: "WHERE predicate".to_string(),
                });
            }
            if !s.group_by.is_empty() {
                operators.push(ExplainOperator {
                    op: "Aggregate".to_string(),
                    detail: format!("GROUP BY {} expression(s)", s.group_by.len()),
                });
            }
            if !s.order_by.is_empty() {
                operators.push(ExplainOperator {
                    op: "Sort".to_string(),
                    detail: format!("ORDER BY {} expression(s)", s.order_by.len()),
                });
            }
            operators.push(ExplainOperator {
                op: "Project".to_string(),
                detail: format!("{} projected column(s)", s.projection.len()),
            });
        }
        Statement::Insert(i) => operators.push(ExplainOperator {
            op: "Insert".to_string(),
            detail: normalize_object_name(&i.table),
        }),
        Statement::Update(u) => operators.push(ExplainOperator {
            op: "Update".to_string(),
            detail: normalize_object_name(&u.table),
        }),
        Statement::Delete(d) => operators.push(ExplainOperator {
            op: "Delete".to_string(),
            detail: normalize_object_name(&d.table),
        }),
        Statement::CreateTable(c) => operators.push(ExplainOperator {
            op: "DDL".to_string(),
            detail: format!("CREATE TABLE {}", normalize_object_name(&c.name)),
        }),
        Statement::AlterTable(a) => operators.push(ExplainOperator {
            op: "DDL".to_string(),
            detail: format!("ALTER TABLE {}", normalize_object_name(&a.table)),
        }),
        _ => operators.push(ExplainOperator {
            op: "Statement".to_string(),
            detail: statement_kind.clone(),
        }),
    }

    let mut read_tables: Vec<String> = collect_read_tables(stmt).into_iter().collect();
    let mut write_tables: Vec<String> = collect_write_tables(stmt).into_iter().collect();
    read_tables.sort();
    write_tables.sort();

    ExplainPlan {
        statement_kind,
        operators,
        read_tables,
        write_tables,
    }
}

pub fn statement_compat_warnings(stmt: &Statement) -> Vec<String> {
    if let Statement::SetOption(opt) = stmt {
        match (&opt.option, &opt.value) {
            (SessionOption::DateFirst, SessionOptionValue::Int(v)) if !(1..=7).contains(v) => {
                return vec![format!(
                    "DATEFIRST {} outside SQL Server range 1..7 (accepted for compatibility)",
                    v
                )]
            }
            (SessionOption::Language, SessionOptionValue::Text(v))
                if !v.eq_ignore_ascii_case("us_english") =>
            {
                return vec![format!(
                    "LANGUAGE '{}' accepted, but only us_english behavior is modeled",
                    v
                )]
            }
            _ => {}
        }
    }
    Vec::new()
}

pub fn split_sql_statements(sql: &str) -> Vec<StatementSlice> {
    let mut out = Vec::new();
    let mut buf = String::new();
    let mut in_string = false;
    let mut paren_depth = 0usize;
    let mut block_depth = 0usize;
    let mut line = 1usize;
    let mut col = 1usize;
    let mut start_line = 1usize;
    let mut start_col = 1usize;
    let chars: Vec<char> = sql.chars().collect();
    let mut idx = 0usize;

    while idx < chars.len() {
        let ch = chars[idx];
        if buf.is_empty() && ch.is_whitespace() {
            advance_pos(ch, &mut line, &mut col);
            idx += 1;
            continue;
        }
        if buf.is_empty() {
            start_line = line;
            start_col = col;
        }

        if ch == '\'' {
            in_string = !in_string;
        } else if !in_string {
            if ch == '(' {
                paren_depth += 1;
            } else if ch == ')' {
                paren_depth = paren_depth.saturating_sub(1);
            }
        }

        if !in_string && paren_depth == 0 {
            if starts_keyword(&chars, idx, "BEGIN") && !starts_keyword(&chars, idx, "BEGIN TRAN") {
                block_depth += 1;
            } else if starts_keyword(&chars, idx, "END") && block_depth > 0 {
                block_depth -= 1;
            }
        }

        if ch == ';' && !in_string && paren_depth == 0 && block_depth == 0 {
            push_slice(&mut out, &buf, start_line, start_col, line, col);
            buf.clear();
            advance_pos(ch, &mut line, &mut col);
            idx += 1;
            continue;
        }

        buf.push(ch);
        advance_pos(ch, &mut line, &mut col);
        idx += 1;
    }
    push_slice(
        &mut out,
        &buf,
        start_line,
        start_col,
        line,
        col.saturating_sub(1),
    );

    for (i, item) in out.iter_mut().enumerate() {
        item.index = i;
    }
    out
}

fn push_slice(
    out: &mut Vec<StatementSlice>,
    buf: &str,
    start_line: usize,
    start_col: usize,
    end_line: usize,
    end_col: usize,
) {
    let trimmed = buf.trim();
    if trimmed.is_empty() {
        return;
    }
    out.push(StatementSlice {
        index: 0,
        sql: trimmed.to_string(),
        normalized_sql: normalize_sql(trimmed),
        span: SourceSpan {
            start_line,
            start_col,
            end_line,
            end_col,
        },
    });
}

fn normalize_sql(sql: &str) -> String {
    sql.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn advance_pos(ch: char, line: &mut usize, col: &mut usize) {
    if ch == '\n' {
        *line += 1;
        *col = 1;
    } else {
        *col += 1;
    }
}

fn starts_keyword(chars: &[char], idx: usize, kw: &str) -> bool {
    let target: Vec<char> = kw.chars().collect();
    if idx + target.len() > chars.len() {
        return false;
    }
    let mut got = String::new();
    for ch in &chars[idx..idx + target.len()] {
        got.push(ch.to_ascii_uppercase());
    }
    got == kw.to_ascii_uppercase()
}

fn statement_kind(stmt: &Statement) -> &'static str {
    match stmt {
        Statement::Select(_) => "SELECT",
        Statement::Insert(_) => "INSERT",
        Statement::Update(_) => "UPDATE",
        Statement::Delete(_) => "DELETE",
        Statement::CreateTable(_) => "CREATE_TABLE",
        Statement::CreateIndex(_) => "CREATE_INDEX",
        Statement::DropTable(_) => "DROP_TABLE",
        Statement::AlterTable(_) => "ALTER_TABLE",
        Statement::SetOption(_) => "SET_OPTION",
        Statement::Set(_) => "SET_VARIABLE",
        Statement::BeginTransaction(_) => "BEGIN_TRANSACTION",
        Statement::CommitTransaction => "COMMIT",
        Statement::RollbackTransaction(_) => "ROLLBACK",
        _ => "STATEMENT",
    }
}

fn feature_tags_for_statement(stmt: &Statement) -> Vec<String> {
    let mut tags = Vec::new();
    match stmt {
        Statement::SetOption(_) => tags.push("set-option".to_string()),
        Statement::Select(_) | Statement::SetOp(_) | Statement::WithCte(_) => {
            tags.push("query".to_string())
        }
        Statement::CreateTable(_)
        | Statement::AlterTable(_)
        | Statement::DropTable(_)
        | Statement::CreateSchema(_)
        | Statement::DropSchema(_)
        | Statement::CreateIndex(_)
        | Statement::DropIndex(_) => tags.push("ddl".to_string()),
        Statement::BeginTransaction(_)
        | Statement::CommitTransaction
        | Statement::RollbackTransaction(_)
        | Statement::SaveTransaction(_)
        | Statement::SetTransactionIsolationLevel(_) => tags.push("transaction".to_string()),
        Statement::CreateProcedure(_)
        | Statement::DropProcedure(_)
        | Statement::CreateFunction(_)
        | Statement::DropFunction(_)
        | Statement::ExecDynamic(_)
        | Statement::ExecProcedure(_)
        | Statement::SpExecuteSql(_)
        | Statement::If(_)
        | Statement::While(_)
        | Statement::BeginEnd(_)
        | Statement::Declare(_)
        | Statement::Set(_)
        | Statement::DeclareTableVar(_)
        | Statement::SelectAssign(_) => tags.push("procedural".to_string()),
        _ => {}
    }
    tags
}

pub fn collect_read_tables(stmt: &Statement) -> std::collections::HashSet<String> {
    let mut out = std::collections::HashSet::new();
    match stmt {
        Statement::Select(s) => collect_tables_from_select(s, &mut out),
        Statement::Update(s) => {
            out.insert(normalize_object_name(&s.table));
        }
        Statement::Delete(s) => {
            out.insert(normalize_object_name(&s.table));
        }
        Statement::SelectAssign(s) => {
            if let Some(from) = &s.from {
                out.insert(normalize_table_ref(from));
            }
            for join in &s.joins {
                out.insert(normalize_table_ref(&join.table));
            }
        }
        Statement::SetOp(s) => {
            out.extend(collect_read_tables(&s.left));
            out.extend(collect_read_tables(&s.right));
        }
        Statement::WithCte(s) => {
            for cte in &s.ctes {
                collect_tables_from_select(&cte.query, &mut out);
            }
            out.extend(collect_read_tables(&s.body));
        }
        _ => {}
    }
    out
}

pub fn collect_write_tables(stmt: &Statement) -> std::collections::HashSet<String> {
    let mut out = std::collections::HashSet::new();
    match stmt {
        Statement::Insert(s) => {
            out.insert(normalize_object_name(&s.table));
        }
        Statement::Update(s) => {
            out.insert(normalize_object_name(&s.table));
        }
        Statement::Delete(s) => {
            out.insert(normalize_object_name(&s.table));
        }
        Statement::CreateTable(s) => {
            out.insert(normalize_object_name(&s.name));
        }
        Statement::DropTable(s) => {
            out.insert(normalize_object_name(&s.name));
        }
        Statement::AlterTable(s) => {
            out.insert(normalize_object_name(&s.table));
        }
        Statement::TruncateTable(s) => {
            out.insert(normalize_object_name(&s.name));
        }
        _ => {}
    }
    out
}

fn collect_tables_from_select(stmt: &SelectStmt, out: &mut std::collections::HashSet<String>) {
    if let Some(from) = &stmt.from {
        out.insert(normalize_table_ref(from));
    }
    for join in &stmt.joins {
        out.insert(normalize_table_ref(&join.table));
    }
}

fn normalize_table_ref(table: &TableRef) -> String {
    normalize_object_name(&table.name)
}

fn normalize_object_name(name: &ObjectName) -> String {
    format!(
        "{}.{}",
        name.schema.as_deref().unwrap_or("dbo").to_uppercase(),
        name.name.to_uppercase()
    )
}

fn select_from_name(stmt: &SelectStmt) -> String {
    stmt.from
        .as_ref()
        .map(normalize_table_ref)
        .unwrap_or_else(|| "<none>".to_string())
}

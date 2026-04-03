use serde::{Deserialize, Serialize};

use crate::ast::{DdlStatement, DmlStatement, ObjectName, SelectStmt, Statement, TableRef};
use crate::parser::parse_sql;

use super::slicing::{split_sql_statements, SourceSpan};

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

pub fn statement_compat_warnings(stmt: &Statement) -> Vec<String> {
    if let Statement::Session(crate::ast::SessionStatement::SetOption(opt)) = stmt {
        match (&opt.option, &opt.value) {
            (crate::ast::SessionOption::DateFirst, crate::ast::SessionOptionValue::Int(v))
                if !(1..=7).contains(v) =>
            {
                return vec![format!(
                    "DATEFIRST {} outside SQL Server range 1..7 (accepted for compatibility)",
                    v
                )]
            }
            (crate::ast::SessionOption::Language, crate::ast::SessionOptionValue::Text(v))
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

pub(crate) fn feature_tags_for_statement(stmt: &Statement) -> Vec<String> {
    let mut tags = Vec::new();
    match stmt {
        Statement::Dml(DmlStatement::Select(_)) => tags.push("query".to_string()),
        Statement::Dml(DmlStatement::Insert(_))
        | Statement::Dml(DmlStatement::Update(_))
        | Statement::Dml(DmlStatement::Delete(_))
        | Statement::Dml(DmlStatement::Merge(_)) => tags.push("dml".to_string()),
        Statement::Ddl(DdlStatement::CreateTable(_))
        | Statement::Ddl(DdlStatement::DropView(_))
        | Statement::Ddl(DdlStatement::AlterTable(_))
        | Statement::Ddl(DdlStatement::DropTable(_))
        | Statement::Ddl(DdlStatement::CreateSchema(_))
        | Statement::Ddl(DdlStatement::DropSchema(_))
        | Statement::Ddl(DdlStatement::CreateIndex(_))
        | Statement::Ddl(DdlStatement::DropIndex(_))
        | Statement::Ddl(DdlStatement::TruncateTable(_))
        | Statement::Ddl(DdlStatement::CreateType(_))
        | Statement::Ddl(DdlStatement::DropType(_))
        | Statement::Ddl(DdlStatement::DropProcedure(_))
        | Statement::Ddl(DdlStatement::DropFunction(_))
        | Statement::Ddl(DdlStatement::DropTrigger(_)) => tags.push("ddl".to_string()),
        Statement::Transaction(_) => tags.push("transaction".to_string()),
        Statement::Procedural(crate::ast::ProceduralStatement::CreateProcedure(_))
        | Statement::Procedural(crate::ast::ProceduralStatement::CreateFunction(_))
        | Statement::Procedural(crate::ast::ProceduralStatement::CreateView(_))
        | Statement::Procedural(crate::ast::ProceduralStatement::CreateTrigger(_))
        | Statement::Procedural(crate::ast::ProceduralStatement::ExecDynamic(_))
        | Statement::Procedural(crate::ast::ProceduralStatement::ExecProcedure(_))
        | Statement::Procedural(crate::ast::ProceduralStatement::SpExecuteSql(_))
        | Statement::Procedural(crate::ast::ProceduralStatement::If(_))
        | Statement::Procedural(crate::ast::ProceduralStatement::While(_))
        | Statement::Procedural(crate::ast::ProceduralStatement::BeginEnd(_))
        | Statement::Procedural(crate::ast::ProceduralStatement::Declare(_))
        | Statement::Procedural(crate::ast::ProceduralStatement::Set(_))
        | Statement::Procedural(crate::ast::ProceduralStatement::DeclareTableVar(_))
        | Statement::Dml(DmlStatement::SelectAssign(_)) => tags.push("procedural".to_string()),
        _ => {}
    }
    tags
}

pub fn collect_read_tables(stmt: &Statement) -> std::collections::HashSet<String> {
    let mut out = std::collections::HashSet::new();
    match stmt {
        Statement::Dml(DmlStatement::Select(s)) => collect_tables_from_select(s, &mut out),
        Statement::Dml(DmlStatement::Update(s)) => {
            out.insert(normalize_object_name(&s.table));
        }
        Statement::Dml(DmlStatement::Delete(s)) => {
            out.insert(normalize_object_name(&s.table));
        }
        Statement::Dml(DmlStatement::SelectAssign(s)) => {
            if let Some(from) = &s.from {
                out.insert(normalize_table_ref(from));
            }
            for join in &s.joins {
                out.insert(normalize_table_ref(&join.table));
            }
        }
        Statement::Dml(DmlStatement::SetOp(s)) => {
            collect_tables_from_statement(&s.left, &mut out);
            collect_tables_from_statement(&s.right, &mut out);
        }
        Statement::WithCte(s) => {
            for cte in &s.ctes {
                collect_tables_from_statement(&cte.query, &mut out);
            }
            out.extend(collect_read_tables(&s.body));
        }
        _ => {}
    }
    out
}

pub fn collect_tables_from_statement(stmt: &Statement, out: &mut std::collections::HashSet<String>) {
    match stmt {
        Statement::Dml(DmlStatement::Select(s)) => collect_tables_from_select(s, out),
        Statement::Dml(DmlStatement::SetOp(s)) => {
            collect_tables_from_statement(&s.left, out);
            collect_tables_from_statement(&s.right, out);
        }
        Statement::WithCte(s) => {
            for cte in &s.ctes {
                collect_tables_from_statement(&cte.query, out);
            }
            collect_tables_from_statement(&s.body, out);
        }
        _ => {}
    }
}

pub fn collect_write_tables(stmt: &Statement) -> std::collections::HashSet<String> {
    let mut out = std::collections::HashSet::new();
    match stmt {
        Statement::Dml(DmlStatement::Insert(s)) => {
            out.insert(normalize_object_name(&s.table));
        }
        Statement::Dml(DmlStatement::Update(s)) => {
            out.insert(normalize_object_name(&s.table));
        }
        Statement::Dml(DmlStatement::Delete(s)) => {
            out.insert(normalize_object_name(&s.table));
        }
        Statement::Ddl(DdlStatement::CreateTable(s)) => {
            out.insert(normalize_object_name(&s.name));
        }
        Statement::Ddl(DdlStatement::DropTable(s)) => {
            out.insert(normalize_object_name(&s.name));
        }
        Statement::Ddl(DdlStatement::DropView(s)) => {
            out.insert(normalize_object_name(&s.name));
        }
        Statement::Ddl(DdlStatement::AlterTable(s)) => {
            out.insert(normalize_object_name(&s.table));
        }
        Statement::Ddl(DdlStatement::TruncateTable(s)) => {
            out.insert(normalize_object_name(&s.name));
        }
        _ => {}
    }
    out
}

pub fn collect_tables_from_select(stmt: &SelectStmt, out: &mut std::collections::HashSet<String>) {
    if let Some(from) = &stmt.from {
        out.insert(normalize_table_ref(from));
    }
    for join in &stmt.joins {
        out.insert(normalize_table_ref(&join.table));
    }
}

pub fn normalize_table_ref(table: &TableRef) -> String {
    match &table.factor {
        crate::ast::TableFactor::Named(o) => normalize_object_name(o),
        crate::ast::TableFactor::Derived(_) => "(DERIVED)".to_string(),
    }
}

pub fn normalize_object_name(name: &ObjectName) -> String {
    format!(
        "{}.{}",
        name.schema.as_deref().unwrap_or("dbo").to_uppercase(),
        name.name.to_uppercase()
    )
}

pub fn select_from_name(stmt: &SelectStmt) -> String {
    stmt.from
        .as_ref()
        .map(normalize_table_ref)
        .unwrap_or_else(|| "<none>".to_string())
}

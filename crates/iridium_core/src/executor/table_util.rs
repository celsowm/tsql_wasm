use std::collections::HashSet;

use crate::ast::{
    DdlStatement, DmlStatement, FromNode, ObjectName, SelectStmt, Statement, TableRef,
};

use super::string_norm::normalize_identifier;

/// Returns the set of tables that `stmt` reads from. The `_ => {}` catch-all is
/// intentional: statements not explicitly listed are assumed to have no read dependencies.
/// When adding new `Statement` variants that reference tables, update this function.
/// This centralised classification avoids scattering table-dependency logic across the codebase.
pub(crate) fn collect_read_tables(stmt: &Statement) -> HashSet<String> {
    let mut out = HashSet::new();
    match stmt {
        Statement::Dml(DmlStatement::Select(s)) => collect_tables_from_select(s, &mut out),
        Statement::Dml(DmlStatement::Update(s)) => {
            out.insert(normalize_table_name(&s.table));
        }
        Statement::Dml(DmlStatement::Delete(s)) => {
            out.insert(normalize_table_name(&s.table));
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
        Statement::WithCte(stmt) => {
            for cte in &stmt.ctes {
                collect_tables_from_statement(&cte.query, &mut out);
            }
            out.extend(collect_read_tables(&stmt.body));
        }
        Statement::Dml(DmlStatement::Merge(s)) => {
            if let Some(name) = s.target.name_as_object() {
                out.insert(normalize_table_name(name));
            }
            match &s.source {
                crate::ast::MergeSource::Table(tr) => {
                    out.insert(normalize_table_ref(tr));
                }
                crate::ast::MergeSource::Subquery(select, _) => {
                    collect_tables_from_select(select, &mut out);
                }
            }
        }
        _ => {}
    }
    out
}

fn collect_tables_from_statement(stmt: &Statement, out: &mut HashSet<String>) {
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

fn collect_tables_from_select(select: &SelectStmt, out: &mut HashSet<String>) {
    if let Some(from) = &select.from_clause {
        collect_tables_from_node(from, out);
    }
}

fn collect_tables_from_node(node: &FromNode, out: &mut HashSet<String>) {
    match node {
        FromNode::Table(table) => {
            out.insert(normalize_table_ref(table));
        }
        FromNode::Aliased { source, .. } => collect_tables_from_node(source, out),
        FromNode::Join { left, right, .. } => {
            collect_tables_from_node(left, out);
            collect_tables_from_node(right, out);
        }
    }
}

/// Returns the set of tables that `stmt` writes to. The `_ => {}` catch-all is
/// intentional: statements not explicitly listed are assumed to have no write dependencies.
/// When adding new `Statement` variants that modify tables, update this function.
/// This centralised classification avoids scattering table-dependency logic across the codebase.
pub(crate) fn collect_write_tables(stmt: &Statement) -> HashSet<String> {
    let mut out = HashSet::new();
    match stmt {
        Statement::Dml(DmlStatement::Insert(s)) => {
            out.insert(normalize_table_name(&s.table));
        }
        Statement::Dml(DmlStatement::Update(s)) => {
            out.insert(normalize_table_name(&s.table));
        }
        Statement::Dml(DmlStatement::Delete(s)) => {
            out.insert(normalize_table_name(&s.table));
        }
        Statement::Ddl(DdlStatement::CreateTable(s)) => {
            out.insert(normalize_identifier(&s.name.name));
        }
        Statement::Ddl(DdlStatement::DropTable(s)) => {
            out.insert(normalize_identifier(&s.name.name));
        }
        Statement::Ddl(DdlStatement::AlterTable(s)) => {
            out.insert(normalize_identifier(&s.table.name));
        }
        Statement::Ddl(DdlStatement::TruncateTable(s)) => {
            out.insert(normalize_identifier(&s.name.name));
        }
        Statement::Ddl(DdlStatement::CreateIndex(s)) => {
            out.insert(normalize_identifier(&s.table.name));
        }
        Statement::Ddl(DdlStatement::DropIndex(s)) => {
            out.insert(normalize_identifier(&s.table.name));
        }
        Statement::Ddl(DdlStatement::CreateSchema(_))
        | Statement::Ddl(DdlStatement::DropSchema(_)) => {
            out.insert("__GLOBAL__".to_string());
        }
        Statement::Ddl(DdlStatement::DropView(_))
        | Statement::Ddl(DdlStatement::DropProcedure(_))
        | Statement::Ddl(DdlStatement::DropFunction(_))
        | Statement::Ddl(DdlStatement::DropTrigger(_))
        | Statement::Ddl(DdlStatement::CreateType(_))
        | Statement::Ddl(DdlStatement::DropType(_))
        | Statement::Procedural(crate::ast::ProceduralStatement::CreateProcedure(_))
        | Statement::Procedural(crate::ast::ProceduralStatement::CreateFunction(_))
        | Statement::Procedural(crate::ast::ProceduralStatement::CreateView(_))
        | Statement::Procedural(crate::ast::ProceduralStatement::CreateTrigger(_)) => {
            out.insert("__GLOBAL__".to_string());
        }
        Statement::Dml(DmlStatement::Merge(s)) => {
            if let Some(name) = s.target.name_as_object() {
                out.insert(normalize_table_name(name));
            }
        }
        Statement::Dml(DmlStatement::BulkInsert(s)) => {
            out.insert(normalize_table_name(&s.table));
        }
        Statement::Dml(DmlStatement::InsertBulk(s)) => {
            out.insert(normalize_table_name(&s.table));
        }
        _ => {}
    }
    out
}

pub(crate) fn normalize_table_name(name: &ObjectName) -> String {
    normalize_identifier(&name.name)
}

pub(crate) fn normalize_table_ref(table_ref: &TableRef) -> String {
    table_ref
        .name_as_object()
        .map(normalize_table_name)
        .unwrap_or_else(|| "(DERIVED)".to_string())
}

pub(crate) fn is_transaction_statement(stmt: &Statement) -> bool {
    matches!(
        stmt,
        Statement::Transaction(_)
            | Statement::Session(crate::ast::SessionStatement::SetTransactionIsolationLevel(
                _
            ))
    )
}

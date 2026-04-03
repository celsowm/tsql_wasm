use std::collections::HashSet;

use crate::ast::{ObjectName, SelectStmt, Statement, TableRef};

pub(crate) fn collect_read_tables(stmt: &Statement) -> HashSet<String> {
    let mut out = HashSet::new();
    match stmt {
        Statement::Select(s) => collect_tables_from_select(s, &mut out),
        Statement::Update(s) => {
            out.insert(normalize_table_name(&s.table));
        }
        Statement::Delete(s) => {
            out.insert(normalize_table_name(&s.table));
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
            collect_tables_from_statement(&s.left, &mut out);
            collect_tables_from_statement(&s.right, &mut out);
        }
        Statement::WithCte(stmt) => {
            for cte in &stmt.ctes {
                collect_tables_from_statement(&cte.query, &mut out);
            }
            out.extend(collect_read_tables(&stmt.body));
        }
        Statement::Merge(s) => {
            out.insert(s.target.name.name().to_uppercase());
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
        Statement::Select(s) => collect_tables_from_select(s, out),
        Statement::SetOp(s) => {
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
    if let Some(from) = &select.from {
        out.insert(normalize_table_ref(from));
    }
    for join in &select.joins {
        out.insert(normalize_table_ref(&join.table));
    }
}

pub(crate) fn collect_write_tables(stmt: &Statement) -> HashSet<String> {
    let mut out = HashSet::new();
    match stmt {
        Statement::Insert(s) => {
            out.insert(normalize_table_name(&s.table));
        }
        Statement::Update(s) => {
            out.insert(normalize_table_name(&s.table));
        }
        Statement::Delete(s) => {
            out.insert(normalize_table_name(&s.table));
        }
        Statement::CreateTable(s) => {
            out.insert(s.name.name.to_uppercase());
        }
        Statement::DropTable(s) => {
            out.insert(s.name.name.to_uppercase());
        }
        Statement::AlterTable(s) => {
            out.insert(s.table.name.to_uppercase());
        }
        Statement::TruncateTable(s) => {
            out.insert(s.name.name.to_uppercase());
        }
        Statement::CreateIndex(s) => {
            out.insert(s.table.name.to_uppercase());
        }
        Statement::DropIndex(s) => {
            out.insert(s.table.name.to_uppercase());
        }
        Statement::CreateSchema(_) | Statement::DropSchema(_) => {
            out.insert("__GLOBAL__".to_string());
        }
        Statement::CreateProcedure(_) | Statement::DropProcedure(_) | Statement::CreateFunction(_) | Statement::DropFunction(_) | Statement::CreateTrigger(_) | Statement::DropTrigger(_) | Statement::CreateView(_) | Statement::DropView(_) => {
            out.insert("__GLOBAL__".to_string());
        }
        Statement::Merge(s) => {
            out.insert(s.target.name.name().to_uppercase());
        }
        _ => {}
    }
    out
}

pub(crate) fn normalize_table_name(name: &ObjectName) -> String {
    name.name.to_uppercase()
}

pub(crate) fn normalize_table_ref(table_ref: &TableRef) -> String {
    table_ref.name.name().to_uppercase()
}

pub(crate) fn is_transaction_statement(stmt: &Statement) -> bool {
    matches!(
        stmt,
        Statement::BeginTransaction(_)
            | Statement::CommitTransaction(_)
            | Statement::RollbackTransaction(_)
            | Statement::SaveTransaction(_)
            | Statement::SetTransactionIsolationLevel(_)
    )
}

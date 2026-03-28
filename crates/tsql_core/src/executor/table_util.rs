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
            | Statement::CommitTransaction
            | Statement::RollbackTransaction(_)
            | Statement::SaveTransaction(_)
            | Statement::SetTransactionIsolationLevel(_)
    )
}

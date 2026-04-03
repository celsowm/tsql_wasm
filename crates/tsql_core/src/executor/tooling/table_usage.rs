use std::collections::HashSet;

use crate::ast::{DdlStatement, DmlStatement, SelectStmt, Statement};

use super::object_name::{normalize_object_name, normalize_table_ref};

pub(crate) fn collect_read_tables(stmt: &Statement) -> HashSet<String> {
    let mut out = HashSet::new();
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

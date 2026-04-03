use serde::{Deserialize, Serialize};
use crate::ast::{DdlStatement, DmlStatement, Statement};
use super::formatting::{format_expr, format_join, format_select_columns};
use super::compatibility::{collect_read_tables, collect_write_tables, normalize_object_name, select_from_name};

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

pub fn explain_statement(stmt: &Statement) -> ExplainPlan {
    let mut operators = Vec::new();
    let statement_kind = super::formatting_kind::statement_kind(stmt).to_string();
    match stmt {
        Statement::Dml(DmlStatement::Select(s)) => {
            operators.push(ExplainOperator {
                op: "Scan".to_string(),
                detail: format!("from {}", select_from_name(s)),
            });
            for join in &s.joins {
                operators.push(ExplainOperator {
                    op: "Join".to_string(),
                    detail: format_join(join),
                });
            }
            if let Some(where_expr) = &s.selection {
                operators.push(ExplainOperator {
                    op: "Filter".to_string(),
                    detail: format!("WHERE {}", format_expr(where_expr)),
                });
            }
            if !s.group_by.is_empty() {
                let group_exprs: Vec<String> = s.group_by.iter().map(format_expr).collect();
                let mut detail = format!("GROUP BY {}", group_exprs.join(", "));
                if let Some(having) = &s.having {
                    detail = format!("{} HAVING {}", detail, format_expr(having));
                }
                operators.push(ExplainOperator {
                    op: "Aggregate".to_string(),
                    detail,
                });
            } else if s.having.is_some() {
                if let Some(having) = &s.having {
                    operators.push(ExplainOperator {
                        op: "Aggregate".to_string(),
                        detail: format!("HAVING {}", format_expr(having)),
                    });
                }
            }
            if !s.order_by.is_empty() {
                let order_exprs: Vec<String> = s.order_by.iter().map(|oe| {
                    let dir = if oe.asc { "" } else { " DESC" };
                    format!("{}{}", format_expr(&oe.expr), dir)
                }).collect();
                operators.push(ExplainOperator {
                    op: "Sort".to_string(),
                    detail: format!("ORDER BY {}", order_exprs.join(", ")),
                });
            }
            operators.push(ExplainOperator {
                op: "Project".to_string(),
                detail: format_select_columns(&s.projection),
            });
        }
        Statement::Dml(DmlStatement::Insert(i)) => operators.push(ExplainOperator {
            op: "Insert".to_string(),
            detail: normalize_object_name(&i.table),
        }),
        Statement::Dml(DmlStatement::Update(u)) => {
            let mut detail = normalize_object_name(&u.table);
            if !u.assignments.is_empty() {
                let assigns: Vec<String> = u.assignments.iter().map(|a| {
                    format!("{} = {}", a.column, format_expr(&a.expr))
                }).collect();
                detail = format!("{} SET {}", detail, assigns.join(", "));
            }
            operators.push(ExplainOperator {
                op: "Update".to_string(),
                detail,
            });
        }
        Statement::Dml(DmlStatement::Delete(d)) => operators.push(ExplainOperator {
            op: "Delete".to_string(),
            detail: format!("FROM {}", normalize_object_name(&d.table)),
        }),
        Statement::Ddl(DdlStatement::CreateTable(c)) => operators.push(ExplainOperator {
            op: "DDL".to_string(),
            detail: format!("CREATE TABLE {}", normalize_object_name(&c.name)),
        }),
        Statement::Ddl(DdlStatement::AlterTable(a)) => operators.push(ExplainOperator {
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

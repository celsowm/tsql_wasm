use std::cmp::Ordering;

use crate::ast::{Expr, OrderByExpr, SelectItem, TopSpec};
use crate::error::DbError;
use crate::types::Value;

use super::clock::Clock;
use super::context::ExecutionContext;
use super::evaluator::eval_constant_expr;
use super::model::JoinedRow;
use super::value_ops::compare_values;
use crate::catalog::Catalog;
use crate::storage::Storage;

pub fn eval_top_n(
    top: &TopSpec,
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<usize, DbError> {
    match eval_constant_expr(&top.value, ctx, catalog, storage, clock)? {
        Value::Int(v) => Ok(v.max(0) as usize),
        Value::BigInt(v) => Ok(v.max(0) as usize),
        _ => Err(DbError::Execution(
            "TOP currently requires an integer expression".into(),
        )),
    }
}

pub fn expand_projection_columns(
    projection: &[SelectItem],
    sample: Option<&JoinedRow>,
) -> Vec<String> {
    let mut columns = Vec::new();
    for item in projection {
        columns.extend(expand_projection_labels(item, sample));
    }
    columns
}

fn expand_projection_labels(item: &SelectItem, sample: Option<&JoinedRow>) -> Vec<String> {
    match &item.expr {
        Expr::Wildcard => {
            if let Some(row) = sample {
                row.iter()
                    .flat_map(|binding| binding.table.columns.iter().map(|c| c.name.clone()))
                    .collect()
            } else {
                vec!["*".to_string()]
            }
        }
        Expr::QualifiedWildcard(parts) => {
            if let Some(row) = sample {
                let table_name = parts.last().unwrap();
                row.iter()
                    .filter(|binding| {
                        binding.alias.eq_ignore_ascii_case(table_name)
                            || binding.table.name.eq_ignore_ascii_case(table_name)
                    })
                    .flat_map(|binding| binding.table.columns.iter().map(|c| c.name.clone()))
                    .collect()
            } else {
                vec![format!("{}.*", parts.join("."))]
            }
        }
        _ => vec![item.alias.clone().unwrap_or_else(|| expr_label(&item.expr))],
    }
}

pub fn expand_qualified_wildcard_values(row: &JoinedRow, table_name: &str) -> Vec<Value> {
    let mut values = Vec::new();
    for binding in row {
        if binding.alias.eq_ignore_ascii_case(table_name)
            || binding.table.name.eq_ignore_ascii_case(table_name)
        {
            for (idx, _) in binding.table.columns.iter().enumerate() {
                let value = binding
                    .row
                    .as_ref()
                    .map(|r| r.values[idx].clone())
                    .unwrap_or(Value::Null);
                values.push(value);
            }
        }
    }
    values
}

pub fn expand_wildcard_values(row: &JoinedRow) -> Vec<Value> {
    let mut values = Vec::new();
    for binding in row {
        for (idx, _) in binding.table.columns.iter().enumerate() {
            let value = binding
                .row
                .as_ref()
                .map(|r| r.values[idx].clone())
                .unwrap_or(Value::Null);
            values.push(value);
        }
    }
    values
}

pub fn expr_label(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(name) => name.clone(),
        Expr::QualifiedIdentifier(parts) => {
            parts.last().cloned().unwrap_or_else(|| "expr".to_string())
        }
        Expr::FunctionCall { name, .. } => name.clone(),
        Expr::Cast { .. } => "CAST".to_string(),
        Expr::TryCast { .. } => "TRY_CAST".to_string(),
        Expr::Convert { .. } => "CONVERT".to_string(),
        Expr::TryConvert { .. } => "TRY_CONVERT".to_string(),
        Expr::Wildcard => "*".to_string(),
        Expr::QualifiedWildcard(parts) => format!("{}.*", parts.join(".")),
        Expr::Case { .. } => "CASE".to_string(),
        Expr::InList { .. } => "IN".to_string(),
        Expr::Between { .. } => "BETWEEN".to_string(),
        Expr::Like { .. } => "LIKE".to_string(),
        Expr::Unary { expr: inner, .. } => expr_label(inner),
        Expr::Subquery(_) => "subquery".to_string(),
        Expr::Exists { .. } => "EXISTS".to_string(),
        Expr::InSubquery { .. } => "IN".to_string(),
        _ => "expr".to_string(),
    }
}

pub fn compare_projected_rows(
    a: &[Value],
    b: &[Value],
    columns: &[String],
    order_by: &[OrderByExpr],
) -> Result<Ordering, DbError> {
    for item in order_by {
        let idx = resolve_projected_order_index(columns, item)
            .ok_or_else(|| DbError::Semantic(format!("invalid column in ORDER BY: {}", expr_label(&item.expr))))?;
        let ord = compare_values(
            a.get(idx).unwrap_or(&Value::Null),
            b.get(idx).unwrap_or(&Value::Null),
        );
        if ord != Ordering::Equal {
            return Ok(if item.asc { ord } else { ord.reverse() });
        }
    }
    Ok(Ordering::Equal)
}

pub fn resolve_projected_order_index(columns: &[String], item: &OrderByExpr) -> Option<usize> {
    match &item.expr {
        Expr::Identifier(name) => columns.iter().position(|c| c.eq_ignore_ascii_case(name)),
        Expr::QualifiedIdentifier(parts) => parts
            .last()
            .and_then(|name| columns.iter().position(|c| c.eq_ignore_ascii_case(name))),
        _ => columns
            .iter()
            .position(|c| c.eq_ignore_ascii_case(&expr_label(&item.expr))),
    }
}

pub fn deduplicate_projected_rows(rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
    use std::collections::HashSet;
    let mut seen = HashSet::new();
    let mut result = Vec::new();
    for row in rows {
        if seen.insert(row.clone()) {
            result.push(row);
        }
    }
    result
}

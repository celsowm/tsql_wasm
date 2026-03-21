use std::cmp::Ordering;
use std::collections::HashMap;

use crate::ast::{Expr, JoinClause, JoinType, OrderByExpr, SelectItem, SelectStmt, TableRef};
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::executor::result::QueryResult;
use crate::storage::InMemoryStorage;
use crate::types::Value;

use super::clock::Clock;
use super::evaluator::{contains_aggregate, eval_constant_expr, eval_expr, eval_predicate};
use super::model::{BoundTable, ContextTable, Group, JoinedRow};
use super::value_ops::{compare_values, value_key};

pub(crate) struct QueryExecutor<'a> {
    pub(crate) catalog: &'a Catalog,
    pub(crate) storage: &'a InMemoryStorage,
    pub(crate) clock: &'a dyn Clock,
}

impl<'a> QueryExecutor<'a> {
    pub(crate) fn execute_select(&self, stmt: SelectStmt) -> Result<QueryResult, DbError> {
        let mut rows = self.build_joined_rows(&stmt.from, &stmt.joins)?;

        if let Some(selection) = &stmt.selection {
            rows = rows
                .into_iter()
                .filter(|row| eval_predicate(selection, row, self.clock).unwrap_or(false))
                .collect();
        }

        let has_aggregate = stmt
            .projection
            .iter()
            .any(|item| contains_aggregate(&item.expr));
        if has_aggregate || !stmt.group_by.is_empty() {
            return self.execute_grouped_select(stmt, rows);
        }

        if !stmt.order_by.is_empty() {
            rows.sort_by(|a, b| compare_joined_rows(a, b, &stmt.order_by, self.clock));
        }

        if let Some(top) = &stmt.top {
            let top_n = match eval_constant_expr(&top.value, self.clock)? {
                Value::Int(v) => v.max(0) as usize,
                Value::BigInt(v) => v.max(0) as usize,
                _ => {
                    return Err(DbError::Execution(
                        "TOP currently requires an integer expression".into(),
                    ))
                }
            };
            rows.truncate(top_n);
        }

        let columns = expand_projection_columns(&stmt.projection, rows.get(0));
        let mut out_rows = Vec::new();

        for row in &rows {
            let mut out = Vec::new();
            for item in &stmt.projection {
                match &item.expr {
                    Expr::Wildcard => out.extend(
                        expand_wildcard_values(row)
                            .into_iter()
                            .map(|v| v.to_json()),
                    ),
                    expr => out.push(eval_expr(expr, row, self.clock)?.to_json()),
                }
            }
            out_rows.push(out);
        }

        Ok(QueryResult {
            columns,
            rows: out_rows,
        })
    }

    fn execute_grouped_select(&self, stmt: SelectStmt, rows: Vec<JoinedRow>) -> Result<QueryResult, DbError> {
        let groups = self.build_groups(&stmt, rows)?;
        let columns = stmt
            .projection
            .iter()
            .flat_map(|item| expand_projection_labels(item, groups.get(0).and_then(|g| g.rows.get(0))))
            .collect::<Vec<_>>();

        let mut result_rows = groups
            .iter()
            .map(|group| project_group_row(&stmt.projection, group, self.clock))
            .collect::<Result<Vec<_>, _>>()?;

        if stmt.group_by.is_empty() && result_rows.is_empty() {
            result_rows.push(project_group_row(&stmt.projection, &Group::default(), self.clock)?);
        }

        if !stmt.order_by.is_empty() {
            result_rows.sort_by(|a, b| compare_projected_rows(a, b, &columns, &stmt.order_by));
        }

        if let Some(top) = &stmt.top {
            let top_n = match eval_constant_expr(&top.value, self.clock)? {
                Value::Int(v) => v.max(0) as usize,
                Value::BigInt(v) => v.max(0) as usize,
                _ => {
                    return Err(DbError::Execution(
                        "TOP currently requires an integer expression".into(),
                    ))
                }
            };
            result_rows.truncate(top_n);
        }

        Ok(QueryResult {
            columns,
            rows: result_rows
                .into_iter()
                .map(|row| row.into_iter().map(|v| v.to_json()).collect())
                .collect(),
        })
    }

    fn build_groups(&self, stmt: &SelectStmt, rows: Vec<JoinedRow>) -> Result<Vec<Group>, DbError> {
        let mut groups = Vec::new();

        if stmt.group_by.is_empty() {
            groups.push(Group { rows });
            return Ok(groups);
        }

        let mut map: HashMap<String, usize> = HashMap::new();
        for row in rows {
            let key_values = stmt
                .group_by
                .iter()
                .map(|expr| eval_expr(expr, &row, self.clock))
                .collect::<Result<Vec<_>, _>>()?;
            let key = key_values.iter().map(value_key).collect::<Vec<_>>().join("|");
            if let Some(idx) = map.get(&key).copied() {
                groups[idx].rows.push(row);
            } else {
                let idx = groups.len();
                map.insert(key, idx);
                groups.push(Group { rows: vec![row] });
            }
        }

        Ok(groups)
    }

    fn build_joined_rows(&self, from: &TableRef, joins: &[JoinClause]) -> Result<Vec<JoinedRow>, DbError> {
        let base = self.resolve_table_ref(from)?;
        let mut rows = self.bind_table_rows(&base)?;

        for join in joins {
            let right = self.resolve_table_ref(&join.table)?;
            let right_rows = self.bind_table_rows(&right)?;
            let mut next_rows = Vec::new();

            for left_row in rows {
                let mut matched = false;
                for right_row in &right_rows {
                    let mut candidate = left_row.clone();
                    candidate.extend(right_row.clone());
                    if eval_predicate(&join.on, &candidate, self.clock)? {
                        matched = true;
                        next_rows.push(candidate);
                    }
                }

                if !matched && join.join_type == JoinType::Left {
                    let mut candidate = left_row.clone();
                    candidate.push(ContextTable {
                        table: right.table.clone(),
                        alias: right.alias.clone(),
                        row: None,
                    });
                    next_rows.push(candidate);
                }
            }
            rows = next_rows;
        }

        Ok(rows)
    }

    fn resolve_table_ref(&self, tref: &TableRef) -> Result<BoundTable, DbError> {
        let schema = tref.name.schema_or_dbo();
        let table = self
            .catalog
            .find_table(schema, &tref.name.name)
            .ok_or_else(|| DbError::Semantic(format!("table '{}.{}' not found", schema, tref.name.name)))?
            .clone();

        Ok(BoundTable {
            alias: tref.alias.clone().unwrap_or_else(|| table.name.clone()),
            table,
        })
    }

    fn bind_table_rows(&self, bound: &BoundTable) -> Result<Vec<JoinedRow>, DbError> {
        let stored_rows = self
            .storage
            .tables
            .get(&bound.table.id)
            .ok_or_else(|| DbError::Storage("table storage not found".into()))?;

        Ok(stored_rows
            .iter()
            .filter(|r| !r.deleted)
            .map(|row| {
                vec![ContextTable {
                    table: bound.table.clone(),
                    alias: bound.alias.clone(),
                    row: Some(row.clone()),
                }]
            })
            .collect())
    }
}

fn expand_projection_columns(projection: &[SelectItem], sample: Option<&JoinedRow>) -> Vec<String> {
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
        _ => vec![projection_label(item, expr_label(&item.expr))],
    }
}

fn expand_wildcard_values(row: &JoinedRow) -> Vec<Value> {
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

fn projection_label(item: &SelectItem, fallback: String) -> String {
    item.alias.clone().unwrap_or(fallback)
}

fn expr_label(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(name) => name.clone(),
        Expr::QualifiedIdentifier(parts) => {
            parts.last().cloned().unwrap_or_else(|| "expr".to_string())
        }
        Expr::FunctionCall { name, .. } => name.clone(),
        Expr::Cast { .. } => "CAST".to_string(),
        Expr::Convert { .. } => "CONVERT".to_string(),
        Expr::Wildcard => "*".to_string(),
        _ => "expr".to_string(),
    }
}

fn project_group_row(projection: &[SelectItem], group: &Group, clock: &dyn Clock) -> Result<Vec<Value>, DbError> {
    let mut out = Vec::new();
    let sample_row = group.rows.get(0);

    for item in projection {
        match &item.expr {
            Expr::Wildcard => {
                if let Some(row) = sample_row {
                    out.extend(expand_wildcard_values(row));
                }
            }
            Expr::FunctionCall { name, args } if name.eq_ignore_ascii_case("COUNT") => {
                let count = if args
                    .first()
                    .map(|a| matches!(a, Expr::Wildcard))
                    .unwrap_or(false)
                {
                    group.rows.len() as i64
                } else if let Some(expr) = args.first() {
                    group
                        .rows
                        .iter()
                        .filter_map(|row| eval_expr(expr, row, clock).ok())
                        .filter(|v| !v.is_null())
                        .count() as i64
                } else {
                    group.rows.len() as i64
                };
                out.push(Value::BigInt(count));
            }
            expr => {
                if let Some(row) = sample_row {
                    out.push(eval_expr(expr, row, clock)?);
                } else {
                    out.push(Value::Null);
                }
            }
        }
    }

    Ok(out)
}

fn compare_joined_rows(a: &JoinedRow, b: &JoinedRow, order_by: &[OrderByExpr], clock: &dyn Clock) -> Ordering {
    for item in order_by {
        let av = eval_expr(&item.expr, a, clock).unwrap_or(Value::Null);
        let bv = eval_expr(&item.expr, b, clock).unwrap_or(Value::Null);
        let ord = compare_values(&av, &bv);
        if ord != Ordering::Equal {
            return if item.desc { ord.reverse() } else { ord };
        }
    }
    Ordering::Equal
}

fn compare_projected_rows(a: &[Value], b: &[Value], columns: &[String], order_by: &[OrderByExpr]) -> Ordering {
    for item in order_by {
        let idx = resolve_projected_order_index(columns, item).unwrap_or(0);
        let ord = compare_values(
            a.get(idx).unwrap_or(&Value::Null),
            b.get(idx).unwrap_or(&Value::Null),
        );
        if ord != Ordering::Equal {
            return if item.desc { ord.reverse() } else { ord };
        }
    }
    Ordering::Equal
}

fn resolve_projected_order_index(columns: &[String], item: &OrderByExpr) -> Option<usize> {
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

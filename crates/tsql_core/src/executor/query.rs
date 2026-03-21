use std::cmp::Ordering;
use std::collections::HashMap;

use crate::ast::{Expr, JoinClause, JoinType, OrderByExpr, SelectItem, SelectStmt, TableRef};
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::executor::result::QueryResult;
use crate::storage::InMemoryStorage;
use crate::types::Value;

use super::clock::Clock;
use super::evaluator::{
    contains_aggregate, eval_binary, eval_constant_expr, eval_expr, eval_predicate,
};
use super::model::{BoundTable, ContextTable, Group, JoinedRow};
use super::value_ops::{compare_values, truthy, value_key};

pub(crate) struct QueryExecutor<'a> {
    pub(crate) catalog: &'a Catalog,
    pub(crate) storage: &'a InMemoryStorage,
    pub(crate) clock: &'a dyn Clock,
}

impl<'a> QueryExecutor<'a> {
    pub(crate) fn execute_select(&self, stmt: SelectStmt) -> Result<QueryResult, DbError> {
        if stmt.from.is_none() {
            return self.execute_expression_select(&stmt);
        }

        let mut rows = self.build_joined_rows(stmt.from.as_ref().unwrap(), &stmt.joins)?;

        if let Some(selection) = &stmt.selection {
            rows.retain(|row| eval_predicate(selection, row, self.clock).unwrap_or(false));
        }

        let has_aggregate = stmt
            .projection
            .iter()
            .any(|item| contains_aggregate(&item.expr));
        if has_aggregate || !stmt.group_by.is_empty() {
            return self.execute_grouped_select(stmt, rows);
        }

        self.execute_flat_select(stmt, rows)
    }

    fn execute_expression_select(&self, stmt: &SelectStmt) -> Result<QueryResult, DbError> {
        let empty_row: JoinedRow = vec![];
        let columns = expand_projection_columns(&stmt.projection, Some(&empty_row));
        let mut out = Vec::new();
        for item in &stmt.projection {
            match &item.expr {
                Expr::Wildcard => {
                    return Err(DbError::Execution("wildcard requires a FROM clause".into()))
                }
                expr => out.push(eval_expr(expr, &empty_row, self.clock)?.to_json()),
            }
        }
        Ok(QueryResult {
            columns,
            rows: vec![out],
        })
    }

    fn execute_flat_select(
        &self,
        stmt: SelectStmt,
        mut rows: Vec<JoinedRow>,
    ) -> Result<QueryResult, DbError> {
        if !stmt.order_by.is_empty() {
            rows.sort_by(|a, b| compare_joined_rows(a, b, &stmt.order_by, self.clock));
        }

        if let Some(top) = &stmt.top {
            let top_n = eval_top_n(top, self.clock)?;
            rows.truncate(top_n);
        }

        let columns = expand_projection_columns(&stmt.projection, rows.first());
        let out_rows = project_flat_rows(&stmt.projection, &rows, self.clock);

        Ok(QueryResult {
            columns,
            rows: out_rows
                .into_iter()
                .map(|row| row.into_iter().map(|v| v.to_json()).collect())
                .collect(),
        })
    }

    fn execute_grouped_select(
        &self,
        stmt: SelectStmt,
        rows: Vec<JoinedRow>,
    ) -> Result<QueryResult, DbError> {
        let groups = self.build_groups(&stmt, rows)?;

        let mut filtered_groups = groups;
        if let Some(having_expr) = &stmt.having {
            filtered_groups.retain(|group| {
                match project_group_row(&stmt.projection, group, self.clock) {
                    Ok(row_values) => {
                        eval_having_predicate(having_expr, &stmt.projection, &row_values)
                    }
                    Err(_) => false,
                }
            });
        }

        let columns = stmt
            .projection
            .iter()
            .flat_map(|item| {
                expand_projection_labels(item, filtered_groups.first().and_then(|g| g.rows.first()))
            })
            .collect::<Vec<_>>();

        let mut result_rows = filtered_groups
            .iter()
            .map(|group| project_group_row(&stmt.projection, group, self.clock))
            .collect::<Result<Vec<_>, _>>()?;

        if stmt.group_by.is_empty() && result_rows.is_empty() {
            result_rows.push(project_group_row(
                &stmt.projection,
                &Group::default(),
                self.clock,
            )?);
        }

        if !stmt.order_by.is_empty() {
            result_rows.sort_by(|a, b| compare_projected_rows(a, b, &columns, &stmt.order_by));
        }

        if let Some(top) = &stmt.top {
            let top_n = eval_top_n(top, self.clock)?;
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
            let key = key_values
                .iter()
                .map(value_key)
                .collect::<Vec<_>>()
                .join("|");
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

    fn build_joined_rows(
        &self,
        from: &TableRef,
        joins: &[JoinClause],
    ) -> Result<Vec<JoinedRow>, DbError> {
        let base = self.resolve_table_ref(from)?;
        let mut rows = self.bind_table_rows(&base)?;

        for join in joins {
            let right = self.resolve_table_ref(&join.table)?;
            let right_rows = self.bind_table_rows(&right)?;
            rows = apply_join(rows, right_rows, right, join, self.clock)?;
        }

        Ok(rows)
    }

    fn resolve_table_ref(&self, tref: &TableRef) -> Result<BoundTable, DbError> {
        let schema = tref.name.schema_or_dbo();
        let table = self
            .catalog
            .find_table(schema, &tref.name.name)
            .ok_or_else(|| {
                DbError::Semantic(format!("table '{}.{}' not found", schema, tref.name.name))
            })?
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

// ─── Join ────────────────────────────────────────────────────────────────

fn apply_join(
    rows: Vec<JoinedRow>,
    right_rows: Vec<JoinedRow>,
    right: BoundTable,
    join: &JoinClause,
    clock: &dyn Clock,
) -> Result<Vec<JoinedRow>, DbError> {
    let mut next_rows = Vec::new();

    for left_row in rows {
        let mut matched = false;
        for right_row in &right_rows {
            let mut candidate = left_row.clone();
            candidate.extend(right_row.clone());
            if eval_predicate(&join.on, &candidate, clock)? {
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

    Ok(next_rows)
}

// ─── TOP ─────────────────────────────────────────────────────────────────

fn eval_top_n(top: &crate::ast::TopSpec, clock: &dyn Clock) -> Result<usize, DbError> {
    match eval_constant_expr(&top.value, clock)? {
        Value::Int(v) => Ok(v.max(0) as usize),
        Value::BigInt(v) => Ok(v.max(0) as usize),
        _ => Err(DbError::Execution(
            "TOP currently requires an integer expression".into(),
        )),
    }
}

// ─── Flat projection ────────────────────────────────────────────────────

fn project_flat_rows(
    projection: &[SelectItem],
    rows: &[JoinedRow],
    clock: &dyn Clock,
) -> Vec<Vec<Value>> {
    rows.iter()
        .map(|row| {
            let mut out = Vec::new();
            for item in projection {
                match &item.expr {
                    Expr::Wildcard => out.extend(expand_wildcard_values(row)),
                    expr => out.push(eval_expr(expr, row, clock).unwrap_or(Value::Null)),
                }
            }
            out
        })
        .collect()
}

// ─── Projection utilities ───────────────────────────────────────────────

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
        _ => vec![item.alias.clone().unwrap_or_else(|| expr_label(&item.expr))],
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

// ─── Aggregate helpers ──────────────────────────────────────────────────

fn is_aggregate_function(name: &str) -> bool {
    matches!(
        name.to_uppercase().as_str(),
        "COUNT" | "SUM" | "AVG" | "MIN" | "MAX"
    )
}

fn lookup_aggregate_in_projection<'a>(
    name: &str,
    args: &[Expr],
    projection: &'a [SelectItem],
) -> Option<(usize, &'a SelectItem)> {
    projection.iter().enumerate().find(|(_, item)| {
        if let Expr::FunctionCall {
            name: pname,
            args: pargs,
        } = &item.expr
        {
            pname.eq_ignore_ascii_case(name) && pargs == args
        } else {
            false
        }
    })
}

fn collect_group_values<'a>(
    expr: &'a Expr,
    group: &'a Group,
    clock: &'a dyn Clock,
) -> impl Iterator<Item = Value> + 'a {
    group
        .rows
        .iter()
        .filter_map(move |row| eval_expr(expr, row, clock).ok())
        .filter(|v| !v.is_null())
}

fn eval_aggregate_count(args: &[Expr], group: &Group, clock: &dyn Clock) -> Value {
    let count = if args.first().is_some_and(|a| matches!(a, Expr::Wildcard)) {
        group.rows.len() as i64
    } else if let Some(expr) = args.first() {
        collect_group_values(expr, group, clock).count() as i64
    } else {
        group.rows.len() as i64
    };
    Value::BigInt(count)
}

fn eval_aggregate_sum(args: &[Expr], group: &Group, clock: &dyn Clock) -> Result<Value, DbError> {
    let expr = args
        .first()
        .ok_or_else(|| DbError::Execution("SUM requires 1 argument".into()))?;
    let mut sum_i64: i64 = 0;
    let mut sum_f64: f64 = 0.0;
    let mut has_values = false;
    let mut is_decimal = false;

    for val in collect_group_values(expr, group, clock) {
        has_values = true;
        match &val {
            Value::Decimal(raw, scale) => {
                is_decimal = true;
                let divisor = 10i128.pow(*scale as u32);
                sum_f64 += (*raw as f64) / (divisor as f64);
            }
            Value::TinyInt(v) => sum_i64 += *v as i64,
            Value::SmallInt(v) => sum_i64 += *v as i64,
            Value::Int(v) => sum_i64 += *v as i64,
            Value::BigInt(v) => sum_i64 += *v,
            _ => return Err(DbError::Execution("SUM requires numeric argument".into())),
        }
    }

    if !has_values {
        return Ok(Value::Null);
    }
    if is_decimal {
        Ok(Value::VarChar(sum_f64.to_string()))
    } else {
        Ok(Value::BigInt(sum_i64))
    }
}

fn eval_aggregate_avg(args: &[Expr], group: &Group, clock: &dyn Clock) -> Result<Value, DbError> {
    let expr = args
        .first()
        .ok_or_else(|| DbError::Execution("AVG requires 1 argument".into()))?;
    let mut sum: f64 = 0.0;
    let mut count: i64 = 0;

    for val in collect_group_values(expr, group, clock) {
        count += 1;
        match &val {
            Value::Decimal(raw, scale) => {
                let divisor = 10i128.pow(*scale as u32);
                sum += (*raw as f64) / (divisor as f64);
            }
            Value::TinyInt(v) => sum += *v as f64,
            Value::SmallInt(v) => sum += *v as f64,
            Value::Int(v) => sum += *v as f64,
            Value::BigInt(v) => sum += *v as f64,
            _ => return Err(DbError::Execution("AVG requires numeric argument".into())),
        }
    }

    if count == 0 {
        return Ok(Value::Null);
    }
    Ok(Value::VarChar((sum / count as f64).to_string()))
}

fn eval_aggregate_min(args: &[Expr], group: &Group, clock: &dyn Clock) -> Result<Value, DbError> {
    let expr = args
        .first()
        .ok_or_else(|| DbError::Execution("MIN requires 1 argument".into()))?;
    let mut min_val: Option<Value> = None;

    for val in collect_group_values(expr, group, clock) {
        match &min_val {
            None => min_val = Some(val),
            Some(current) => {
                if compare_values(&val, current) == Ordering::Less {
                    min_val = Some(val);
                }
            }
        }
    }

    Ok(min_val.unwrap_or(Value::Null))
}

fn eval_aggregate_max(args: &[Expr], group: &Group, clock: &dyn Clock) -> Result<Value, DbError> {
    let expr = args
        .first()
        .ok_or_else(|| DbError::Execution("MAX requires 1 argument".into()))?;
    let mut max_val: Option<Value> = None;

    for val in collect_group_values(expr, group, clock) {
        match &max_val {
            None => max_val = Some(val),
            Some(current) => {
                if compare_values(&val, current) == Ordering::Greater {
                    max_val = Some(val);
                }
            }
        }
    }

    Ok(max_val.unwrap_or(Value::Null))
}

// ─── Grouped projection ─────────────────────────────────────────────────

fn project_group_row(
    projection: &[SelectItem],
    group: &Group,
    clock: &dyn Clock,
) -> Result<Vec<Value>, DbError> {
    let mut out = Vec::new();
    let sample_row = group.rows.first();

    for item in projection {
        match &item.expr {
            Expr::Wildcard => {
                if let Some(row) = sample_row {
                    out.extend(expand_wildcard_values(row));
                }
            }
            Expr::FunctionCall { name, args } if name.eq_ignore_ascii_case("COUNT") => {
                out.push(eval_aggregate_count(args, group, clock));
            }
            Expr::FunctionCall { name, args } if is_aggregate_function(name) => {
                let val = match name.to_uppercase().as_str() {
                    "SUM" => eval_aggregate_sum(args, group, clock)?,
                    "AVG" => eval_aggregate_avg(args, group, clock)?,
                    "MIN" => eval_aggregate_min(args, group, clock)?,
                    "MAX" => eval_aggregate_max(args, group, clock)?,
                    _ => Value::Null,
                };
                out.push(val);
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

// ─── HAVING evaluation ──────────────────────────────────────────────────

fn eval_having_predicate(
    expr: &Expr,
    projection: &[SelectItem],
    projected_values: &[Value],
) -> bool {
    match expr {
        Expr::FunctionCall { name, args } if is_aggregate_function(name) => {
            lookup_aggregate_in_projection(name, args, projection)
                .and_then(|(i, _)| projected_values.get(i))
                .is_some_and(|val| !val.is_null() && truthy(val))
        }
        Expr::Binary { left, op, right } => {
            let lv = resolve_having_value(left, projection, projected_values);
            let rv = resolve_having_value(right, projection, projected_values);
            matches!(eval_binary(op, lv, rv), Ok(Value::Bit(true)))
        }
        _ => false,
    }
}

fn resolve_having_value(
    expr: &Expr,
    projection: &[SelectItem],
    projected_values: &[Value],
) -> Value {
    match expr {
        Expr::FunctionCall { name, args } if is_aggregate_function(name) => {
            lookup_aggregate_in_projection(name, args, projection)
                .and_then(|(i, _)| projected_values.get(i))
                .cloned()
                .unwrap_or(Value::Null)
        }
        Expr::Integer(v) => {
            if *v >= i32::MIN as i64 && *v <= i32::MAX as i64 {
                Value::Int(*v as i32)
            } else {
                Value::BigInt(*v)
            }
        }
        _ => Value::Null,
    }
}

// ─── Ordering ───────────────────────────────────────────────────────────

fn compare_joined_rows(
    a: &JoinedRow,
    b: &JoinedRow,
    order_by: &[OrderByExpr],
    clock: &dyn Clock,
) -> Ordering {
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

fn compare_projected_rows(
    a: &[Value],
    b: &[Value],
    columns: &[String],
    order_by: &[OrderByExpr],
) -> Ordering {
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

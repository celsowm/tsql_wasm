use std::cmp::Ordering;
use crate::ast::{
    Expr, JoinClause, JoinType, OrderByExpr, SelectItem, SelectStmt, TopSpec,
};
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::storage::Storage;
use crate::types::Value;

use super::clock::Clock;
use super::context::ExecutionContext;
use super::evaluator::{eval_constant_expr, eval_expr, compare_values, value_key, eval_predicate};
use super::model::{BoundTable, ContextTable, JoinedRow};

pub struct QueryExecutor<'a> {
    pub catalog: &'a dyn Catalog,
    pub storage: &'a dyn Storage,
    pub clock: &'a dyn Clock,
}

#[derive(Debug, Clone)]
pub struct Group {
    pub key: Vec<Value>,
    pub rows: Vec<JoinedRow>,
}

impl<'a> QueryExecutor<'a> {
    pub fn execute_select(
        &self,
        stmt: SelectStmt,
        ctx: &mut ExecutionContext,
    ) -> Result<super::result::QueryResult, DbError> {
        let mut source_rows = if let Some(from) = stmt.from {
            let bound_table = self.bind_table(from)?;
            let mut current_rows = self.bind_table_rows(&bound_table)?;

            if !stmt.joins.is_empty() {
                for join in &stmt.joins {
                    let right_bound = self.bind_table(join.table.clone())?;
                    let right_rows = self.bind_table_rows(&right_bound)?;
                    current_rows = apply_join(
                        current_rows,
                        right_rows,
                        right_bound,
                        join,
                        ctx,
                        self.catalog,
                        self.storage,
                        self.clock,
                    )?;
                }
            }
            current_rows
        } else {
            vec![vec![]] // Single empty row for SELECT without FROM
        };

        if let Some(where_clause) = stmt.selection {
            let mut filtered = Vec::new();
            for row in source_rows {
                if eval_predicate(&where_clause, &row, ctx, self.catalog, self.storage, self.clock)? {
                    filtered.push(row);
                }
            }
            source_rows = filtered;
        }

        let result = if !stmt.group_by.is_empty() {
            self.execute_grouped_select(stmt.projection, source_rows, stmt.group_by, stmt.having, ctx)?
        } else {
            self.execute_flat_select(stmt.projection, source_rows, ctx)?
        };

        let mut final_rows = result.rows;
        if stmt.distinct {
            final_rows = deduplicate_projected_rows(final_rows);
        }

        if !stmt.order_by.is_empty() {
            let columns = &result.columns;
            let order_by_refs = &stmt.order_by;
            final_rows.sort_by(|a, b| {
                compare_projected_rows(a, b, columns, order_by_refs)
            });
        }

        if let Some(top) = stmt.top {
            let n = eval_top_n(&top, ctx, self.catalog, self.storage, self.clock)?;
            if final_rows.len() > n {
                final_rows.truncate(n);
            }
        }

        // OFFSET is not yet supported in the AST

        Ok(super::result::QueryResult {
            columns: result.columns,
            rows: final_rows,
        })
    }

    fn execute_flat_select(
        &self,
        projection: Vec<SelectItem>,
        rows: Vec<JoinedRow>,
        ctx: &mut ExecutionContext,
    ) -> Result<super::result::QueryResult, DbError> {
        let columns = expand_projection_columns(&projection, rows.first());
        let projected_rows = self.project_flat_rows(&projection, &rows, ctx);
        Ok(super::result::QueryResult {
            columns,
            rows: projected_rows,
        })
    }

    fn execute_grouped_select(
        &self,
        projection: Vec<SelectItem>,
        rows: Vec<JoinedRow>,
        group_by: Vec<Expr>,
        having: Option<Expr>,
        ctx: &mut ExecutionContext,
    ) -> Result<super::result::QueryResult, DbError> {
        let groups = self.build_groups(rows, &group_by, ctx)?;
        let mut projected_rows = Vec::new();

        for group in groups {
            if let Some(having_expr) = &having {
                if !eval_predicate(having_expr, &group.rows[0], ctx, self.catalog, self.storage, self.clock)? {
                    continue;
                }
            }
            projected_rows.push(self.project_group_row(&projection, &group, ctx)?);
        }

        let columns = expand_projection_columns(&projection, None); // TODO: sample
        Ok(super::result::QueryResult {
            columns,
            rows: projected_rows,
        })
    }

    fn build_groups(
        &self,
        rows: Vec<JoinedRow>,
        group_by: &[Expr],
        ctx: &mut ExecutionContext,
    ) -> Result<Vec<Group>, DbError> {
        let mut groups: Vec<Group> = Vec::new();
        for row in rows {
            let mut key = Vec::new();
            for expr in group_by {
                key.push(eval_expr(expr, &row, ctx, self.catalog, self.storage, self.clock)?);
            }

            if let Some(group) = groups.iter_mut().find(|g| g.key == key) {
                group.rows.push(row);
            } else {
                groups.push(Group {
                    key,
                    rows: vec![row],
                });
            }
        }
        Ok(groups)
    }

    fn bind_table(&self, tref: crate::ast::TableRef) -> Result<BoundTable, DbError> {
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
            .get_rows(bound.table.id)?;

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

    fn project_flat_rows(
        &self,
        projection: &[SelectItem],
        rows: &[JoinedRow],
        ctx: &mut ExecutionContext,
    ) -> Vec<Vec<Value>> {
        rows.iter()
            .map(|row| {
                let mut out = Vec::new();
                for item in projection {
                    match &item.expr {
                        Expr::Wildcard => out.extend(expand_wildcard_values(row)),
                        expr => out.push(
                            eval_expr(expr, row, ctx, self.catalog, self.storage, self.clock)
                                .unwrap_or(Value::Null),
                        ),
                    }
                }
                out
            })
            .collect()
    }

    fn project_group_row(
        &self,
        projection: &[SelectItem],
        group: &Group,
        ctx: &mut ExecutionContext,
    ) -> Result<Vec<Value>, DbError> {
        let mut out = Vec::new();
        let sample_row = &group.rows[0];

        for item in projection {
            match &item.expr {
                Expr::FunctionCall { name, args } if is_aggregate_function(name) => {
                    out.push(self.eval_aggregate(name, args, group, ctx));
                }
                expr => {
                    // If it's a grouped column, just evaluate on sample row
                    out.push(eval_expr(expr, sample_row, ctx, self.catalog, self.storage, self.clock)?);
                }
            }
        }
        Ok(out)
    }

    fn eval_aggregate(&self, name: &str, args: &[Expr], group: &Group, ctx: &mut ExecutionContext) -> Value {
        match name.to_uppercase().as_str() {
            "COUNT" => eval_aggregate_count(args, group, ctx, self.catalog, self.storage, self.clock),
            "SUM" => eval_aggregate_sum(args, group, ctx, self.catalog, self.storage, self.clock).unwrap_or(Value::Null),
            "AVG" => eval_aggregate_avg(args, group, ctx, self.catalog, self.storage, self.clock).unwrap_or(Value::Null),
            "MIN" => eval_aggregate_min(args, group, ctx, self.catalog, self.storage, self.clock).unwrap_or(Value::Null),
            "MAX" => eval_aggregate_max(args, group, ctx, self.catalog, self.storage, self.clock).unwrap_or(Value::Null),
            _ => Value::Null,
        }
    }

    fn compare_joined_rows(
        &self,
        a: &JoinedRow,
        b: &JoinedRow,
        order_by: &[OrderByExpr],
        ctx: &mut ExecutionContext,
    ) -> Ordering {
        for item in order_by {
            let av = eval_expr(&item.expr, a, ctx, self.catalog, self.storage, self.clock).unwrap_or(Value::Null);
            let bv = eval_expr(&item.expr, b, ctx, self.catalog, self.storage, self.clock).unwrap_or(Value::Null);
            let ord = compare_values(&av, &bv);
            if ord != Ordering::Equal {
                return if item.desc { ord.reverse() } else { ord };
            }
        }
        Ordering::Equal
    }
}

// ─── Join ────────────────────────────────────────────────────────────────

fn apply_join(
    rows: Vec<JoinedRow>,
    right_rows: Vec<JoinedRow>,
    right: BoundTable,
    join: &JoinClause,
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Vec<JoinedRow>, DbError> {
    match join.join_type {
        JoinType::Inner | JoinType::Left => {
            apply_join_left(rows, right_rows, right, join, ctx, catalog, storage, clock)
        }
        JoinType::Right => {
            apply_join_right(rows, right_rows, right, join, ctx, catalog, storage, clock)
        }
        JoinType::Full => apply_join_full(rows, right_rows, right, join, ctx, catalog, storage, clock),
    }
}

fn apply_join_left(
    rows: Vec<JoinedRow>,
    right_rows: Vec<JoinedRow>,
    right: BoundTable,
    join: &JoinClause,
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Vec<JoinedRow>, DbError> {
    let mut next_rows = Vec::new();

    for left_row in rows {
        let mut matched = false;
        for right_row in &right_rows {
            let mut candidate = left_row.clone();
            candidate.extend(right_row.clone());
            if eval_predicate(&join.on, &candidate, ctx, catalog, storage, clock)? {
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

fn apply_join_right(
    rows: Vec<JoinedRow>,
    right_rows: Vec<JoinedRow>,
    _right: BoundTable, // Fixed: prepend underscore
    join: &JoinClause,
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Vec<JoinedRow>, DbError> {
    let mut next_rows = Vec::new();

    for right_row in &right_rows {
        let mut matched = false;
        for left_row in &rows {
            let mut candidate = left_row.clone();
            candidate.extend(right_row.clone());
            if eval_predicate(&join.on, &candidate, ctx, catalog, storage, clock)? {
                matched = true;
                next_rows.push(candidate);
            }
        }

        if !matched {
            // Null-pad the left side
            let left_table = rows
                .first()
                .and_then(|r| r.first())
                .map(|ctx| (ctx.table.clone(), ctx.alias.clone()));
            if let Some((table, alias)) = left_table {
                let mut candidate = vec![ContextTable {
                    table,
                    alias,
                    row: None,
                }];
                candidate.extend(right_row.clone());
                next_rows.push(candidate);
            }
        }
    }

    Ok(next_rows)
}

fn apply_join_full(
    rows: Vec<JoinedRow>,
    right_rows: Vec<JoinedRow>,
    right: BoundTable,
    join: &JoinClause,
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Vec<JoinedRow>, DbError> {
    let mut next_rows = Vec::new();
    let mut matched_right: Vec<bool> = vec![false; right_rows.len()];

    for left_row in &rows {
        let mut matched = false;
        for (ri, right_row) in right_rows.iter().enumerate() {
            let mut candidate = left_row.clone();
            candidate.extend(right_row.clone());
            if eval_predicate(&join.on, &candidate, ctx, catalog, storage, clock)? {
                matched = true;
                matched_right[ri] = true;
                next_rows.push(candidate);
            }
        }

        if !matched {
            let mut candidate = left_row.clone();
            candidate.push(ContextTable {
                table: right.table.clone(),
                alias: right.alias.clone(),
                row: None,
            });
            next_rows.push(candidate);
        }
    }

    // Add unmatched right rows with null-padded left
    let left_table = rows
        .first()
        .and_then(|r| r.first())
        .map(|ctx| (ctx.table.clone(), ctx.alias.clone()));
    for (ri, matched) in matched_right.iter().enumerate() {
        if !matched {
            if let Some((table, alias)) = &left_table {
                let mut candidate = vec![ContextTable {
                    table: table.clone(),
                    alias: alias.clone(),
                    row: None,
                }];
                candidate.extend(right_rows[ri].clone());
                next_rows.push(candidate);
            }
        }
    }

    Ok(next_rows)
}

// ─── TOP ─────────────────────────────────────────────────────────────────

fn eval_top_n(
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

// ─── Aggregate helpers ──────────────────────────────────────────────────

fn is_aggregate_function(name: &str) -> bool {
    matches!(
        name.to_uppercase().as_str(),
        "COUNT" | "SUM" | "AVG" | "MIN" | "MAX"
    )
}

fn collect_group_values<'a>(
    expr: &'a Expr,
    group: &'a Group,
    ctx: &'a mut ExecutionContext,
    catalog: &'a dyn Catalog,
    storage: &'a dyn Storage,
    clock: &'a dyn Clock,
) -> Vec<Value> {
    group
        .rows
        .iter()
        .filter_map(move |row| eval_expr(expr, row, ctx, catalog, storage, clock).ok())
        .filter(|v| !v.is_null())
        .collect()
}

fn eval_aggregate_count(
    args: &[Expr],
    group: &Group,
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Value {
    let count = if args.first().is_some_and(|a| matches!(a, Expr::Wildcard)) {
        group.rows.len() as i64
    } else if let Some(expr) = args.first() {
        collect_group_values(expr, group, ctx, catalog, storage, clock).len() as i64
    } else {
        group.rows.len() as i64
    };
    Value::BigInt(count)
}

fn eval_aggregate_sum(
    args: &[Expr],
    group: &Group,
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    let expr = args
        .first()
        .ok_or_else(|| DbError::Execution("SUM requires 1 argument".into()))?;
    let mut sum_i64: i64 = 0;
    let mut sum_f64: f64 = 0.0;
    let mut has_values = false;
    let mut is_decimal = false;

    for val in collect_group_values(expr, group, ctx, catalog, storage, clock) {
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
        Ok(Value::Decimal((sum_f64 * 100.0) as i128, 2)) // Simplified
    } else {
        Ok(Value::BigInt(sum_i64))
    }
}

fn eval_aggregate_avg(
    args: &[Expr],
    group: &Group,
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    let values = collect_group_values(args.first().unwrap(), group, ctx, catalog, storage, clock);
    if values.is_empty() {
        return Ok(Value::Null);
    }
    let sum = eval_aggregate_sum(args, group, ctx, catalog, storage, clock)?;
    match sum {
        Value::BigInt(v) => Ok(Value::BigInt(v / values.len() as i64)),
        Value::Decimal(v, s) => {
             let divisor = 10i128.pow(s as u32);
             let f = (v as f64) / (divisor as f64);
             let avg = f / (values.len() as f64);
             Ok(Value::Decimal((avg * 100.0) as i128, 2))
        }
        _ => Ok(Value::Null),
    }
}

fn eval_aggregate_min(
    args: &[Expr],
    group: &Group,
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    let values = collect_group_values(args.first().unwrap(), group, ctx, catalog, storage, clock);
    Ok(values.into_iter().min_by(compare_values).unwrap_or(Value::Null))
}

fn eval_aggregate_max(
    args: &[Expr],
    group: &Group,
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    let values = collect_group_values(args.first().unwrap(), group, ctx, catalog, storage, clock);
    Ok(values.into_iter().max_by(compare_values).unwrap_or(Value::Null))
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

fn deduplicate_projected_rows(rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
    let mut seen: Vec<String> = Vec::new();
    let mut result = Vec::new();
    for row in rows {
        let key = row.iter().map(value_key).collect::<Vec<_>>().join("|");
        if !seen.contains(&key) {
            seen.push(key);
            result.push(row);
        }
    }
    result
}

use std::cmp::Ordering;
use std::collections::HashSet;

use crate::ast::{BinaryOp, Expr, JoinType, OrderByExpr, SelectStmt, TableRef};
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::types::Value;

use super::context::ExecutionContext;
use super::cte::resolve_cte_table;
use super::evaluator::eval_expr;
use super::metadata::resolve_virtual_table;
use super::model::{BoundTable, JoinedRow};
use super::planner::{LogicalPlan, PhysicalJoin, PhysicalPlan, PhysicalScan, ScanStrategy};
use super::value_ops::compare_values;

pub fn build_logical_plan(stmt: &SelectStmt) -> Result<LogicalPlan, DbError> {
    let Some(from) = &stmt.from else {
        return Err(DbError::Execution("planner requires FROM source".into()));
    };
    let mut plan = LogicalPlan::Scan {
        table: from.clone(),
    };
    for join in &stmt.joins {
        plan = LogicalPlan::Join {
            left: Box::new(plan),
            join: join.clone(),
        };
    }
    if let Some(selection) = &stmt.selection {
        plan = LogicalPlan::Filter {
            input: Box::new(plan),
            predicate: selection.clone(),
        };
    }
    if !stmt.group_by.is_empty() || stmt.having.is_some() {
        plan = LogicalPlan::Aggregate {
            input: Box::new(plan),
            group_by: stmt.group_by.clone(),
            having: stmt.having.clone(),
        };
    }
    plan = LogicalPlan::Project {
        input: Box::new(plan),
        projection: stmt.projection.clone(),
    };
    if stmt.distinct {
        plan = LogicalPlan::Distinct {
            input: Box::new(plan),
        };
    }
    if !stmt.order_by.is_empty() {
        plan = LogicalPlan::Sort {
            input: Box::new(plan),
            order_by: stmt.order_by.clone(),
        };
    }
    if let Some(top) = &stmt.top {
        plan = LogicalPlan::Top {
            input: Box::new(plan),
            top: top.clone(),
        };
    }
    Ok(plan)
}

pub fn build_physical_plan(
    stmt: &SelectStmt,
    logical: &LogicalPlan,
    catalog: &dyn Catalog,
    ctx: &mut ExecutionContext,
    bind_table_fn: impl Fn(TableRef, &dyn Catalog, &mut ExecutionContext) -> Result<BoundTable, DbError>,
) -> Result<PhysicalPlan, DbError> {
    let Some(from) = stmt.from.clone() else {
        return Err(DbError::Execution("planner requires FROM source".into()));
    };

    let all_inner = stmt.joins.iter().all(|j| j.join_type == JoinType::Inner || j.join_type == JoinType::Cross);
    let mut alias_predicates: std::collections::HashMap<String, Vec<Expr>> =
        std::collections::HashMap::new();
    let mut residual = stmt.selection.clone();
    if all_inner && stmt.joins.is_empty() {
        alias_predicates.insert("".to_string(), split_conjuncts(stmt.selection.clone()));
        residual = None;
    }

    let mut joins = stmt.joins.clone();
    if all_inner && !joins.is_empty() {
        joins = reorder_inner_joins_heuristic(&from, joins)?;
    }

    let base_bound = bind_table_fn(from, catalog, ctx)?;
    let base_predicate = if joins.is_empty() {
        alias_predicates.remove("").and_then(and_terms)
    } else {
        None
    };
    let base_scan = plan_scan(&base_bound, base_predicate, &stmt.order_by, catalog);

    let mut physical_joins = Vec::new();
    for join in joins {
        let right_bound = bind_table_fn(join.table.clone(), catalog, ctx)?;
        let right_pred = alias_predicates
            .remove(&right_bound.alias.to_uppercase())
            .and_then(and_terms);
        let right_scan = plan_scan(&right_bound, right_pred, &[], catalog);
        physical_joins.push(PhysicalJoin {
            right: right_scan,
            join,
        });
    }

    let required_columns = required_columns_from_logical(logical);
    let order_satisfied_by_scan = physical_joins.is_empty()
        && base_scan.pushed_predicate.is_none()
        && scan_satisfies_order(&base_scan, &stmt.order_by, catalog);

    Ok(PhysicalPlan {
        base: base_scan,
        joins: physical_joins,
        applies: stmt.applies.clone(),
        residual_filter: residual,
        projection: stmt.projection.clone(),
        group_by: stmt.group_by.clone(),
        having: stmt.having.clone(),
        distinct: stmt.distinct,
        order_by: stmt.order_by.clone(),
        top: stmt.top.clone(),
        required_columns,
        order_satisfied_by_scan,
        offset: stmt.offset.clone(),
        fetch: stmt.fetch.clone(),
    })
}

fn reorder_inner_joins_heuristic(
    from: &TableRef,
    joins: Vec<crate::ast::JoinClause>,
) -> Result<Vec<crate::ast::JoinClause>, DbError> {
    // Keep lexical join order to preserve ON-clause binding semantics.
    let _ = from;
    Ok(joins)
}

fn plan_scan(
    bound: &BoundTable,
    pushed_predicate: Option<Expr>,
    order_by: &[OrderByExpr],
    catalog: &dyn Catalog,
) -> PhysicalScan {
    let strategy = choose_scan_strategy(bound, pushed_predicate.as_ref(), order_by, catalog);
    PhysicalScan {
        bound: bound.clone(),
        strategy,
        pushed_predicate,
    }
}

pub fn execute_scan(
    scan: &PhysicalScan,
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn crate::storage::Storage,
    clock: &dyn super::clock::Clock,
) -> Result<Vec<JoinedRow>, DbError> {
    let rows = bind_table_rows(&scan.bound, ctx, storage)?;
    let mut scanned = match scan.strategy {
        ScanStrategy::TableScan => rows,
        ScanStrategy::IndexSeek { .. } | ScanStrategy::IndexScan { .. } => {
            apply_index_strategy(rows, scan, ctx, catalog, storage, clock)?
        }
    };
    if let Some(predicate) = &scan.pushed_predicate {
        scanned.retain(|row| {
            super::evaluator::eval_predicate(predicate, row, ctx, catalog, storage, clock)
                .unwrap_or(false)
        });
    }
    Ok(scanned)
}

pub fn bind_table(
    tref: TableRef,
    catalog: &dyn Catalog,
    ctx: &mut ExecutionContext,
) -> Result<BoundTable, DbError> {
    let mut tref = tref;
    if let Some(mapped) = ctx.resolve_table_name(&tref.name.name) {
        tref.name.name = mapped;
        if tref.name.schema.is_none() {
            tref.name.schema = Some("dbo".to_string());
        }
    } else {
        // Fallback for regular tables that don't start with @ or #
        if !tref.name.name.starts_with('@') && !tref.name.name.starts_with('#') {
            // Keep original name
        }
    }
    let schema = tref.name.schema_or_dbo();
    let name = &tref.name.name;

    if let Some(cte) = resolve_cte_table(&ctx.ctes, schema, name) {
        return Ok(BoundTable {
            alias: tref.alias.clone().unwrap_or_else(|| name.clone()),
            table: cte.table_def.clone(),
            virtual_rows: None,
        });
    }

    if let Some((table, rows)) = resolve_virtual_table(schema, name, catalog) {
        return Ok(BoundTable {
            alias: tref.alias.clone().unwrap_or_else(|| name.clone()),
            table,
            virtual_rows: Some(rows),
        });
    }


    let table = catalog
        .find_table(schema, name)
        .ok_or_else(|| DbError::Semantic(format!("table '{}.{}' not found", schema, name)))?
        .clone();

    Ok(BoundTable {
        alias: tref.alias.clone().unwrap_or_else(|| table.name.clone()),
        table,
        virtual_rows: None,
    })
}

fn bind_table_rows(
    bound: &BoundTable,
    ctx: &ExecutionContext,
    storage: &dyn crate::storage::Storage,
) -> Result<Vec<JoinedRow>, DbError> {
    if let Some(cte) = ctx.ctes.get(&bound.table.name.to_uppercase()) {
        return Ok(super::cte::cte_to_context_rows(cte, &bound.alias));
    }

    if let Some(rows) = &bound.virtual_rows {
        return Ok(rows
            .iter()
            .enumerate()
            .map(|(i, row)| {
                vec![super::model::ContextTable {
                    table: bound.table.clone(),
                    alias: bound.alias.clone(),
                    row: Some(row.clone()),
                    storage_index: Some(i),
                }]
            })
            .collect());
    }

    let stored_rows = storage.get_rows(bound.table.id)?;

    Ok(stored_rows
        .iter()
        .enumerate()
        .filter(|(_, r)| !r.deleted)
        .map(|(i, row)| {
            vec![super::model::ContextTable {
                table: bound.table.clone(),
                alias: bound.alias.clone(),
                row: Some(row.clone()),
                storage_index: Some(i),
            }]
        })
        .collect())
}

fn apply_index_strategy(
    rows: Vec<JoinedRow>,
    scan: &PhysicalScan,
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn crate::storage::Storage,
    clock: &dyn super::clock::Clock,
) -> Result<Vec<JoinedRow>, DbError> {
    let index_id = match scan.strategy {
        ScanStrategy::IndexSeek { index_id } | ScanStrategy::IndexScan { index_id } => index_id,
        ScanStrategy::TableScan => return Ok(rows),
    };
    let Some(index) = catalog
        .get_indexes()
        .iter()
        .find(|idx| idx.id == index_id && idx.table_id == scan.bound.table.id)
    else {
        return Ok(rows);
    };
    let Some(first_col_id) = index.column_ids.first().copied() else {
        return Ok(rows);
    };
    let Some(col_idx) = scan
        .bound
        .table
        .columns
        .iter()
        .position(|c| c.id == first_col_id)
    else {
        return Ok(rows);
    };

    let mut keyed: Vec<(Value, JoinedRow)> = rows
        .into_iter()
        .filter_map(|row| {
            let v = row
                .first()
                .and_then(|ct| ct.row.as_ref())
                .and_then(|r| r.values.get(col_idx))
                .cloned()?;
            Some((v, row))
        })
        .collect();
    keyed.sort_by(|a, b| compare_values(&a.0, &b.0));

    let out = if matches!(scan.strategy, ScanStrategy::IndexSeek { .. }) {
        if let Some((op, rhs)) = extract_index_predicate_rhs(
            scan.pushed_predicate.as_ref(),
            &scan.bound.alias,
            &scan.bound.table.columns[col_idx].name,
        ) {
            let rhs_val = eval_expr(&rhs, &[], ctx, catalog, storage, clock)?;
            keyed
                .into_iter()
                .filter(|(lhs, _)| {
                    matches!(op, BinaryOp::Eq)
                        && compare_values(lhs, &rhs_val) == Ordering::Equal
                })
                .map(|(_, row)| row)
                .collect()
        } else {
            keyed.into_iter().map(|(_, row)| row).collect()
        }
    } else if let Some((op, rhs)) = extract_index_predicate_rhs(
        scan.pushed_predicate.as_ref(),
        &scan.bound.alias,
        &scan.bound.table.columns[col_idx].name,
    ) {
        let rhs_val = eval_expr(&rhs, &[], ctx, catalog, storage, clock)?;
        keyed
            .into_iter()
            .filter(|(lhs, _)| compare_with_op(compare_values(lhs, &rhs_val), op))
            .map(|(_, row)| row)
            .collect()
    } else {
        keyed.into_iter().map(|(_, row)| row).collect()
    };
    Ok(out)
}

pub fn split_conjuncts(expr: Option<Expr>) -> Vec<Expr> {
    fn walk(expr: Expr, out: &mut Vec<Expr>) {
        match expr {
            Expr::Binary {
                left,
                op: BinaryOp::And,
                right,
            } => {
                walk(*left, out);
                walk(*right, out);
            }
            other => out.push(other),
        }
    }
    let mut out = Vec::new();
    if let Some(e) = expr {
        walk(e, &mut out);
    }
    out
}

pub fn and_terms(mut terms: Vec<Expr>) -> Option<Expr> {
    if terms.is_empty() {
        return None;
    }
    let mut acc = terms.remove(0);
    for term in terms {
        acc = Expr::Binary {
            left: Box::new(acc),
            op: BinaryOp::And,
            right: Box::new(term),
        };
    }
    Some(acc)
}

fn expr_aliases(expr: &Expr) -> HashSet<String> {
    fn walk(expr: &Expr, out: &mut HashSet<String>) {
        match expr {
            Expr::QualifiedIdentifier(parts) => {
                if let Some(alias) = parts.first() {
                    out.insert(alias.to_uppercase());
                }
            }
            Expr::Binary { left, right, .. } => {
                walk(left, out);
                walk(right, out);
            }
            Expr::Unary { expr, .. } => walk(expr, out),
            Expr::IsNull(inner) | Expr::IsNotNull(inner) => walk(inner, out),
            Expr::Cast { expr, .. } => walk(expr, out),
            Expr::Convert { expr, .. } => walk(expr, out),
            Expr::Case {
                operand,
                when_clauses,
                else_result,
            } => {
                if let Some(op) = operand {
                    walk(op, out);
                }
                for wc in when_clauses {
                    walk(&wc.condition, out);
                    walk(&wc.result, out);
                }
                if let Some(er) = else_result {
                    walk(er, out);
                }
            }
            Expr::InList { expr, list, .. } => {
                walk(expr, out);
                for item in list {
                    walk(item, out);
                }
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                walk(expr, out);
                walk(low, out);
                walk(high, out);
            }
            Expr::Like { expr, pattern, .. } => {
                walk(expr, out);
                walk(pattern, out);
            }
            Expr::FunctionCall { args, .. } => {
                for arg in args {
                    walk(arg, out);
                }
            }
            Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } => {}
            _ => {}
        }
    }
    let mut out = HashSet::new();
    walk(expr, &mut out);
    out
}

fn required_columns_from_logical(plan: &LogicalPlan) -> Vec<String> {
    fn collect(plan: &LogicalPlan, out: &mut HashSet<String>) {
        match plan {
            LogicalPlan::Scan { table } => {
                out.insert(format!(
                    "{}.{}",
                    table.name.schema_or_dbo().to_uppercase(),
                    table.name.name.to_uppercase()
                ));
            }
            LogicalPlan::Join { left, join } => {
                collect(left, out);
                out.insert(join.table.name.name.to_uppercase());
            }
            LogicalPlan::Filter { input, predicate } => {
                collect(input, out);
                for alias in expr_aliases(predicate) {
                    out.insert(alias);
                }
            }
            LogicalPlan::Aggregate {
                input,
                group_by,
                having,
            } => {
                collect(input, out);
                for expr in group_by {
                    for alias in expr_aliases(expr) {
                        out.insert(alias);
                    }
                }
                if let Some(h) = having {
                    for alias in expr_aliases(h) {
                        out.insert(alias);
                    }
                }
            }
            LogicalPlan::Project { input, projection } => {
                collect(input, out);
                for item in projection {
                    for alias in expr_aliases(&item.expr) {
                        out.insert(alias);
                    }
                }
            }
            LogicalPlan::Distinct { input } => collect(input, out),
            LogicalPlan::Sort { input, order_by } => {
                collect(input, out);
                for item in order_by {
                    for alias in expr_aliases(&item.expr) {
                        out.insert(alias);
                    }
                }
            }
            LogicalPlan::Top { input, top } => {
                collect(input, out);
                for alias in expr_aliases(&top.value) {
                    out.insert(alias);
                }
            }
        }
    }
    let mut out = HashSet::new();
    collect(plan, &mut out);
    out.into_iter().collect()
}

fn choose_scan_strategy(
    bound: &BoundTable,
    predicate: Option<&Expr>,
    order_by: &[OrderByExpr],
    catalog: &dyn Catalog,
) -> ScanStrategy {
    let indexes: Vec<&crate::catalog::IndexDef> = catalog
        .get_indexes()
        .iter()
        .filter(|idx| idx.table_id == bound.table.id)
        .collect();
    if indexes.is_empty() {
        return ScanStrategy::TableScan;
    }
    let Some(idx) = indexes.first() else {
        return ScanStrategy::TableScan;
    };
    let Some(first_col_id) = idx.column_ids.first() else {
        return ScanStrategy::TableScan;
    };
    let Some(first_col) = bound.table.columns.iter().find(|c| c.id == *first_col_id) else {
        return ScanStrategy::TableScan;
    };

    if let Some(pred) = predicate {
        if let Some((op, _)) =
            extract_index_predicate_rhs(Some(pred), &bound.alias, &first_col.name)
        {
            if matches!(op, BinaryOp::Eq) {
                return ScanStrategy::IndexSeek { index_id: idx.id };
            }
            return ScanStrategy::IndexScan { index_id: idx.id };
        }
    }
    if order_by.len() == 1 {
        if let Expr::QualifiedIdentifier(parts) = &order_by[0].expr {
            if parts.len() >= 2
                && parts[0].eq_ignore_ascii_case(&bound.alias)
                && parts[1].eq_ignore_ascii_case(&first_col.name)
                && order_by[0].asc
            {
                return ScanStrategy::IndexScan { index_id: idx.id };
            }
        }
    }
    ScanStrategy::TableScan
}

fn scan_satisfies_order(
    scan: &PhysicalScan,
    order_by: &[OrderByExpr],
    catalog: &dyn Catalog,
) -> bool {
    if order_by.is_empty() {
        return true;
    }
    let index_id = match scan.strategy {
        ScanStrategy::IndexSeek { index_id } | ScanStrategy::IndexScan { index_id } => index_id,
        ScanStrategy::TableScan => return false,
    };
    let Some(index) = catalog.get_indexes().iter().find(|idx| idx.id == index_id) else {
        return false;
    };
    let Some(col_id) = index.column_ids.first() else {
        return false;
    };
    let Some(col) = scan.bound.table.columns.iter().find(|c| c.id == *col_id) else {
        return false;
    };
    if order_by.len() != 1 || !order_by[0].asc {
        return false;
    }
    match &order_by[0].expr {
        Expr::QualifiedIdentifier(parts) if parts.len() >= 2 => {
            parts[0].eq_ignore_ascii_case(&scan.bound.alias)
                && parts[1].eq_ignore_ascii_case(&col.name)
        }
        Expr::Identifier(name) => name.eq_ignore_ascii_case(&col.name),
        _ => false,
    }
}

fn extract_index_predicate_rhs(
    predicate: Option<&Expr>,
    alias: &str,
    column: &str,
) -> Option<(BinaryOp, Expr)> {
    let pred = predicate?;
    match pred {
        Expr::Binary { left, op, right } => {
            if let Expr::QualifiedIdentifier(parts) = left.as_ref() {
                if parts.len() >= 2
                    && parts[0].eq_ignore_ascii_case(alias)
                    && parts[1].eq_ignore_ascii_case(column)
                    && is_supported_index_op(*op)
                {
                    return Some((*op, (*right.clone())));
                }
            }
            if let Expr::QualifiedIdentifier(parts) = right.as_ref() {
                if parts.len() >= 2
                    && parts[0].eq_ignore_ascii_case(alias)
                    && parts[1].eq_ignore_ascii_case(column)
                    && is_supported_index_op(*op)
                {
                    return Some((*op, (*left.clone())));
                }
            }
            if *op == BinaryOp::And {
                extract_index_predicate_rhs(Some(left), alias, column)
                    .or_else(|| extract_index_predicate_rhs(Some(right), alias, column))
            } else {
                None
            }
        }
        _ => None,
    }
}

fn is_supported_index_op(op: BinaryOp) -> bool {
    matches!(
        op,
        BinaryOp::Eq | BinaryOp::Gt | BinaryOp::Gte | BinaryOp::Lt | BinaryOp::Lte
    )
}

fn compare_with_op(ord: Ordering, op: BinaryOp) -> bool {
    match op {
        BinaryOp::Eq => ord == Ordering::Equal,
        BinaryOp::Gt => ord == Ordering::Greater,
        BinaryOp::Gte => ord == Ordering::Greater || ord == Ordering::Equal,
        BinaryOp::Lt => ord == Ordering::Less,
        BinaryOp::Lte => ord == Ordering::Less || ord == Ordering::Equal,
        _ => false,
    }
}

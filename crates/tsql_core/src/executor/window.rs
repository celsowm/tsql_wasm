use crate::ast::{Expr, OrderByExpr, SelectItem, WindowFunc, WindowFrame, WindowFrameUnits, WindowFrameExtent, WindowFrameBound};
use crate::error::DbError;
use crate::types::Value;

use super::clock::Clock;
use super::context::ExecutionContext;
use super::evaluator::eval_expr;
use super::model::{JoinedRow, Group};
use super::value_ops::compare_values;
use super::aggregates::dispatch_aggregate;
use std::cmp::Ordering;
use std::collections::HashMap;

pub struct WindowExecutor<'a> {
    catalog: &'a dyn crate::catalog::Catalog,
    storage: &'a dyn crate::storage::Storage,
    clock: &'a dyn Clock,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct WindowSpec {
    partition_by: Vec<Expr>,
    order_by: Vec<OrderByExpr>,
    frame: Option<WindowFrame>,
}

impl<'a> WindowExecutor<'a> {
    pub fn new(
        catalog: &'a dyn crate::catalog::Catalog,
        storage: &'a dyn crate::storage::Storage,
        clock: &'a dyn Clock,
    ) -> Self {
        Self {
            catalog,
            storage,
            clock,
        }
    }

    pub fn execute(
        &self,
        projection: &[SelectItem],
        rows: Vec<JoinedRow>,
        ctx: &mut ExecutionContext,
    ) -> Result<super::result::QueryResult, DbError> {
        let has_window = projection
            .iter()
            .any(|item| has_window_function(&item.expr));

        if !has_window {
            return self.execute_regular_select(projection, rows, ctx);
        }

        // Group all unique window functions by their window specification
        let mut window_specs: Vec<(WindowSpec, Vec<Expr>)> = Vec::new();
        for item in projection {
            self.collect_window_exprs(&item.expr, &mut window_specs);
        }

        // Map to store calculated values: results_map[window_expr][row_idx] = value
        let mut results_map: HashMap<Expr, Vec<Value>> = HashMap::new();

        // Process each window specification group
        for (spec, win_exprs) in &window_specs {
            let mut sorted_rows_with_indices: Vec<(usize, JoinedRow)> = rows.iter().cloned().enumerate().collect();

            // Sort rows based on PARTITION BY and ORDER BY
            sorted_rows_with_indices.sort_by(|(idx_a, a), (idx_b, b)| {
                for expr in &spec.partition_by {
                    let va = eval_expr(expr, a, ctx, self.catalog, self.storage, self.clock).unwrap_or(Value::Null);
                    let vb = eval_expr(expr, b, ctx, self.catalog, self.storage, self.clock).unwrap_or(Value::Null);
                    let ord = compare_values(&va, &vb);
                    if ord != Ordering::Equal {
                        return ord;
                    }
                }
                let ord = self.compare_rows(a, b, &spec.order_by, ctx);
                if ord != Ordering::Equal {
                    return ord;
                }
                // Stable sort by original index in the input Vec<JoinedRow>
                idx_a.cmp(idx_b)
            });

            // Identify partitions
            let mut partition_starts = Vec::new();
            if !sorted_rows_with_indices.is_empty() {
                partition_starts.push(0);
                for i in 1..sorted_rows_with_indices.len() {
                    let mut equal = true;
                    for expr in &spec.partition_by {
                        let v_prev = eval_expr(expr, &sorted_rows_with_indices[i-1].1, ctx, self.catalog, self.storage, self.clock).unwrap_or(Value::Null);
                        let v_curr = eval_expr(expr, &sorted_rows_with_indices[i].1, ctx, self.catalog, self.storage, self.clock).unwrap_or(Value::Null);
                        if compare_values(&v_prev, &v_curr) != Ordering::Equal {
                            equal = false;
                            break;
                        }
                    }
                    if !equal {
                        partition_starts.push(i);
                    }
                }
            }
            partition_starts.push(sorted_rows_with_indices.len());

            for p in 0..partition_starts.len() - 1 {
                let start = partition_starts[p];
                let end = partition_starts[p+1];
                let partition = &sorted_rows_with_indices[start..end];

                // For each row in the partition, calculate each window function
                for (i, (original_idx, _)) in partition.iter().enumerate() {
                    for win_expr in win_exprs {
                        if let Expr::WindowFunction { func, args, .. } = win_expr {
                            let val = match func {
                                WindowFunc::RowNumber => Value::Int((i + 1) as i32),
                                WindowFunc::Rank => {
                                    let mut final_rank = 1;
                                    for j in 0..i {
                                        if self.compare_rows(&partition[j].1, &partition[i].1, &spec.order_by, ctx) == Ordering::Less {
                                            final_rank = (j + 2) as i32;
                                        } else {
                                            break;
                                        }
                                    }
                                    Value::Int(final_rank)
                                }
                                WindowFunc::DenseRank => {
                                    let mut dense_rank = 1;
                                    for j in 1..=i {
                                        if self.compare_rows(&partition[j-1].1, &partition[j].1, &spec.order_by, ctx) == Ordering::Less {
                                            dense_rank += 1;
                                        }
                                    }
                                    Value::Int(dense_rank)
                                }
                                WindowFunc::Lag => {
                                    let offset = args.get(1).and_then(|e| {
                                        if let Expr::Integer(n) = e { Some(*n as usize) } else { None }
                                    }).unwrap_or(1);
                                    if i >= offset {
                                        eval_expr(&args[0], &partition[i - offset].1, ctx, self.catalog, self.storage, self.clock).unwrap_or(Value::Null)
                                    } else {
                                        args.get(2).map(|e| eval_expr(e, &partition[i].1, ctx, self.catalog, self.storage, self.clock).unwrap_or(Value::Null)).unwrap_or(Value::Null)
                                    }
                                }
                                WindowFunc::Lead => {
                                    let offset = args.get(1).and_then(|e| {
                                        if let Expr::Integer(n) = e { Some(*n as usize) } else { None }
                                    }).unwrap_or(1);
                                    if i + offset < partition.len() {
                                        eval_expr(&args[0], &partition[i + offset].1, ctx, self.catalog, self.storage, self.clock).unwrap_or(Value::Null)
                                    } else {
                                        args.get(2).map(|e| eval_expr(e, &partition[i].1, ctx, self.catalog, self.storage, self.clock).unwrap_or(Value::Null)).unwrap_or(Value::Null)
                                    }
                                }
                                WindowFunc::FirstValue => {
                                    let frame_rows = self.get_frame_rows(partition, i, &spec.frame, &spec.order_by, ctx);
                                    if frame_rows.is_empty() {
                                        Value::Null
                                    } else {
                                        eval_expr(&args[0], &frame_rows[0], ctx, self.catalog, self.storage, self.clock).unwrap_or(Value::Null)
                                    }
                                }
                                WindowFunc::LastValue => {
                                    let frame_rows = self.get_frame_rows(partition, i, &spec.frame, &spec.order_by, ctx);
                                    if frame_rows.is_empty() {
                                        Value::Null
                                    } else {
                                        eval_expr(&args[0], &frame_rows[frame_rows.len() - 1], ctx, self.catalog, self.storage, self.clock).unwrap_or(Value::Null)
                                    }
                                }
                                WindowFunc::NTile => {
                                    let n_buckets = if let Some(e) = args.get(0) {
                                        match eval_expr(e, &partition[i].1, ctx, self.catalog, self.storage, self.clock) {
                                            Ok(Value::Int(n)) => n as i64,
                                            Ok(Value::BigInt(n)) => n,
                                            Ok(Value::TinyInt(n)) => n as i64,
                                            Ok(Value::SmallInt(n)) => n as i64,
                                            _ => 1,
                                        }
                                    } else {
                                        1
                                    };

                                    if n_buckets <= 0 {
                                        Value::Null
                                    } else {
                                        let partition_size = partition.len() as i64;
                                        let bucket_size = partition_size / n_buckets;
                                        let remainder = partition_size % n_buckets;

                                        let mut current_row = 0i64;
                                        let mut found_bucket = 1i64;
                                        for b in 1..=n_buckets {
                                            let rows_in_this_bucket = bucket_size + if b <= remainder { 1 } else { 0 };
                                            if (i as i64) >= current_row && (i as i64) < current_row + rows_in_this_bucket {
                                                found_bucket = b;
                                                break;
                                            }
                                            current_row += rows_in_this_bucket;
                                        }
                                        Value::BigInt(found_bucket)
                                    }
                                }
                                WindowFunc::Aggregate(name) => {
                                    let frame_rows = self.get_frame_rows(partition, i, &spec.frame, &spec.order_by, ctx);
                                    let group = Group { key: vec![], rows: frame_rows };
                                    let res = dispatch_aggregate(name.as_str(), args, &group, ctx, self.catalog, self.storage, self.clock);
                                    match res {
                                        Some(Ok(v)) => v,
                                        Some(Err(e)) => return Err(e),
                                        None => Value::Null,
                                    }
                                }
                            };
                            results_map.entry(win_expr.clone()).or_insert_with(|| vec![Value::Null; rows.len()])[*original_idx] = val;
                        }
                    }
                }
            }
        }

        // One final pass to evaluate all projected expressions with window results available in context
        let mut final_projected_rows = Vec::with_capacity(rows.len());
        for (idx, row) in rows.iter().enumerate() {
            let mut window_map = HashMap::new();
            for (expr, values) in &results_map {
                window_map.insert(expr.clone(), values[idx].clone());
            }

            ctx.window_context = Some(window_map);
            let mut projected_row = Vec::with_capacity(projection.len());
            for item in projection {
                projected_row.push(eval_expr(&item.expr, row, ctx, self.catalog, self.storage, self.clock).unwrap_or(Value::Null));
            }
            final_projected_rows.push(projected_row);
        }
        ctx.window_context = None;

        let mut column_types = vec![crate::types::DataType::VarChar { max_len: 4000 }; projection.len()];
        for col_idx in 0..projection.len() {
            for row in &final_projected_rows {
                if !row[col_idx].is_null() {
                    column_types[col_idx] = row[col_idx].data_type().unwrap_or(crate::types::DataType::VarChar { max_len: 4000 });
                    break;
                }
            }
        }

        let columns: Vec<String> = projection
            .iter()
            .map(|i| {
                i.alias.clone().unwrap_or_else(|| {
                    super::projection::expr_label(&i.expr)
                })
            })
            .collect();

        Ok(super::result::QueryResult {
            columns,
            column_types,
            rows: final_projected_rows,
        })
    }

    fn collect_window_exprs<'b>(
        &self,
        expr: &'b Expr,
        window_specs: &mut Vec<(WindowSpec, Vec<Expr>)>,
    ) {
        match expr {
            Expr::WindowFunction { partition_by, order_by, frame, .. } => {
                let spec = WindowSpec {
                    partition_by: partition_by.clone(),
                    order_by: order_by.clone(),
                    frame: frame.clone(),
                };
                if let Some(existing) = window_specs.iter_mut().find(|(s, _)| s == &spec) {
                    if !existing.1.contains(expr) {
                        existing.1.push(expr.clone());
                    }
                } else {
                    window_specs.push((spec, vec![expr.clone()]));
                }
            }
            Expr::Binary { left, right, .. } => {
                self.collect_window_exprs(left, window_specs);
                self.collect_window_exprs(right, window_specs);
            }
            Expr::Unary { expr: inner, .. } => {
                self.collect_window_exprs(inner, window_specs);
            }
            Expr::IsNull(inner) | Expr::IsNotNull(inner) | Expr::Cast { expr: inner, .. } | Expr::Convert { expr: inner, .. } => {
                self.collect_window_exprs(inner, window_specs);
            }
            Expr::Case { operand, when_clauses, else_result } => {
                if let Some(op) = operand {
                    self.collect_window_exprs(op, window_specs);
                }
                for wc in when_clauses {
                    self.collect_window_exprs(&wc.condition, window_specs);
                    self.collect_window_exprs(&wc.result, window_specs);
                }
                if let Some(el) = else_result {
                    self.collect_window_exprs(el, window_specs);
                }
            }
            Expr::FunctionCall { args, .. } => {
                for arg in args {
                    self.collect_window_exprs(arg, window_specs);
                }
            }
            Expr::InList { expr: inner, list, .. } => {
                self.collect_window_exprs(inner, window_specs);
                for item in list {
                    self.collect_window_exprs(item, window_specs);
                }
            }
            Expr::Between { expr: inner, low, high, .. } => {
                self.collect_window_exprs(inner, window_specs);
                self.collect_window_exprs(low, window_specs);
                self.collect_window_exprs(high, window_specs);
            }
            Expr::Like { expr: inner, pattern, .. } => {
                self.collect_window_exprs(inner, window_specs);
                self.collect_window_exprs(pattern, window_specs);
            }
            Expr::InSubquery { expr: inner, .. } => {
                self.collect_window_exprs(inner, window_specs);
            }
            _ => {}
        }
    }

    fn get_frame_rows(&self, partition: &[(usize, JoinedRow)], current_idx: usize, frame_spec: &Option<WindowFrame>, order_by: &[OrderByExpr], ctx: &mut ExecutionContext) -> Vec<JoinedRow> {
        let (start_idx, end_idx) = match frame_spec {
            None => {
                // T-SQL default for OVER(ORDER BY ...) is RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW.
                // Without ORDER BY, it is the whole partition.
                if order_by.is_empty() {
                    (0, partition.len())
                } else {
                    (0, self.resolve_bound(partition, current_idx, &WindowFrameBound::CurrentRow, true, WindowFrameUnits::Range, order_by, ctx))
                }
            }
            Some(f) => {
                match &f.extent {
                    WindowFrameExtent::Bound(b) => {
                        (self.resolve_bound(partition, current_idx, b, false, f.units, order_by, ctx),
                         self.resolve_bound(partition, current_idx, &WindowFrameBound::CurrentRow, true, f.units, order_by, ctx))
                    }
                    WindowFrameExtent::Between(b1, b2) => {
                        (self.resolve_bound(partition, current_idx, b1, false, f.units, order_by, ctx),
                         self.resolve_bound(partition, current_idx, b2, true, f.units, order_by, ctx))
                    }
                }
            }
        };

        let start_clamped = start_idx.min(partition.len());
        let end_clamped = end_idx.min(partition.len());

        if start_clamped >= end_clamped {
            return vec![];
        }

        partition[start_clamped..end_clamped].iter().map(|(_, r)| r.clone()).collect()
    }

    fn resolve_bound(&self, partition: &[(usize, JoinedRow)], current_idx: usize, bound: &WindowFrameBound, is_end: bool, units: WindowFrameUnits, order_by: &[OrderByExpr], ctx: &mut ExecutionContext) -> usize {
        match units {
            WindowFrameUnits::Rows => {
                match bound {
                    WindowFrameBound::UnboundedPreceding => 0,
                    WindowFrameBound::Preceding(n) => current_idx.saturating_sub(n.unwrap_or(0) as usize),
                    WindowFrameBound::CurrentRow => if is_end { current_idx + 1 } else { current_idx },
                    WindowFrameBound::Following(n) => current_idx + n.unwrap_or(0) as usize + 1,
                    WindowFrameBound::UnboundedFollowing => partition.len(),
                }
            }
            WindowFrameUnits::Range | WindowFrameUnits::Groups => {
                match bound {
                    WindowFrameBound::UnboundedPreceding => 0,
                    WindowFrameBound::UnboundedFollowing => partition.len(),
                    WindowFrameBound::CurrentRow => {
                        if is_end {
                            // Find last peer
                            let mut i = current_idx;
                            while i + 1 < partition.len() && self.compare_rows(&partition[i].1, &partition[i+1].1, order_by, ctx) == Ordering::Equal {
                                i += 1;
                            }
                            i + 1
                        } else {
                            // Find first peer
                            let mut i = current_idx;
                            while i > 0 && self.compare_rows(&partition[i].1, &partition[i-1].1, order_by, ctx) == Ordering::Equal {
                                i -= 1;
                            }
                            i
                        }
                    }
                    _ => {
                        if is_end { current_idx + 1 } else { current_idx }
                    }
                }
            }
        }
    }

    fn execute_regular_select(
        &self,
        projection: &[SelectItem],
        rows: Vec<JoinedRow>,
        ctx: &mut ExecutionContext,
    ) -> Result<super::result::QueryResult, DbError> {
        let mut result = Vec::new();
        for row in &rows {
            let mut out = Vec::new();
            for item in projection {
                match &item.expr {
                    Expr::Wildcard => {
                        for ct in row {
                            if let Some(r) = &ct.row {
                                out.extend(r.values.clone());
                            }
                        }
                    }
                    expr => {
                        out.push(
                            eval_expr(expr, row, ctx, self.catalog, self.storage, self.clock)
                                .unwrap_or(Value::Null),
                        );
                    }
                }
            }
            result.push(out);
        }

        let columns: Vec<String> = projection
            .iter()
            .map(|i| i.alias.clone().unwrap_or_else(|| "".to_string()))
            .collect();

        let mut column_types = Vec::new();
        if !result.is_empty() {
            for val in &result[0] {
                column_types.push(val.data_type().unwrap_or(crate::types::DataType::VarChar { max_len: 4000 }));
            }
        } else {
            column_types = vec![crate::types::DataType::VarChar { max_len: 4000 }; columns.len()];
        }

        Ok(super::result::QueryResult {
            columns,
            column_types,
            rows: result,
        })
    }

    fn compare_rows(
        &self,
        a: &JoinedRow,
        b: &JoinedRow,
        order_by: &[OrderByExpr],
        ctx: &mut ExecutionContext,
    ) -> Ordering {
        for item in order_by {
            let val_a = eval_expr(&item.expr, a, ctx, self.catalog, self.storage, self.clock)
                .unwrap_or(Value::Null);
            let val_b = eval_expr(&item.expr, b, ctx, self.catalog, self.storage, self.clock)
                .unwrap_or(Value::Null);

            let ord = compare_values(&val_a, &val_b);
            if ord != Ordering::Equal {
                return if item.asc { ord } else { ord.reverse() };
            }
        }
        Ordering::Equal
    }
}

pub fn has_window_function(expr: &Expr) -> bool {
    match expr {
        Expr::WindowFunction { .. } => true,
        Expr::Binary { left, right, .. } => has_window_function(left) || has_window_function(right),
        Expr::Unary { expr: inner, .. } => has_window_function(inner),
        Expr::Cast { expr: inner, .. } | Expr::Convert { expr: inner, .. } | Expr::IsNull(inner) | Expr::IsNotNull(inner) => has_window_function(inner),
        Expr::Case { operand, when_clauses, else_result } => {
            let has_in_operand = operand.as_ref().map_or(false, |e| has_window_function(e));
            let has_in_when = when_clauses
                .iter()
                .any(|wc| has_window_function(&wc.condition) || has_window_function(&wc.result));
            let has_in_else = else_result.as_ref().map_or(false, |e| has_window_function(e));
            has_in_operand || has_in_when || has_in_else
        }
        Expr::FunctionCall { args, .. } => args.iter().any(has_window_function),
        Expr::InList { expr: inner, list, .. } => has_window_function(inner) || list.iter().any(has_window_function),
        Expr::Between { expr: inner, low, high, .. } => has_window_function(inner) || has_window_function(low) || has_window_function(high),
        Expr::Like { expr: inner, pattern, .. } => has_window_function(inner) || has_window_function(pattern),
        Expr::InSubquery { expr: inner, .. } => has_window_function(inner),
        _ => false,
    }
}

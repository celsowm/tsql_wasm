use crate::ast::{Expr, OrderByExpr, SelectItem, WindowFunc, WindowFrame, WindowFrameUnits, WindowFrameExtent, WindowFrameBound};
use crate::error::DbError;
use crate::types::Value;

use super::clock::Clock;
use super::context::ExecutionContext;
use super::evaluator::eval_expr;
use super::model::{JoinedRow, Group};
use super::value_ops::compare_values;
use super::aggregates::dispatch_aggregate;
use super::value_helpers::value_to_f64;
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

/// A wrapper around a row and its pre-evaluated sort keys for window processing.
struct WindowRow<'a> {
    original_idx: usize,
    row: &'a JoinedRow,
    partition_values: Vec<Value>,
    order_values: Vec<Value>,
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

        // Map to store calculated values: results_map[window_expr_debug_string][row_idx] = value
        let mut results_map: HashMap<String, Vec<Value>> = HashMap::new();

        // Process each window specification group
        for (spec, win_exprs) in &window_specs {
            // 1. Pre-evaluate all partition and order expressions (Schwartzian Transform)
            let mut window_rows: Vec<WindowRow> = rows.iter().enumerate().map(|(idx, row)| {
                let p_vals = spec.partition_by.iter()
                    .map(|e| eval_expr(e, row, ctx, self.catalog, self.storage, self.clock).unwrap_or(Value::Null))
                    .collect();
                let o_vals = spec.order_by.iter()
                    .map(|o| eval_expr(&o.expr, row, ctx, self.catalog, self.storage, self.clock).unwrap_or(Value::Null))
                    .collect();
                
                WindowRow {
                    original_idx: idx,
                    row,
                    partition_values: p_vals,
                    order_values: o_vals,
                }
            }).collect();

            // 2. Sort rows based on cached PARTITION BY and ORDER BY values
            window_rows.sort_by(|a, b| {
                // First by partition
                for i in 0..spec.partition_by.len() {
                    let ord = compare_values(&a.partition_values[i], &b.partition_values[i]);
                    if ord != Ordering::Equal {
                        return ord;
                    }
                }
                // Then by order
                for (i, order_expr) in spec.order_by.iter().enumerate() {
                    let ord = compare_values(&a.order_values[i], &b.order_values[i]);
                    if ord != Ordering::Equal {
                        return if order_expr.asc { ord } else { ord.reverse() };
                    }
                }
                // Stable sort by original index
                a.original_idx.cmp(&b.original_idx)
            });

            // 3. Identify partition boundaries
            let mut partition_starts = Vec::new();
            if !window_rows.is_empty() {
                partition_starts.push(0);
                for i in 1..window_rows.len() {
                    let mut in_same_partition = true;
                    for j in 0..spec.partition_by.len() {
                        if compare_values(&window_rows[i-1].partition_values[j], &window_rows[i].partition_values[j]) != Ordering::Equal {
                            in_same_partition = false;
                            break;
                        }
                    }
                    if !in_same_partition {
                        partition_starts.push(i);
                    }
                }
            }
            partition_starts.push(window_rows.len());

            // 4. Calculate window functions for each partition
            for p in 0..partition_starts.len() - 1 {
                let start = partition_starts[p];
                let end = partition_starts[p+1];
                let partition = &window_rows[start..end];

                let mut current_rank = 1;
                let mut current_dense_rank = 1;

                for (i, w_row) in partition.iter().enumerate() {
                    // Update rank/dense_rank if not the first row and not a peer of the previous row
                    if i > 0 {
                        let is_peer = self.are_peers(&partition[i-1], w_row);
                        if !is_peer {
                            current_rank = (i + 1) as i32;
                            current_dense_rank += 1;
                        }
                    }

                    for win_expr in win_exprs {
                        let val = match win_expr {
                            Expr::WindowFunction { func, args, .. } => {
                                match func {
                                    WindowFunc::RowNumber => Value::Int((i + 1) as i32),
                                    WindowFunc::Rank => Value::Int(current_rank),
                                    WindowFunc::DenseRank => Value::Int(current_dense_rank),
                                    WindowFunc::Lag => {
                                        let offset = args.get(1).and_then(|e| {
                                            if let Expr::Integer(n) = e { Some(*n as usize) } else { None }
                                        }).unwrap_or(1);
                                        if i >= offset {
                                            eval_expr(&args[0], partition[i - offset].row, ctx, self.catalog, self.storage, self.clock).unwrap_or(Value::Null)
                                        } else {
                                            args.get(2).map(|e| eval_expr(e, w_row.row, ctx, self.catalog, self.storage, self.clock).unwrap_or(Value::Null)).unwrap_or(Value::Null)
                                        }
                                    }
                                    WindowFunc::Lead => {
                                        let offset = args.get(1).and_then(|e| {
                                            if let Expr::Integer(n) = e { Some(*n as usize) } else { None }
                                        }).unwrap_or(1);
                                        if i + offset < partition.len() {
                                            eval_expr(&args[0], partition[i + offset].row, ctx, self.catalog, self.storage, self.clock).unwrap_or(Value::Null)
                                        } else {
                                            args.get(2).map(|e| eval_expr(e, w_row.row, ctx, self.catalog, self.storage, self.clock).unwrap_or(Value::Null)).unwrap_or(Value::Null)
                                        }
                                    }
                                    WindowFunc::FirstValue => {
                                        let frame_rows = self.get_frame_rows_optimized(partition, i, &spec.frame, &spec.order_by, ctx);
                                        if frame_rows.is_empty() {
                                            Value::Null
                                        } else {
                                            eval_expr(&args[0], frame_rows[0], ctx, self.catalog, self.storage, self.clock).unwrap_or(Value::Null)
                                        }
                                    }
                                    WindowFunc::LastValue => {
                                        let frame_rows = self.get_frame_rows_optimized(partition, i, &spec.frame, &spec.order_by, ctx);
                                        if frame_rows.is_empty() {
                                            Value::Null
                                        } else {
                                            eval_expr(&args[0], frame_rows[frame_rows.len() - 1], ctx, self.catalog, self.storage, self.clock).unwrap_or(Value::Null)
                                        }
                                    }
                                    WindowFunc::NTile => {
                                        let n_buckets = if let Some(e) = args.get(0) {
                                            match eval_expr(e, w_row.row, ctx, self.catalog, self.storage, self.clock) {
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
                                    WindowFunc::PercentileCont | WindowFunc::PercentileDisc => {
                                        let percentile = if let Some(e) = args.get(0) {
                                            match eval_expr(e, w_row.row, ctx, self.catalog, self.storage, self.clock) {
                                                Ok(Value::Float(v)) => f64::from_bits(v),
                                                Ok(v) => value_to_f64(&v).unwrap_or(f64::NAN),
                                                _ => f64::NAN,
                                            }
                                        } else {
                                            f64::NAN
                                        };

                                        if percentile.is_nan() || percentile < 0.0 || percentile > 1.0 {
                                            Value::Null
                                        } else {
                                            let mut values: Vec<f64> = Vec::new();
                                            for p_row in partition.iter() {
                                                if !p_row.order_values.is_empty() {
                                                    if let Ok(f) = value_to_f64(&p_row.order_values[0]) {
                                                        values.push(f);
                                                    }
                                                }
                                            }

                                            values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));

                                            if values.is_empty() {
                                                Value::Null
                                            } else {
                                                let n = values.len();
                                                let exact_index = percentile * (n - 1) as f64;
                                                let lo = exact_index.floor() as usize;
                                                let hi = exact_index.ceil() as usize;

                                                if func == &WindowFunc::PercentileDisc {
                                                     Value::Float(values[hi].to_bits())
                                                } else {
                                                    if lo == hi {
                                                        Value::Float(values[lo].to_bits())
                                                    } else {
                                                        let frac = exact_index - lo as f64;
                                                        let result = values[lo] * (1.0 - frac) + values[hi] * frac;
                                                        Value::Float(result.to_bits())
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    WindowFunc::PercentileRank => {
                                        if partition.len() <= 1 {
                                            Value::Float(0.0f64.to_bits())
                                        } else {
                                            let result = (current_rank - 1) as f64 / (partition.len() - 1) as f64;
                                            Value::Float(result.to_bits())
                                        }
                                    }
                                    WindowFunc::Aggregate(name) => {
                                        let frame_rows = self.get_frame_rows_optimized(partition, i, &spec.frame, &spec.order_by, ctx);
                                        let group = Group { key: vec![], rows: frame_rows.into_iter().cloned().collect() };
                                        let res = dispatch_aggregate(name.as_str(), args, &group, ctx, self.catalog, self.storage, self.clock);
                                        match res {
                                            Some(Ok(v)) => v,
                                            Some(Err(e)) => return Err(e),
                                            None => Value::Null,
                                        }
                                    }
                                }
                            }
                            _ => Value::Null,
                        };
                        let key = format!("{:?}", win_expr);
                        results_map.entry(key).or_insert_with(|| vec![Value::Null; rows.len()])[w_row.original_idx] = val;
                    }
                }
            }
        }

        // 5. Final pass to evaluate all projected expressions
        let mut final_projected_rows = Vec::with_capacity(rows.len());
        for (idx, row) in rows.iter().enumerate() {
            let mut window_map = HashMap::new();
            for (key, values) in &results_map {
                window_map.insert(key.clone(), values[idx].clone());
            }

            ctx.row.window_context = Some(window_map);
            let mut projected_row = Vec::with_capacity(projection.len());
            for item in projection {
                projected_row.push(eval_expr(&item.expr, row, ctx, self.catalog, self.storage, self.clock)?);
            }
            final_projected_rows.push(projected_row);
        }
        ctx.row.window_context = None;

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

    fn are_peers(&self, a: &WindowRow, b: &WindowRow) -> bool {
        if a.order_values.len() != b.order_values.len() { return false; }
        for i in 0..a.order_values.len() {
            if compare_values(&a.order_values[i], &b.order_values[i]) != Ordering::Equal {
                return false;
            }
        }
        true
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
            Expr::IsNull(inner) | Expr::IsNotNull(inner) | Expr::Cast { expr: inner, .. } | Expr::Convert { expr: inner, .. } | Expr::TryCast { expr: inner, .. } | Expr::TryConvert { expr: inner, .. } => {
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

    fn get_frame_rows_optimized<'b>(
        &self, 
        partition: &'b [WindowRow<'a>], 
        current_idx: usize, 
        frame_spec: &Option<WindowFrame>, 
        order_by: &[OrderByExpr], 
        _ctx: &mut ExecutionContext
    ) -> Vec<&'a JoinedRow> {
        let (start_idx, end_idx) = match frame_spec {
            None => {
                if order_by.is_empty() {
                    (0, partition.len())
                } else {
                    (0, self.resolve_bound_optimized(partition, current_idx, &WindowFrameBound::CurrentRow, true, WindowFrameUnits::Range))
                }
            }
            Some(f) => {
                match &f.extent {
                    WindowFrameExtent::Bound(b) => {
                        (self.resolve_bound_optimized(partition, current_idx, b, false, f.units),
                         self.resolve_bound_optimized(partition, current_idx, &WindowFrameBound::CurrentRow, true, f.units))
                    }
                    WindowFrameExtent::Between(b1, b2) => {
                        (self.resolve_bound_optimized(partition, current_idx, b1, false, f.units),
                         self.resolve_bound_optimized(partition, current_idx, b2, true, f.units))
                    }
                }
            }
        };

        let start_clamped = start_idx.min(partition.len());
        let end_clamped = end_idx.min(partition.len());

        if start_clamped >= end_clamped {
            return vec![];
        }

        partition[start_clamped..end_clamped].iter().map(|w| w.row).collect()
    }

    fn resolve_bound_optimized(&self, partition: &[WindowRow], current_idx: usize, bound: &WindowFrameBound, is_end: bool, units: WindowFrameUnits) -> usize {
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
                            let mut i = current_idx;
                            while i + 1 < partition.len() && self.are_peers(&partition[i], &partition[i+1]) {
                                i += 1;
                            }
                            i + 1
                        } else {
                            let mut i = current_idx;
                            while i > 0 && self.are_peers(&partition[i], &partition[i-1]) {
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
}

pub fn has_window_function(expr: &Expr) -> bool {
    match expr {
        Expr::WindowFunction { .. } => true,
        Expr::Binary { left, right, .. } => has_window_function(left) || has_window_function(right),
        Expr::Unary { expr: inner, .. } => has_window_function(inner),
        Expr::Cast { expr: inner, .. } | Expr::Convert { expr: inner, .. } | Expr::TryCast { expr: inner, .. } | Expr::TryConvert { expr: inner, .. } | Expr::IsNull(inner) | Expr::IsNotNull(inner) => has_window_function(inner),
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

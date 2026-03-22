use crate::ast::{Expr, OrderByExpr, SelectItem, WindowFunc};
use crate::error::DbError;
use crate::types::Value;

use super::clock::Clock;
use super::context::ExecutionContext;
use super::evaluator::eval_expr;
use super::model::JoinedRow;
use super::value_ops::compare_values;
use std::cmp::Ordering;

pub struct WindowExecutor<'a> {
    catalog: &'a dyn crate::catalog::Catalog,
    storage: &'a dyn crate::storage::Storage,
    clock: &'a dyn Clock,
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
            .any(|item| matches!(item.expr, Expr::WindowFunction { .. }));

        if !has_window {
            return self.execute_regular_select(projection, rows, ctx);
        }

        let window_cols: Vec<(usize, &WindowFunc, Vec<OrderByExpr>, Vec<Expr>, Vec<Expr>)> = projection
            .iter()
            .enumerate()
            .filter_map(|(idx, item)| {
                if let Expr::WindowFunction { func, args, partition_by, order_by, .. } = &item.expr {
                    Some((idx, func, order_by.clone(), partition_by.clone(), args.clone()))
                } else {
                    None
                }
            })
            .collect();

        let order_by = window_cols.get(0).map(|x| x.2.clone()).unwrap_or_default();
        let partition_by = window_cols.get(0).map(|x| x.3.clone()).unwrap_or_default();

        let mut sorted_rows = rows.clone();
        if !partition_by.is_empty() || !order_by.is_empty() {
            let pb = partition_by.clone();
            let ob = order_by.clone();
            sorted_rows.sort_by(|a, b| {
                for expr in &pb {
                    let va = eval_expr(expr, a, ctx, self.catalog, self.storage, self.clock).unwrap_or(Value::Null);
                    let vb = eval_expr(expr, b, ctx, self.catalog, self.storage, self.clock).unwrap_or(Value::Null);
                    let ord = compare_values(&va, &vb);
                    if ord != Ordering::Equal {
                        return ord;
                    }
                }
                self.compare_rows(a, b, &ob, ctx)
            });
        }

        let partitions = if !partition_by.is_empty() {
            self.compute_partitions(&sorted_rows, &partition_by, ctx)?
        } else {
            vec![sorted_rows]
        };

        let mut final_rows: Vec<Vec<Value>> = Vec::new();

        for partition in partitions {
            let n = partition.len();
            
            for i in 0..n {
                let mut result_row: Vec<Value> = Vec::with_capacity(projection.len());
                for item in projection {
                    match &item.expr {
                        Expr::WindowFunction { .. } => {
                            result_row.push(Value::Null);
                        }
                        expr => {
                            result_row.push(
                                eval_expr(expr, &partition[i], ctx, self.catalog, self.storage, self.clock)
                                    .unwrap_or(Value::Null),
                            );
                        }
                    }
                }

                for (col_pos, func, order_by_spec, _, func_args) in &window_cols {
                    let window_val = match func {
                        WindowFunc::RowNumber => Value::Int((i + 1) as i32),
                        WindowFunc::Rank => {
                            let mut rank = (i + 1) as i32;
                            for j in (0..i).rev() {
                                let cmp = self.compare_rows(&partition[j], &partition[i], order_by_spec, ctx);
                                if cmp == Ordering::Equal {
                                    rank = (j + 1) as i32;
                                } else {
                                    break;
                                }
                            }
                            Value::Int(rank)
                        }
                        WindowFunc::DenseRank => {
                            let mut dense_rank = 1;
                            for j in 1..=i {
                                let cmp = self.compare_rows(&partition[j - 1], &partition[j], order_by_spec, ctx);
                                if cmp != Ordering::Equal {
                                    dense_rank += 1;
                                }
                            }
                            Value::Int(dense_rank)
                        }
                        WindowFunc::Lag => {
                            if i >= 1 {
                                if let Some(arg_expr) = func_args.first() {
                                    eval_expr(arg_expr, &partition[i - 1], ctx, self.catalog, self.storage, self.clock)
                                        .unwrap_or(Value::Null)
                                } else {
                                    Value::Null
                                }
                            } else {
                                Value::Null
                            }
                        }
                        WindowFunc::Lead => {
                            if i + 1 < n {
                                if let Some(arg_expr) = func_args.first() {
                                    eval_expr(arg_expr, &partition[i + 1], ctx, self.catalog, self.storage, self.clock)
                                        .unwrap_or(Value::Null)
                                } else {
                                    Value::Null
                                }
                            } else {
                                Value::Null
                            }
                        }
                        WindowFunc::NTile => {
                            let buckets = func_args.first()
                                .and_then(|e| {
                                    if let Expr::Integer(n) = e { Some(*n as i32) } else { None }
                                })
                                .unwrap_or(2);
                            let bucket = ((i as i32 * buckets) / n as i32) + 1;
                            Value::Int(bucket.min(buckets))
                        }
                    };

                    result_row[*col_pos] = window_val;
                }

                final_rows.push(result_row);
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
            rows: final_rows,
        })
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

        Ok(super::result::QueryResult {
            columns,
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

    fn compute_partitions(
        &self,
        rows: &[JoinedRow],
        partition_by: &[Expr],
        ctx: &mut ExecutionContext,
    ) -> Result<Vec<Vec<JoinedRow>>, DbError> {
        let mut partitions: Vec<Vec<JoinedRow>> = Vec::new();
        let mut current_partition: Vec<JoinedRow> = Vec::new();

        for (i, row) in rows.iter().enumerate() {
            if i > 0 {
                let prev_row = &rows[i - 1];
                if !self.partition_equals(prev_row, row, partition_by, ctx)? {
                    if !current_partition.is_empty() {
                        partitions.push(current_partition);
                        current_partition = Vec::new();
                    }
                }
            }
            current_partition.push(row.clone());
        }

        if !current_partition.is_empty() {
            partitions.push(current_partition);
        }

        Ok(partitions)
    }

    fn partition_equals(
        &self,
        row1: &JoinedRow,
        row2: &JoinedRow,
        partition_by: &[Expr],
        ctx: &mut ExecutionContext,
    ) -> Result<bool, DbError> {
        if partition_by.is_empty() {
            return Ok(true);
        }

        let mut vals1 = Vec::new();
        let mut vals2 = Vec::new();

        for expr in partition_by {
            vals1.push(eval_expr(expr, row1, ctx, self.catalog, self.storage, self.clock)?);
            vals2.push(eval_expr(expr, row2, ctx, self.catalog, self.storage, self.clock)?);
        }

        Ok(vals1 == vals2)
    }
}

pub fn has_window_function(expr: &Expr) -> bool {
    match expr {
        Expr::WindowFunction { .. } => true,
        Expr::Binary { left, right, .. } => has_window_function(left) || has_window_function(right),
        Expr::Unary { expr, .. } => has_window_function(expr),
        Expr::Case { operand, when_clauses, else_result } => {
            let has_in_operand = operand.as_ref().map_or(false, |e| has_window_function(e));
            let has_in_when = when_clauses
                .iter()
                .any(|wc| has_window_function(&wc.condition) || has_window_function(&wc.result));
            let has_in_else = else_result.as_ref().map_or(false, |e| has_window_function(e));
            has_in_operand || has_in_when || has_in_else
        }
        _ => false,
    }
}
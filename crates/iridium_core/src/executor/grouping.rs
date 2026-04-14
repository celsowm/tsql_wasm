use std::cmp::Ordering;
use std::collections::HashMap;

use crate::ast::Expr;
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::storage::Storage;
use crate::types::Value;

use super::aggregates::{dispatch_aggregate, Group};
use super::clock::Clock;
use super::context::ExecutionContext;
use super::evaluator::eval_expr;
use super::model::{ContextTable, JoinedRow};
use super::projection::expand_projection_columns;
use super::value_ops::{compare_values, truthy};

pub(crate) struct GroupExecutor<'a> {
    pub catalog: &'a dyn Catalog,
    pub storage: &'a dyn Storage,
    pub clock: &'a dyn Clock,
}

fn eval_having_expr(
    expr: &Expr,
    row: &[ContextTable],
    group: &Group,
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    match expr {
        Expr::FunctionCall { name, args } => {
            if let Some(res) = dispatch_aggregate(name, args, group, ctx, catalog, storage, clock) {
                res
            } else {
                // Fallback to standard eval_expr if it's not an aggregate
                eval_expr(expr, row, ctx, catalog, storage, clock)
            }
        }
        Expr::Binary { left, op, right } => {
            let lv = eval_having_expr(left, row, group, ctx, catalog, storage, clock)?;
            let rv = eval_having_expr(right, row, group, ctx, catalog, storage, clock)?;
            super::operators::eval_binary(
                op,
                lv,
                rv,
                ctx.metadata.ansi_nulls,
                ctx.options.concat_null_yields_null,
                ctx.options.arithabort,
                ctx.options.ansi_warnings,
            )
        }
        Expr::Unary { op, expr: inner } => {
            let val = eval_having_expr(inner, row, group, ctx, catalog, storage, clock)?;
            super::operators::eval_unary(op, val)
        }
        Expr::Case {
            operand,
            when_clauses,
            else_result,
        } => {
            let operand_val = match operand {
                Some(e) => Some(eval_having_expr(
                    e, row, group, ctx, catalog, storage, clock,
                )?),
                None => None,
            };
            for clause in when_clauses {
                let match_found = if let Some(ref op_val) = operand_val {
                    let when_val = eval_having_expr(
                        &clause.condition,
                        row,
                        group,
                        ctx,
                        catalog,
                        storage,
                        clock,
                    )?;
                    matches!(compare_values(op_val, &when_val), Ordering::Equal)
                } else {
                    let cond = eval_having_expr(
                        &clause.condition,
                        row,
                        group,
                        ctx,
                        catalog,
                        storage,
                        clock,
                    )?;
                    truthy(&cond)
                };
                if match_found {
                    return eval_having_expr(
                        &clause.result,
                        row,
                        group,
                        ctx,
                        catalog,
                        storage,
                        clock,
                    );
                }
            }
            match else_result {
                Some(expr) => eval_having_expr(expr, row, group, ctx, catalog, storage, clock),
                None => Ok(Value::Null),
            }
        }
        Expr::Between {
            expr: between_expr,
            low,
            high,
            negated,
        } => {
            let val = eval_having_expr(between_expr, row, group, ctx, catalog, storage, clock)?;
            let low_val = eval_having_expr(low, row, group, ctx, catalog, storage, clock)?;
            let high_val = eval_having_expr(high, row, group, ctx, catalog, storage, clock)?;
            if val.is_null() || low_val.is_null() || high_val.is_null() {
                return Ok(Value::Null);
            }
            let ge_low = matches!(
                compare_values(&val, &low_val),
                Ordering::Greater | Ordering::Equal
            );
            let le_high = matches!(
                compare_values(&val, &high_val),
                Ordering::Less | Ordering::Equal
            );
            let result = ge_low && le_high;
            Ok(Value::Bit(if *negated { !result } else { result }))
        }
        _ => eval_expr(expr, row, ctx, catalog, storage, clock),
    }
}

fn eval_having_predicate(
    expr: &Expr,
    row: &[ContextTable],
    group: &Group,
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<bool, DbError> {
    let value = eval_having_expr(expr, row, group, ctx, catalog, storage, clock)?;
    let result = match &value {
        Value::Bit(v) => *v,
        Value::Null => false,
        other => truthy(other),
    };
    Ok(result)
}

impl<'a> GroupExecutor<'a> {
    pub fn build_groups(
        &self,
        rows: Vec<JoinedRow>,
        group_by: &[Expr],
        ctx: &mut ExecutionContext,
    ) -> Result<Vec<Group>, DbError> {
        if group_by.is_empty() {
            return Ok(vec![Group { key: vec![], rows }]);
        }
        let mut map: HashMap<Vec<Value>, usize> = HashMap::new();
        let mut groups: Vec<Group> = Vec::new();
        for row in rows {
            let mut key = Vec::new();
            for expr in group_by {
                key.push(eval_expr(
                    expr,
                    &row,
                    ctx,
                    self.catalog,
                    self.storage,
                    self.clock,
                )?);
            }

            if let Some(&idx) = map.get(&key) {
                groups[idx].rows.push(row);
            } else {
                let idx = groups.len();
                map.insert(key.clone(), idx);
                groups.push(Group {
                    key,
                    rows: vec![row],
                });
            }
        }
        Ok(groups)
    }

    pub fn execute_grouped_select(
        &self,
        projection: Vec<crate::ast::SelectItem>,
        rows: Vec<JoinedRow>,
        group_by: Vec<Expr>,
        having: Option<Expr>,
        ctx: &mut ExecutionContext,
    ) -> Result<super::result::QueryResult, DbError> {
        let groups = self.build_groups(rows, &group_by, ctx)?;
        let mut projected_rows = Vec::new();

        for group in groups {
            if let Some(having_expr) = &having {
                let sample_row = if group.rows.is_empty() {
                    vec![]
                } else {
                    group.rows[0].clone()
                };
                if !eval_having_predicate(
                    having_expr,
                    &sample_row,
                    &group,
                    ctx,
                    self.catalog,
                    self.storage,
                    self.clock,
                )? {
                    continue;
                }
            }
            projected_rows.push(self.project_group_row(&projection, &group, ctx)?);
        }

        let columns = expand_projection_columns(&projection, None);
        let mut column_types = Vec::new();
        if !projected_rows.is_empty() {
            for val in &projected_rows[0] {
                column_types.push(
                    val.data_type()
                        .unwrap_or(crate::types::DataType::VarChar { max_len: 4000 }),
                );
            }
        } else {
            column_types = vec![crate::types::DataType::VarChar { max_len: 4000 }; columns.len()];
        }
        let column_nullabilities = vec![true; columns.len()];
        Ok(super::result::QueryResult {
            columns,
            column_types,
            column_nullabilities,
            rows: projected_rows,
            ..Default::default()
        })
    }

    fn project_group_row(
        &self,
        projection: &[crate::ast::SelectItem],
        group: &Group,
        ctx: &mut ExecutionContext,
    ) -> Result<Vec<Value>, DbError> {
        let mut out = Vec::new();
        let sample_row = if group.rows.is_empty() {
            None
        } else {
            Some(&group.rows[0])
        };

        ctx.row.current_group = Some(group.clone());
        for item in projection {
            match &item.expr {
                Expr::Wildcard => {
                    if let Some(row) = sample_row {
                        out.extend(super::projection::expand_wildcard_values(row));
                    }
                }
                expr => {
                    let row_to_use = sample_row.cloned().unwrap_or_else(Vec::new);
                    out.push(eval_expr(
                        expr,
                        &row_to_use,
                        ctx,
                        self.catalog,
                        self.storage,
                        self.clock,
                    )?);
                }
            }
        }
        ctx.row.current_group = None;
        Ok(out)
    }
}

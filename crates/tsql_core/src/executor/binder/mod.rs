use crate::ast::Expr;
use crate::error::DbError;
use crate::types::{DataType, Value};

use crate::executor::context::ExecutionContext;
use crate::executor::evaluator::eval_expr;
use crate::executor::model::ContextTable;

pub use bind_expr::BoundExpr;

mod bind_expr;
mod eval_bound;

/// A column binding: (table_idx in the row, col_idx in the table, data_type).
#[derive(Debug, Clone)]
pub struct ColumnBinding {
    pub table_idx: usize,
    pub col_idx: usize,
    pub data_type: DataType,
}

/// Pre-binds all column references in an expression tree against a known row schema.
pub fn bind_expr(
    expr: &Expr,
    row: &[ContextTable],
    ctx: &ExecutionContext,
) -> Result<BoundExpr, DbError> {
    match expr {
        Expr::Identifier(name) => {
            if name.starts_with('@') {
                return Ok(BoundExpr::Dynamic(expr.clone()));
            }
            match resolve_column_in_row(row, name) {
                Some(binding) => Ok(BoundExpr::Column {
                    table_idx: binding.table_idx,
                    col_idx: binding.col_idx,
                    data_type: binding.data_type,
                }),
                None => {
                    if column_exists_in_apply_stack(&ctx.row.apply_stack, name)
                        || column_exists_in_outer_row(ctx.row.outer_row.as_deref(), name)
                    {
                        Ok(BoundExpr::Dynamic(expr.clone()))
                    } else {
                        Err(DbError::column_not_found(name))
                    }
                }
            }
        }
        Expr::QualifiedIdentifier(parts) => {
            if parts.len() != 2 {
                return Ok(BoundExpr::Dynamic(expr.clone()));
            }
            let table_name = &parts[0];
            let column_name = &parts[1];
            match resolve_qualified_in_row(row, table_name, column_name) {
                Some(binding) => Ok(BoundExpr::Column {
                    table_idx: binding.table_idx,
                    col_idx: binding.col_idx,
                    data_type: binding.data_type,
                }),
                None => {
                    if qualified_exists_in_apply_stack(
                        &ctx.row.apply_stack,
                        table_name,
                        column_name,
                    ) || qualified_exists_in_outer_row(
                        ctx.row.outer_row.as_deref(),
                        table_name,
                        column_name,
                    ) {
                        Ok(BoundExpr::Dynamic(expr.clone()))
                    } else {
                        Err(DbError::column_not_found_qualified(table_name, column_name))
                    }
                }
            }
        }
        Expr::Wildcard => Err(DbError::Execution(
            "wildcard is not a scalar expression".into(),
        )),
        Expr::QualifiedWildcard(_) => Err(DbError::Execution(
            "qualified wildcard is not a scalar expression".into(),
        )),
        Expr::Integer(v) => Ok(BoundExpr::Literal(
            if *v >= i32::MIN as i64 && *v <= i32::MAX as i64 {
                Value::Int(*v as i32)
            } else {
                Value::BigInt(*v)
            },
        )),
        Expr::FloatLiteral(s) => {
            Ok(BoundExpr::Literal(
                crate::executor::value_ops::parse_numeric_literal(s)?,
            ))
        }
        Expr::BinaryLiteral(b) => Ok(BoundExpr::Literal(Value::VarBinary(b.clone()))),
        Expr::String(s) => Ok(BoundExpr::Literal(Value::VarChar(s.clone()))),
        Expr::UnicodeString(s) => Ok(BoundExpr::Literal(Value::NVarChar(s.clone()))),
        Expr::Null => Ok(BoundExpr::Literal(Value::Null)),
        Expr::FunctionCall { name, args } => {
            let bound_args = args
                .iter()
                .map(|a| bind_expr(a, row, ctx))
                .collect::<Result<Vec<_>, _>>()?;
            if !bound_args
                .iter()
                .all(|arg| matches!(arg, BoundExpr::Literal(_)))
            {
                return Ok(BoundExpr::Dynamic(expr.clone()));
            }
            Ok(BoundExpr::FunctionCall {
                name: name.clone(),
                args: bound_args,
            })
        }
        Expr::Binary { left, op, right } => Ok(BoundExpr::Binary {
            left: Box::new(bind_expr(left, row, ctx)?),
            op: *op,
            right: Box::new(bind_expr(right, row, ctx)?),
        }),
        Expr::Unary { op, expr: inner } => Ok(BoundExpr::Unary {
            op: *op,
            expr: Box::new(bind_expr(inner, row, ctx)?),
        }),
        Expr::IsNull(inner) => Ok(BoundExpr::IsNull(Box::new(bind_expr(inner, row, ctx)?))),
        Expr::IsNotNull(inner) => Ok(BoundExpr::IsNotNull(Box::new(bind_expr(inner, row, ctx)?))),
        Expr::Cast {
            expr: inner,
            target,
        } => Ok(BoundExpr::Cast {
            expr: Box::new(bind_expr(inner, row, ctx)?),
            target: target.clone(),
        }),
        Expr::TryCast {
            expr: inner,
            target,
        } => Ok(BoundExpr::TryCast {
            expr: Box::new(bind_expr(inner, row, ctx)?),
            target: target.clone(),
        }),
        Expr::Convert {
            target,
            expr: inner,
            style,
        } => Ok(BoundExpr::Convert {
            target: target.clone(),
            expr: Box::new(bind_expr(inner, row, ctx)?),
            style: *style,
        }),
        Expr::TryConvert {
            target,
            expr: inner,
            style,
        } => Ok(BoundExpr::TryConvert {
            target: target.clone(),
            expr: Box::new(bind_expr(inner, row, ctx)?),
            style: *style,
        }),
        Expr::Case {
            operand,
            when_clauses,
            else_result,
        } => {
            let bound_operand = operand
                .as_ref()
                .map(|o| bind_expr(o, row, ctx))
                .transpose()?
                .map(Box::new);
            let bound_when_clauses = when_clauses
                .iter()
                .map(|wc| {
                    Ok((
                        bind_expr(&wc.condition, row, ctx)?,
                        bind_expr(&wc.result, row, ctx)?,
                    ))
                })
                .collect::<Result<Vec<_>, DbError>>()?;
            let bound_else = else_result
                .as_ref()
                .map(|e| bind_expr(e, row, ctx))
                .transpose()?
                .map(Box::new);
            Ok(BoundExpr::Case {
                operand: bound_operand,
                when_clauses: bound_when_clauses,
                else_result: bound_else,
            })
        }
        Expr::InList {
            expr: inner,
            list,
            negated,
        } => {
            let bound_inner = Box::new(bind_expr(inner, row, ctx)?);
            let bound_list = list
                .iter()
                .map(|e| bind_expr(e, row, ctx))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(BoundExpr::InList {
                expr: bound_inner,
                list: bound_list,
                negated: *negated,
            })
        }
        Expr::Between {
            expr: inner,
            low,
            high,
            negated,
        } => Ok(BoundExpr::Between {
            expr: Box::new(bind_expr(inner, row, ctx)?),
            low: Box::new(bind_expr(low, row, ctx)?),
            high: Box::new(bind_expr(high, row, ctx)?),
            negated: *negated,
        }),
        Expr::Like {
            expr: inner,
            pattern,
            negated,
        } => Ok(BoundExpr::Like {
            expr: Box::new(bind_expr(inner, row, ctx)?),
            pattern: Box::new(bind_expr(pattern, row, ctx)?),
            negated: *negated,
        }),
        Expr::WindowFunction { .. } => Ok(BoundExpr::WindowFunction {
            key: format!("{:?}", expr),
        }),
        Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } => {
            // Subqueries need the full execution context and can't be statically bound.
            Ok(BoundExpr::Dynamic(expr.clone()))
        }
    }
}

/// Evaluates a bound expression against a row.
/// Uses direct array indexing for `BoundExpr::Column` — zero string comparisons.
pub fn eval_bound_expr(
    bound: &BoundExpr,
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn crate::catalog::Catalog,
    storage: &dyn crate::storage::Storage,
    clock: &dyn crate::executor::clock::Clock,
) -> Result<Value, DbError> {
    match bound {
        BoundExpr::Column {
            table_idx, col_idx, ..
        } => {
            if let Some(table) = row.get(*table_idx) {
                if let Some(ref stored_row) = table.row {
                    if let Some(val) = stored_row.values.get(*col_idx) {
                        return Ok(val.clone());
                    }
                }
            }
            Ok(Value::Null)
        }
        BoundExpr::Dynamic(expr) => eval_expr(expr, row, ctx, catalog, storage, clock),
        _ => eval_bound::eval_bound_expr_inner(bound, row, ctx, catalog, storage, clock),
    }
}

fn resolve_column_in_row(row: &[ContextTable], name: &str) -> Option<ColumnBinding> {
    let mut found: Option<ColumnBinding> = None;
    for (table_idx, binding) in row.iter().enumerate() {
        for (col_idx, col) in binding.table.columns.iter().enumerate() {
            if col.name.eq_ignore_ascii_case(name) {
                if found.is_some() {
                    return None;
                }
                found = Some(ColumnBinding {
                    table_idx,
                    col_idx,
                    data_type: col.data_type.clone(),
                });
            }
        }
    }
    found
}

fn resolve_qualified_in_row(
    row: &[ContextTable],
    table_name: &str,
    column_name: &str,
) -> Option<ColumnBinding> {
    for (table_idx, binding) in row.iter().enumerate() {
        if binding.alias.eq_ignore_ascii_case(table_name)
            || binding.table.name.eq_ignore_ascii_case(table_name)
        {
            for (col_idx, col) in binding.table.columns.iter().enumerate() {
                if col.name.eq_ignore_ascii_case(column_name) {
                    return Some(ColumnBinding {
                        table_idx,
                        col_idx,
                        data_type: col.data_type.clone(),
                    });
                }
            }
            return None;
        }
    }
    None
}

fn column_exists_in_apply_stack(apply_stack: &[Vec<ContextTable>], name: &str) -> bool {
    for apply_row in apply_stack.iter().rev() {
        for binding in apply_row {
            for col in &binding.table.columns {
                if col.name.eq_ignore_ascii_case(name) {
                    return true;
                }
            }
        }
    }
    false
}

fn column_exists_in_outer_row(outer_row: Option<&[ContextTable]>, name: &str) -> bool {
    if let Some(row) = outer_row {
        for binding in row {
            for col in &binding.table.columns {
                if col.name.eq_ignore_ascii_case(name) {
                    return true;
                }
            }
        }
    }
    false
}

fn qualified_exists_in_apply_stack(
    apply_stack: &[Vec<ContextTable>],
    table_name: &str,
    column_name: &str,
) -> bool {
    for apply_row in apply_stack.iter().rev() {
        for binding in apply_row {
            if binding.alias.eq_ignore_ascii_case(table_name)
                || binding.table.name.eq_ignore_ascii_case(table_name)
            {
                for col in &binding.table.columns {
                    if col.name.eq_ignore_ascii_case(column_name) {
                        return true;
                    }
                }
            }
        }
    }
    false
}

fn qualified_exists_in_outer_row(
    outer_row: Option<&[ContextTable]>,
    table_name: &str,
    column_name: &str,
) -> bool {
    if let Some(row) = outer_row {
        for binding in row {
            if binding.alias.eq_ignore_ascii_case(table_name)
                || binding.table.name.eq_ignore_ascii_case(table_name)
            {
                for col in &binding.table.columns {
                    if col.name.eq_ignore_ascii_case(column_name) {
                        return true;
                    }
                }
            }
        }
    }
    false
}

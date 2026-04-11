use crate::error::DbError;
use crate::types::Value;

use super::super::clock::Clock;
use super::super::context::ExecutionContext;
use super::super::model::ContextTable;
use super::bind_expr::BoundExpr;
use crate::catalog::Catalog;
use crate::storage::Storage;

pub fn eval_bound_expr_inner(
    bound: &BoundExpr,
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    match bound {
        BoundExpr::Literal(val) => Ok(val.clone()),
        BoundExpr::Binary { left, op, right } => {
            let l = eval_bound_expr_inner(left, row, ctx, catalog, storage, clock)?;
            let r = eval_bound_expr_inner(right, row, ctx, catalog, storage, clock)?;
            super::super::operators::eval_binary(
                op,
                l,
                r,
                ctx.metadata.ansi_nulls,
                ctx.options.concat_null_yields_null,
                ctx.options.arithabort,
                ctx.options.ansi_warnings,
            )
        }
        BoundExpr::Unary { op, expr } => {
            let v = eval_bound_expr_inner(expr, row, ctx, catalog, storage, clock)?;
            super::super::operators::eval_unary(op, v)
        }
        BoundExpr::IsNull(expr) => {
            let v = eval_bound_expr_inner(expr, row, ctx, catalog, storage, clock)?;
            Ok(Value::Bit(v.is_null()))
        }
        BoundExpr::IsNotNull(expr) => {
            let v = eval_bound_expr_inner(expr, row, ctx, catalog, storage, clock)?;
            Ok(Value::Bit(!v.is_null()))
        }
        BoundExpr::Cast { expr, target } => {
            let v = eval_bound_expr_inner(expr, row, ctx, catalog, storage, clock)?;
            let rt = super::super::type_mapping::data_type_spec_to_runtime(target);
            super::super::value_ops::coerce_value_to_type_with_dateformat(
                v,
                &rt,
                &ctx.options.dateformat,
            )
        }
        BoundExpr::TryCast { expr, target } => {
            let v = eval_bound_expr_inner(expr, row, ctx, catalog, storage, clock)?;
            let rt = super::super::type_mapping::data_type_spec_to_runtime(target);
            match super::super::value_ops::coerce_value_to_type_with_dateformat(
                v,
                &rt,
                &ctx.options.dateformat,
            ) {
                Ok(val) => Ok(val),
                Err(_) => Ok(Value::Null),
            }
        }
        BoundExpr::Convert {
            target,
            expr,
            style: _,
        } => {
            let v = eval_bound_expr_inner(expr, row, ctx, catalog, storage, clock)?;
            let rt = super::super::type_mapping::data_type_spec_to_runtime(target);
            super::super::value_ops::coerce_value_to_type_with_dateformat(
                v,
                &rt,
                &ctx.options.dateformat,
            )
        }
        BoundExpr::TryConvert {
            target,
            expr,
            style: _,
        } => {
            let v = eval_bound_expr_inner(expr, row, ctx, catalog, storage, clock)?;
            let rt = super::super::type_mapping::data_type_spec_to_runtime(target);
            match super::super::value_ops::coerce_value_to_type_with_dateformat(
                v,
                &rt,
                &ctx.options.dateformat,
            ) {
                Ok(val) => Ok(val),
                Err(_) => Ok(Value::Null),
            }
        }
        BoundExpr::FunctionCall { name, args } => {
            // For function calls with bound args, we need to fall back to dynamic eval
            // because eval_function expects Expr trees, not pre-evaluated values.
            // We reconstruct a synthetic Expr tree with literal values.
            let arg_exprs = args
                .iter()
                .map(bound_to_literal_expr)
                .collect::<Result<Vec<_>, _>>()?;
            super::super::scalar::eval_function(name, &arg_exprs, row, ctx, catalog, storage, clock)
        }
        BoundExpr::Case {
            operand,
            when_clauses,
            else_result,
        } => {
            let operand_val = operand
                .as_ref()
                .map(|o| eval_bound_expr_inner(o, row, ctx, catalog, storage, clock))
                .transpose()?;
            for (cond, result) in when_clauses {
                let cond_val = eval_bound_expr_inner(cond, row, ctx, catalog, storage, clock)?;
                let matched = if let Some(ref op) = operand_val {
                    super::super::value_ops::truthy(&super::super::operators::eval_binary(
                        &crate::ast::BinaryOp::Eq,
                        op.clone(),
                        cond_val,
                        ctx.metadata.ansi_nulls,
                        ctx.options.concat_null_yields_null,
                        ctx.options.arithabort,
                        ctx.options.ansi_warnings,
                    )?)
                } else {
                    super::super::value_ops::truthy(&cond_val)
                };
                if matched {
                    return eval_bound_expr_inner(result, row, ctx, catalog, storage, clock);
                }
            }
            if let Some(else_expr) = else_result {
                eval_bound_expr_inner(else_expr, row, ctx, catalog, storage, clock)
            } else {
                Ok(Value::Null)
            }
        }
        BoundExpr::InList {
            expr,
            list,
            negated,
        } => {
            let val = eval_bound_expr_inner(expr, row, ctx, catalog, storage, clock)?;
            if val.is_null() {
                return Ok(Value::Null);
            }
            let mut found = false;
            let mut has_null = false;
            for item in list {
                let item_val = eval_bound_expr_inner(item, row, ctx, catalog, storage, clock)?;
                if item_val.is_null() {
                    has_null = true;
                    continue;
                }
                let equals = super::super::operators::eval_binary(
                    &crate::ast::BinaryOp::Eq,
                    val.clone(),
                    item_val,
                    ctx.metadata.ansi_nulls,
                    ctx.options.concat_null_yields_null,
                    ctx.options.arithabort,
                    ctx.options.ansi_warnings,
                )?;
                if super::super::value_ops::truthy(&equals) {
                    found = true;
                    break;
                }
            }
            let result = if *negated { !found } else { found };
            if result {
                Ok(Value::Bit(result))
            } else if has_null {
                Ok(Value::Null)
            } else {
                Ok(Value::Bit(false))
            }
        }
        BoundExpr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let val = eval_bound_expr_inner(expr, row, ctx, catalog, storage, clock)?;
            let lo = eval_bound_expr_inner(low, row, ctx, catalog, storage, clock)?;
            let hi = eval_bound_expr_inner(high, row, ctx, catalog, storage, clock)?;
            if val.is_null() || lo.is_null() || hi.is_null() {
                return Ok(Value::Null);
            }
            let ge_low = super::super::operators::eval_binary(
                &crate::ast::BinaryOp::Gte,
                val.clone(),
                lo,
                ctx.metadata.ansi_nulls,
                ctx.options.concat_null_yields_null,
                ctx.options.arithabort,
                ctx.options.ansi_warnings,
            )?;
            let le_high = super::super::operators::eval_binary(
                &crate::ast::BinaryOp::Lte,
                val,
                hi,
                ctx.metadata.ansi_nulls,
                ctx.options.concat_null_yields_null,
                ctx.options.arithabort,
                ctx.options.ansi_warnings,
            )?;
            let in_range = super::super::value_ops::truthy(&ge_low)
                && super::super::value_ops::truthy(&le_high);
            Ok(Value::Bit(if *negated { !in_range } else { in_range }))
        }
        BoundExpr::Like {
            expr,
            pattern,
            negated,
        } => {
            let val = eval_bound_expr_inner(expr, row, ctx, catalog, storage, clock)?;
            let pat = eval_bound_expr_inner(pattern, row, ctx, catalog, storage, clock)?;
            if val.is_null() || pat.is_null() {
                return Ok(Value::Null);
            }
            let val_str = val.to_string_value();
            let pat_str = pat.to_string_value();
            let matched = simple_like_match(&val_str, &pat_str);
            Ok(Value::Bit(if *negated { !matched } else { matched }))
        }
        BoundExpr::Subquery(_) | BoundExpr::Exists { .. } | BoundExpr::InSubquery { .. } => Err(
            DbError::Execution("subquery expression reached bound evaluator".into()),
        ),
        BoundExpr::WindowFunction { key } => ctx
            .get_window_value(key)
            .ok_or_else(|| DbError::Execution("window function value not found".into())),
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
        BoundExpr::Dynamic(expr) => {
            super::super::evaluator::eval_expr(expr, row, ctx, catalog, storage, clock)
        }
    }
}

/// Converts a BoundExpr that is a Literal back to an Expr::Integer/FloatLiteral/String/Null
/// so it can be passed to eval_function.
fn bound_to_literal_expr(bound: &BoundExpr) -> Result<crate::ast::Expr, DbError> {
    match bound {
        BoundExpr::Literal(Value::Int(v)) => Ok(crate::ast::Expr::Integer(*v as i64)),
        BoundExpr::Literal(Value::BigInt(v)) => Ok(crate::ast::Expr::Integer(*v)),
        BoundExpr::Literal(Value::Float(bits)) => {
            let f = f64::from_bits(*bits);
            Ok(crate::ast::Expr::FloatLiteral(f.to_string()))
        }
        BoundExpr::Literal(Value::Decimal(raw, scale)) => Ok(crate::ast::Expr::FloatLiteral(
            crate::types::format_decimal(*raw, *scale),
        )),
        BoundExpr::Literal(Value::VarChar(s)) => Ok(crate::ast::Expr::String(s.clone())),
        BoundExpr::Literal(Value::NVarChar(s)) => Ok(crate::ast::Expr::UnicodeString(s.clone())),
        BoundExpr::Literal(Value::Bit(b)) => {
            if *b {
                Ok(crate::ast::Expr::Integer(1))
            } else {
                Ok(crate::ast::Expr::Integer(0))
            }
        }
        BoundExpr::Literal(Value::Null) => Ok(crate::ast::Expr::Null),
        BoundExpr::Literal(Value::VarBinary(b)) => Ok(crate::ast::Expr::BinaryLiteral(b.clone())),
        _ => Err(DbError::Execution("non-literal in function arg".into())),
    }
}

fn simple_like_match(s: &str, pattern: &str) -> bool {
    let s_chars: Vec<char> = s.to_ascii_uppercase().chars().collect();
    let p_chars: Vec<char> = pattern.to_ascii_uppercase().chars().collect();
    simple_like_match_impl(&s_chars, 0, &p_chars, 0)
}

fn simple_like_match_impl(s: &[char], si: usize, p: &[char], pi: usize) -> bool {
    if pi >= p.len() {
        return si >= s.len();
    }
    match p[pi] {
        '%' => {
            if pi + 1 >= p.len() {
                return true;
            }
            for skip in 0..=(s.len().saturating_sub(si)) {
                if simple_like_match_impl(s, si + skip, p, pi + 1) {
                    return true;
                }
            }
            false
        }
        '_' => {
            if si >= s.len() {
                return false;
            }
            simple_like_match_impl(s, si + 1, p, pi + 1)
        }
        _ => {
            if si >= s.len() || s[si] != p[pi] {
                return false;
            }
            simple_like_match_impl(s, si + 1, p, pi + 1)
        }
    }
}

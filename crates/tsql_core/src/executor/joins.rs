use crate::ast::{BinaryOp, Expr};
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::storage::Storage;
use crate::types::Value;

use super::clock::Clock;
use super::context::ExecutionContext;
use super::evaluator::eval_expr;
use super::model::ContextTable;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JoinSide {
    Left,
    Right,
    Both,
    None,
}

fn combine_sides(s1: JoinSide, s2: JoinSide) -> JoinSide {
    match (s1, s2) {
        (JoinSide::None, other) | (other, JoinSide::None) => other,
        (JoinSide::Left, JoinSide::Left) => JoinSide::Left,
        (JoinSide::Right, JoinSide::Right) => JoinSide::Right,
        _ => JoinSide::Both,
    }
}

fn get_expr_side(expr: &Expr, left_shape: &[ContextTable], right_shape: &[ContextTable]) -> JoinSide {
    match expr {
        Expr::Identifier(name) => {
            let in_left = left_shape.iter().any(|b| b.table.columns.iter().any(|c| c.name.eq_ignore_ascii_case(name)));
            let in_right = right_shape.iter().any(|b| b.table.columns.iter().any(|c| c.name.eq_ignore_ascii_case(name)));
            match (in_left, in_right) {
                (true, false) => JoinSide::Left,
                (false, true) => JoinSide::Right,
                (true, true) => JoinSide::Both,
                (false, false) => JoinSide::None,
            }
        }
        Expr::QualifiedIdentifier(parts) => {
            if parts.len() != 2 { return JoinSide::None; }
            let table_name = &parts[0];
            let in_left = left_shape.iter().any(|b| b.alias.eq_ignore_ascii_case(table_name) || b.table.name.eq_ignore_ascii_case(table_name));
            let in_right = right_shape.iter().any(|b| b.alias.eq_ignore_ascii_case(table_name) || b.table.name.eq_ignore_ascii_case(table_name));
            match (in_left, in_right) {
                (true, false) => JoinSide::Left,
                (false, true) => JoinSide::Right,
                (true, true) => JoinSide::Both,
                (false, false) => JoinSide::None,
            }
        }
        Expr::Binary { left, right, .. } => {
            let l_side = get_expr_side(left, left_shape, right_shape);
            let r_side = get_expr_side(right, left_shape, right_shape);
            combine_sides(l_side, r_side)
        }
        Expr::Unary { expr, .. } => get_expr_side(expr, left_shape, right_shape),
        Expr::Cast { expr, .. } | Expr::TryCast { expr, .. } | Expr::Convert { expr, .. } | Expr::TryConvert { expr, .. } => get_expr_side(expr, left_shape, right_shape),
        Expr::FunctionCall { args, .. } => {
            let mut side = JoinSide::None;
            for arg in args {
                side = combine_sides(side, get_expr_side(arg, left_shape, right_shape));
            }
            side
        }
        _ => JoinSide::None,
    }
}

pub(crate) fn find_equi_join_conditions(
    on: &Expr,
    left_shape: &[ContextTable],
    right_shape: &[ContextTable],
) -> Option<(Vec<Expr>, Vec<Expr>)> {
    match on {
        Expr::Binary { left, op: BinaryOp::And, right } => {
            let (mut l1, mut r1) = find_equi_join_conditions(left, left_shape, right_shape)?;
            let (l2, r2) = find_equi_join_conditions(right, left_shape, right_shape)?;
            l1.extend(l2);
            r1.extend(r2);
            Some((l1, r1))
        }
        Expr::Binary { left, op: BinaryOp::Eq, right } => {
            let l_side = get_expr_side(left, left_shape, right_shape);
            let r_side = get_expr_side(right, left_shape, right_shape);
            match (l_side, r_side) {
                (JoinSide::Left, JoinSide::Right) => Some((vec![(**left).clone()], vec![(**right).clone()])),
                (JoinSide::Right, JoinSide::Left) => Some((vec![(**right).clone()], vec![(**left).clone()])),
                _ => None
            }
        }
        _ => None
    }
}

pub(crate) fn eval_key(
    exprs: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Vec<Value>, DbError> {
    let mut key = Vec::with_capacity(exprs.len());
    for expr in exprs {
        key.push(eval_expr(expr, row, ctx, catalog, storage, clock)?);
    }
    Ok(key)
}

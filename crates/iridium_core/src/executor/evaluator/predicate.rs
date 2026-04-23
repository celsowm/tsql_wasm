use crate::ast::Expr;
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::executor::clock::Clock;
use crate::executor::context::ExecutionContext;
use crate::executor::model::ContextTable;
use crate::executor::predicates::{
    eval_between, eval_case, eval_in_list, eval_like,
};
use crate::storage::Storage;
use crate::types::Value;

pub(crate) fn eval_predicate_expr(
    expr: &Expr,
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    match expr {
        Expr::Case {
            operand,
            when_clauses,
            else_result,
        } => eval_case(
            operand.as_deref(),
            when_clauses,
            else_result.as_deref(),
            row,
            ctx,
            catalog,
            storage,
            clock,
        ),
        Expr::InList {
            expr: in_expr,
            list,
            negated,
        } => eval_in_list(in_expr, list, *negated, row, ctx, catalog, storage, clock),
        Expr::Between {
            expr: between_expr,
            low,
            high,
            negated,
        } => eval_between(
            between_expr,
            low,
            high,
            *negated,
            row,
            ctx,
            catalog,
            storage,
            clock,
        ),
        Expr::Like {
            expr: like_expr,
            pattern,
            escape,
            negated,
        } => eval_like(
            like_expr,
            pattern,
            escape.as_deref(),
            *negated,
            row,
            ctx,
            catalog,
            storage,
            clock,
        ),
        _ => Err(DbError::Execution(
            "eval_predicate_expr called with non-predicate expression".into(),
        )),
    }
}

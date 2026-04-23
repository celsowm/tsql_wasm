use crate::ast::Expr;
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::executor::clock::Clock;
use crate::executor::context::ExecutionContext;
use crate::executor::model::ContextTable;
use crate::executor::predicates::{eval_exists, eval_in_subquery, eval_scalar_subquery};
use crate::storage::Storage;
use crate::types::Value;

pub(crate) fn eval_subquery_expr(
    expr: &Expr,
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    match expr {
        Expr::Subquery(stmt) => eval_scalar_subquery(stmt, row, ctx, catalog, storage, clock),
        Expr::Exists { subquery, negated } => {
            eval_exists(subquery, *negated, row, ctx, catalog, storage, clock)
        }
        Expr::InSubquery {
            expr: in_expr,
            subquery,
            negated,
        } => eval_in_subquery(
            in_expr, subquery, *negated, row, ctx, catalog, storage, clock,
        ),
        _ => Err(DbError::Execution(
            "eval_subquery_expr called with non-subquery expression".into(),
        )),
    }
}

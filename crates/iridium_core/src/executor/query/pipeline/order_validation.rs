use crate::error::DbError;

use crate::ast::OrderByExpr;
use crate::executor::projection::{expr_label, resolve_projected_order_index};

pub(crate) fn validate_projected_order_by(
    columns: &[String],
    order_by: &[OrderByExpr],
) -> Result<(), DbError> {
    for item in order_by {
        if resolve_projected_order_index(columns, item).is_none() {
            return Err(DbError::invalid_identifier(format!(
                "invalid column in ORDER BY: {}",
                expr_label(&item.expr)
            )));
        }
    }
    Ok(())
}

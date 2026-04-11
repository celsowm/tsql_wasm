use crate::catalog::TableDef;
use crate::error::DbError;
use crate::storage::StoredRow;
use crate::types::Value;

use super::super::super::clock::Clock;
use super::super::super::context::ExecutionContext;
use super::super::super::model::single_row_context;

pub(crate) fn validate_row_against_table(
    table: &TableDef,
    values: &[Value],
) -> Result<(), DbError> {
    for (col, value) in table.columns.iter().zip(values.iter()) {
        if !col.nullable && value.is_null() {
            return Err(DbError::Execution(format!(
                "column '{}' does not allow NULL",
                col.name
            )));
        }
    }
    Ok(())
}

pub(crate) fn enforce_checks_on_row(
    table: &TableDef,
    row: &StoredRow,
    ctx: &mut ExecutionContext<'_>,
    catalog: &mut dyn crate::catalog::Catalog,
    storage: &mut dyn crate::storage::Storage,
    clock: &dyn Clock,
) -> Result<(), DbError> {
    let joined = single_row_context(table, row.clone());
    for col in &table.columns {
        if let Some(check_expr) = &col.check {
            let check_val =
                super::super::super::evaluator::eval_expr(check_expr, &joined, ctx, catalog, storage, clock)?;
            if !check_val.is_null() && !super::super::super::value_ops::truthy(&check_val) {
                let cname = col
                    .check_constraint_name
                    .as_deref()
                    .unwrap_or("unnamed_check");
                return Err(DbError::Execution(format!(
                    "CHECK constraint '{}' violated",
                    cname
                )));
            }
        }
    }

    for chk in &table.check_constraints {
        let check_val =
            super::super::super::evaluator::eval_expr(&chk.expr, &joined, ctx, catalog, storage, clock)?;
        if !check_val.is_null() && !super::super::super::value_ops::truthy(&check_val) {
            return Err(DbError::Execution(format!(
                "CHECK constraint '{}' violated",
                chk.name
            )));
        }
    }

    Ok(())
}

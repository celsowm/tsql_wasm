use crate::ast::Assignment;
use crate::catalog::{Catalog, TableDef};
use crate::error::DbError;
use crate::storage::{Storage, StoredRow};

use super::super::super::clock::Clock;
use super::super::super::context::ExecutionContext;
use super::super::super::evaluator::eval_expr_to_type_in_context;
use super::super::super::model::single_row_context;
use super::padding::{apply_ansi_padding, enforce_string_length};

#[allow(clippy::too_many_arguments)]
pub(crate) fn apply_assignments(
    table: &TableDef,
    row: &mut StoredRow,
    assignments: &[Assignment],
    joined: &super::super::super::model::JoinedRow,
    ctx: &mut ExecutionContext<'_>,
    catalog: &mut dyn Catalog,
    storage: &mut dyn Storage,
    clock: &dyn Clock,
) -> Result<(), DbError> {
    for assignment in assignments {
        let idx = table
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(&assignment.column))
            .ok_or_else(|| DbError::column_not_found(&assignment.column))?;
        let target = &table.columns[idx].data_type;
        if table.columns[idx].computed_expr.is_some() {
            return Err(DbError::Execution(format!(
                "cannot update computed column '{}'",
                table.columns[idx].name
            )));
        }
        let value = eval_expr_to_type_in_context(
            &assignment.expr,
            target,
            joined,
            ctx,
            catalog,
            storage,
            clock,
        )?;
        let mut value = value;
        apply_ansi_padding(&mut value, target, table.columns[idx].ansi_padding_on);
        enforce_string_length(target, &value, &table.columns[idx].name)?;
        row.values[idx] = value;
    }

    for (idx, col) in table.columns.iter().enumerate() {
        if let Some(computed) = &col.computed_expr {
            let snapshot = row.clone();
            let joined = single_row_context(table, snapshot);
            let value =
                super::super::super::evaluator::eval_expr(computed, &joined, ctx, catalog, storage, clock)?;
            let mut value = value;
            apply_ansi_padding(&mut value, &col.data_type, col.ansi_padding_on);
            enforce_string_length(&col.data_type, &value, &col.name)?;
            row.values[idx] = value;
        }
    }

    for (col, value) in table.columns.iter().zip(row.values.iter_mut()) {
        apply_ansi_padding(value, &col.data_type, col.ansi_padding_on);
        enforce_string_length(&col.data_type, value, &col.name)?;
    }
    Ok(())
}

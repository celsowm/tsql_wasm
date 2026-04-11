mod analysis;
mod distinct;
pub mod iterator;
mod order;
mod order_validation;
mod pagination;
mod pagination_value;
mod projection;

use crate::error::DbError;

use crate::executor::context::ExecutionContext;
use crate::executor::model::JoinedRow;
use crate::executor::result;

use super::plan::RelationalQuery;
use super::QueryExecutor;

pub(crate) fn execute_rows_to_result(
    executor: &QueryExecutor<'_>,
    query: &RelationalQuery,
    mut source_rows: Vec<JoinedRow>,
    ctx: &mut ExecutionContext,
) -> Result<result::QueryResult, DbError> {
    let state = analysis::PipelineState::new(query);

    if state.needs_pre_sort {
        order::apply_source_ordering(executor, query, &mut source_rows, ctx)?;
    }

    let result = projection::execute_projection_stage(executor, query, source_rows, ctx, &state)?;
    let result_columns = result.columns.clone();
    let result_column_types = result.column_types.clone();
    let mut final_rows = result.rows;

    if query.projection.distinct {
        final_rows = distinct::deduplicate_rows(final_rows);
    }

    if !query.sort.order_by.is_empty() && !state.needs_pre_sort {
        final_rows =
            order::apply_result_ordering(executor, query, &result_columns, final_rows, ctx)?;
    }

    final_rows = pagination::apply_pagination(executor, query, final_rows, ctx)?;

    Ok(result::QueryResult {
        columns: result_columns,
        column_types: result_column_types,
        rows: final_rows,
        ..Default::default()
    })
}

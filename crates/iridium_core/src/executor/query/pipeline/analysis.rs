use crate::ast::Expr;

use crate::executor::aggregates::is_aggregate_function;
use crate::executor::projection::{expand_projection_columns, resolve_projected_order_index};
use crate::executor::query::plan::RelationalQuery;
use crate::executor::window::has_window_function;

#[derive(Debug, Clone)]
pub(crate) struct PipelineState {
    pub(crate) has_aggregate: bool,
    pub(crate) has_window: bool,
    pub(crate) needs_pre_sort: bool,
}

impl PipelineState {
    pub(crate) fn new(query: &RelationalQuery) -> Self {
        let has_aggregate = query
            .projection
            .items
            .iter()
            .any(|item| matches!(&item.expr, Expr::FunctionCall { name, .. } if is_aggregate_function(name)));

        let has_window = query
            .projection
            .items
            .iter()
            .any(|item| has_window_function(&item.expr));

        let projection_columns: Vec<String> = query
            .projection
            .items
            .iter()
            .flat_map(|item| expand_projection_columns(std::slice::from_ref(item), None))
            .collect();

        let needs_pre_sort = requires_pre_sort(query, &projection_columns, has_aggregate);

        Self {
            has_aggregate,
            has_window,
            needs_pre_sort,
        }
    }
}

fn requires_pre_sort(
    query: &RelationalQuery,
    projection_columns: &[String],
    has_aggregate: bool,
) -> bool {
    !query.sort.order_by.is_empty()
        && !has_aggregate
        && query
            .sort
            .order_by
            .iter()
            .any(|ob| resolve_projected_order_index(projection_columns, ob).is_none())
}

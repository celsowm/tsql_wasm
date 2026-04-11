use crate::ast::{TableFactor, TableRef};
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::storage::Storage;

use crate::executor::clock::Clock;
use crate::executor::context::ExecutionContext;
use crate::executor::model::BoundTable;
use super::super::plan::RelationalQuery;
use super::query_result_to_bound_table;

pub(super) fn bind_view(
    catalog: &dyn Catalog,
    _storage: &dyn Storage,
    _clock: &dyn Clock,
    tref: &TableRef,
    ctx: &mut ExecutionContext,
    query_executor_proxy: &impl Fn(RelationalQuery, &mut ExecutionContext) -> Result<crate::executor::result::QueryResult, DbError>,
) -> Result<Option<BoundTable>, DbError> {
    let schema = tref.factor.as_object_name().map(|o| o.schema_or_dbo()).unwrap_or("dbo");
    let name = match &tref.factor {
        TableFactor::Named(o) => &o.name,
        TableFactor::Derived(_) => return Ok(None),
        TableFactor::Values { .. } => return Ok(None),
    };

    let Some(view) = catalog.find_view(schema, name).cloned() else {
        return Ok(None);
    };

    let view_query = match view.query {
        crate::ast::Statement::Dml(crate::ast::DmlStatement::Select(s)) => s,
        _ => return Err(DbError::Execution("view query must be SELECT".into())),
    };

    let result = query_executor_proxy(view_query.into(), ctx)?;
    Ok(Some(query_result_to_bound_table(
        tref.alias.clone().unwrap_or_else(|| name.clone()),
        name.clone(),
        result,
    )))
}



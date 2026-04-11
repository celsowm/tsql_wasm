use crate::catalog::Catalog;
use crate::error::DbError;
use crate::executor::model::BoundTable;

use super::binder;
use super::QueryExecutor;
use crate::executor::context::ExecutionContext;

pub(crate) fn bind_table(
    executor: &QueryExecutor<'_>,
    catalog: &dyn Catalog,
    tref: crate::ast::TableRef,
    ctx: &mut ExecutionContext,
) -> Result<BoundTable, DbError> {
    binder::bind_table(catalog, executor.storage, executor.clock, tref, ctx, executor)
}

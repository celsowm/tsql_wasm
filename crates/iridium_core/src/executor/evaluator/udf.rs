use crate::ast::Statement;
use crate::catalog::Catalog;
use crate::error::{DbError, StmtOutcome};
use crate::executor::clock::Clock;
use crate::executor::context::ExecutionContext;
use crate::executor::script::ScriptExecutor;
use crate::storage::Storage;
use crate::types::Value;

pub(crate) fn eval_udf_body<'a>(
    stmts: &[Statement],
    ctx: &mut ExecutionContext<'_>,
    catalog: &'a dyn Catalog,
    storage: &'a dyn Storage,
    clock: &'a dyn Clock,
) -> Result<Value, DbError> {
    let mut catalog_owned = catalog.clone_boxed();
    let mut storage_owned = storage.clone_boxed();

    let mut executor = ScriptExecutor {
        catalog: catalog_owned.as_mut(),
        storage: storage_owned.as_mut(),
        clock,
    };
    match executor.execute_batch(stmts, ctx) {
        Ok(StmtOutcome::Return(Some(val))) => Ok(val),
        Ok(StmtOutcome::Return(None)) => Ok(Value::Null),
        Ok(StmtOutcome::Ok(_)) => Ok(Value::Null),
        Ok(_) => Ok(Value::Null),
        Err(e) => Err(e),
    }
}

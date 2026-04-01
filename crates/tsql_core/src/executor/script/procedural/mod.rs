pub(crate) mod assignment;
pub(crate) mod control_flow;
pub(crate) mod cursor;
pub(crate) mod raiserror;
pub(crate) mod try_catch;
pub(crate) mod routine;
pub(crate) mod print;
pub(crate) mod variable;

use crate::error::DbError;
use crate::executor::context::ExecutionContext;
use super::ScriptExecutor;

impl<'a> ScriptExecutor<'a> {
    pub(crate) fn leave_scope_and_cleanup(&mut self, ctx: &mut ExecutionContext) -> Result<(), DbError> {
        let dropped_physical = ctx.leave_scope_collect_table_vars();
        for physical in dropped_physical {
            if self.catalog.find_table("dbo", &physical).is_none() {
                continue;
            }
            crate::executor::schema::SchemaExecutor {
                catalog: self.catalog,
                storage: self.storage,
            }
            .drop_table(crate::ast::DropTableStmt {
                name: crate::ast::ObjectName {
                    schema: Some("dbo".to_string()),
                    name: physical,
                },
            })?;
        }
        Ok(())
    }
}

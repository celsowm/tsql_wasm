use super::super::ScriptExecutor;
use crate::ast::{CreateFunctionStmt, CreateProcedureStmt, CreateTriggerStmt, CreateViewStmt};
use crate::error::DbError;
use crate::executor::tooling::{format_routine_definition, format_trigger_definition};

impl<'a> ScriptExecutor<'a> {
    pub(crate) fn execute_create_procedure(
        &mut self,
        stmt: CreateProcedureStmt,
    ) -> Result<(), DbError> {
        let schema = stmt.name.schema_or_dbo().to_string();
        let mut routine = crate::catalog::RoutineDef {
            object_id: self.catalog.alloc_object_id(),
            schema,
            name: stmt.name.name,
            params: stmt.params,
            kind: crate::catalog::RoutineKind::Procedure { body: stmt.body },
            definition_sql: String::new(),
        };
        routine.definition_sql = format_routine_definition(&routine);
        self.catalog.create_routine(routine)?;
        Ok(())
    }

    pub(crate) fn execute_create_function(
        &mut self,
        stmt: CreateFunctionStmt,
    ) -> Result<(), DbError> {
        let schema = stmt.name.schema_or_dbo().to_string();
        let mut routine = crate::catalog::RoutineDef {
            object_id: self.catalog.alloc_object_id(),
            schema,
            name: stmt.name.name,
            params: stmt.params,
            kind: crate::catalog::RoutineKind::Function {
                returns: stmt.returns,
                body: stmt.body,
            },
            definition_sql: String::new(),
        };
        routine.definition_sql = format_routine_definition(&routine);
        self.catalog.create_routine(routine)?;
        Ok(())
    }

    pub(crate) fn execute_create_view(
        &mut self,
        stmt: CreateViewStmt,
        ctx: &mut crate::executor::context::ExecutionContext<'_>,
    ) -> Result<(), DbError> {
        self.schema(ctx).create_view(stmt)
    }

    pub(crate) fn execute_create_trigger(
        &mut self,
        stmt: CreateTriggerStmt,
    ) -> Result<(), DbError> {
        let schema = stmt.name.schema_or_dbo().to_string();
        let mut trigger = crate::catalog::TriggerDef {
            object_id: self.catalog.alloc_object_id(),
            schema,
            name: stmt.name.name,
            table_schema: stmt.table.schema_or_dbo().to_string(),
            table_name: stmt.table.name,
            events: stmt.events,
            is_instead_of: stmt.is_instead_of,
            body: stmt.body,
            definition_sql: String::new(),
        };
        trigger.definition_sql = format_trigger_definition(&trigger);
        self.catalog.create_trigger(trigger)?;
        Ok(())
    }
}

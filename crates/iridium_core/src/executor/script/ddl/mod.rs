use super::ScriptExecutor;
use crate::ast::{AlterTableStmt, CreateTableStmt, DropTableStmt, TruncateTableStmt};
use crate::error::DbError;
use crate::executor::context::ExecutionContext;
use crate::executor::mutation::MutationExecutor;
use crate::executor::result::QueryResult;

mod temp_table;

impl<'a> ScriptExecutor<'a> {
    pub(crate) fn execute_ddl(
        &mut self,
        ddl: crate::ast::DdlStatement,
        ctx: &mut ExecutionContext<'_>,
    ) -> Result<Option<QueryResult>, DbError> {
        use crate::ast::DdlStatement;
        match ddl {
            DdlStatement::CreateTable(stmt) => self.execute_create_table(stmt, ctx),
            DdlStatement::DropTable(stmt) => self.execute_drop_table(stmt, ctx),
            DdlStatement::CreateType(stmt) => {
                self.schema(ctx).create_type(stmt)?;
                Ok(None)
            }
            DdlStatement::DropType(stmt) => {
                self.schema(ctx).drop_type(stmt)?;
                Ok(None)
            }
            DdlStatement::CreateIndex(stmt) => {
                self.schema(ctx).create_index(stmt)?;
                Ok(None)
            }
            DdlStatement::DropIndex(stmt) => {
                self.schema(ctx).drop_index(stmt)?;
                Ok(None)
            }
            DdlStatement::CreateSchema(stmt) => {
                self.schema(ctx).create_schema(stmt)?;
                Ok(None)
            }
            DdlStatement::DropSchema(stmt) => {
                self.schema(ctx).drop_schema(stmt)?;
                Ok(None)
            }
            DdlStatement::DropProcedure(stmt) => {
                let schema = stmt.name.schema_or_dbo().to_string();
                self.catalog.drop_routine(&schema, &stmt.name.name, false)?;
                Ok(None)
            }
            DdlStatement::DropFunction(stmt) => {
                let schema = stmt.name.schema_or_dbo().to_string();
                self.catalog.drop_routine(&schema, &stmt.name.name, true)?;
                Ok(None)
            }
            DdlStatement::DropTrigger(stmt) => {
                let schema = stmt.name.schema_or_dbo().to_string();
                self.catalog.drop_trigger(&schema, &stmt.name.name)?;
                Ok(None)
            }
            DdlStatement::TruncateTable(stmt) => self.execute_truncate_table(stmt, ctx),
            DdlStatement::AlterTable(stmt) => self.execute_alter_table(stmt, ctx),
            DdlStatement::DropView(stmt) => self.execute_drop_view(stmt, ctx),
            DdlStatement::CreateSynonym(stmt) => {
                self.schema(ctx).create_synonym(stmt)?;
                Ok(None)
            }
            DdlStatement::DropSynonym(stmt) => {
                self.schema(ctx).drop_synonym(stmt)?;
                Ok(None)
            }
            DdlStatement::CreateSequence(stmt) => {
                self.schema(ctx).create_sequence(stmt)?;
                Ok(None)
            }
            DdlStatement::DropSequence(stmt) => {
                self.schema(ctx).drop_sequence(stmt)?;
                Ok(None)
            }
        }
    }

    pub(crate) fn execute_drop_view(
        &mut self,
        stmt: crate::ast::DropViewStmt,
        ctx: &mut ExecutionContext<'_>,
    ) -> Result<Option<QueryResult>, DbError> {
        self.schema(ctx).drop_view(stmt)?;
        Ok(None)
    }

    pub(crate) fn execute_create_table(
        &mut self,
        mut stmt: CreateTableStmt,
        ctx: &mut ExecutionContext<'_>,
    ) -> Result<Option<QueryResult>, DbError> {
        temp_table::map_create_temp_table(&mut stmt, ctx);
        self.schema(ctx).create_table(stmt)?;
        Ok(None)
    }

    pub(crate) fn execute_drop_table(
        &mut self,
        mut stmt: DropTableStmt,
        ctx: &mut ExecutionContext<'_>,
    ) -> Result<Option<QueryResult>, DbError> {
        temp_table::resolve_drop_table_name(&mut stmt, ctx);
        self.schema(ctx).drop_table(stmt)?;
        Ok(None)
    }

    pub(crate) fn execute_truncate_table(
        &mut self,
        mut stmt: TruncateTableStmt,
        ctx: &mut ExecutionContext<'_>,
    ) -> Result<Option<QueryResult>, DbError> {
        if let Some(_mapped) = temp_table::resolve_table_name_for_mutation(&mut stmt.name, ctx) {
            // resolved
        }
        let schema = stmt.name.schema_or_dbo();
        let table_name = &stmt.name.name;
        let table = self
            .catalog
            .find_table(schema, table_name)
            .ok_or_else(|| DbError::table_not_found(schema, table_name))?
            .clone();
        self.storage.clear_table(table.id)?;

        let mut_exec = MutationExecutor {
            catalog: self.catalog,
            storage: self.storage,
            clock: self.clock,
        };
        mut_exec.push_dirty_truncate(ctx, &table.name);
        Ok(None)
    }

    pub(crate) fn execute_alter_table(
        &mut self,
        mut stmt: AlterTableStmt,
        ctx: &mut ExecutionContext<'_>,
    ) -> Result<Option<QueryResult>, DbError> {
        if let Some(_mapped) = temp_table::resolve_table_name_for_mutation(&mut stmt.table, ctx) {
            // resolved
        }
        let schema = stmt.table.schema_or_dbo();
        let table_name = &stmt.table.name;
        let table = self
            .catalog
            .find_table(schema, table_name)
            .ok_or_else(|| DbError::table_not_found(schema, table_name))?
            .clone();

        self.schema(ctx).alter_table(stmt)?;

        let rows = {
            let rows = match self.storage.scan_rows(table.id) {
                Ok(rows) => rows,
                Err(_) => return Ok(None),
            };
            match rows.collect::<Result<Vec<_>, DbError>>() {
                Ok(rows) => rows,
                Err(_) => return Ok(None),
            }
        };

        let mut_exec = MutationExecutor {
            catalog: self.catalog,
            storage: self.storage,
            clock: self.clock,
        };
        mut_exec.push_dirty_replace(ctx, &table.name, rows);

        Ok(None)
    }
}

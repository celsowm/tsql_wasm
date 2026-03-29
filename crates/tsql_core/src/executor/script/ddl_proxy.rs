use crate::ast::{AlterTableStmt, CreateTableStmt, DropTableStmt, TruncateTableStmt};
use crate::error::DbError;
use crate::executor::context::ExecutionContext;
use crate::executor::result::QueryResult;
use crate::executor::schema::SchemaExecutor;
use super::ScriptExecutor;

impl<'a> ScriptExecutor<'a> {
    pub(crate) fn execute_create_table(
        &mut self,
        mut stmt: CreateTableStmt,
        ctx: &mut ExecutionContext,
    ) -> Result<Option<QueryResult>, DbError> {
        if stmt.name.name.starts_with('#') {
            let logical = stmt.name.name.clone();
            let physical = format!("__temp_{}", logical.trim_start_matches('#'));
            ctx.temp_table_map
                .insert(logical.to_uppercase(), physical.clone());
            stmt.name.schema = Some("dbo".to_string());
            stmt.name.name = physical;
        }
        SchemaExecutor {
            catalog: self.catalog,
            storage: self.storage,
        }
        .create_table(stmt)?;
        Ok(None)
    }

    pub(crate) fn execute_drop_table(
        &mut self,
        mut stmt: DropTableStmt,
        ctx: &mut ExecutionContext,
    ) -> Result<Option<QueryResult>, DbError> {
        if stmt.name.name.starts_with('#') {
            let key = stmt.name.name.to_uppercase();
            if let Some(mapped) = ctx.temp_table_map.remove(&key) {
                stmt.name.schema = Some("dbo".to_string());
                stmt.name.name = mapped;
            }
        } else if stmt.name.name.starts_with('@') {
            if let Some(mapped) = ctx.resolve_table_name(&stmt.name.name) {
                stmt.name.schema = Some("dbo".to_string());
                stmt.name.name = mapped;
            }
        }
        SchemaExecutor {
            catalog: self.catalog,
            storage: self.storage,
        }
        .drop_table(stmt)?;
        Ok(None)
    }

    pub(crate) fn execute_truncate_table(
        &mut self,
        mut stmt: TruncateTableStmt,
        ctx: &mut ExecutionContext,
    ) -> Result<Option<QueryResult>, DbError> {
        if let Some(mapped) = ctx.resolve_table_name(&stmt.name.name) {
            stmt.name.name = mapped;
            if stmt.name.schema.is_none() {
                stmt.name.schema = Some("dbo".to_string());
            }
        }
        let schema = stmt.name.schema_or_dbo();
        let table_name = &stmt.name.name;
        let table = self
            .catalog
            .find_table(schema, table_name)
            .ok_or_else(|| {
                DbError::Semantic(format!("table '{}.{}' not found", schema, table_name))
            })?
            .clone();
        self.storage.clear_table(table.id)?;
        self.push_dirty_truncate(ctx, &table.name);
        Ok(None)
    }

    pub(crate) fn execute_alter_table(
        &mut self,
        mut stmt: AlterTableStmt,
        ctx: &mut ExecutionContext,
    ) -> Result<Option<QueryResult>, DbError> {
        if let Some(mapped) = ctx.resolve_table_name(&stmt.table.name) {
            stmt.table.name = mapped;
            if stmt.table.schema.is_none() {
                stmt.table.schema = Some("dbo".to_string());
            }
        }
        let schema = stmt.table.schema_or_dbo();
        let table_name = &stmt.table.name;
        let table = self
            .catalog
            .find_table(schema, table_name)
            .ok_or_else(|| {
                DbError::Semantic(format!("table '{}.{}' not found", schema, table_name))
            })?
            .clone();

        SchemaExecutor {
            catalog: self.catalog,
            storage: self.storage,
        }
        .alter_table(stmt)?;

        if let Ok(rows) = self.storage.get_rows(table.id) {
            self.push_dirty_replace(ctx, &table.name, rows);
        }

        Ok(None)
    }
}

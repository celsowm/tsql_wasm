use crate::ast::{AlterTableStmt, CreateTableStmt, DropTableStmt, TruncateTableStmt};
use crate::error::DbError;
use crate::executor::context::ExecutionContext;
use crate::executor::result::QueryResult;
use crate::executor::mutation::MutationExecutor;
use crate::executor::string_norm::normalize_identifier;
use super::ScriptExecutor;

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
                self.schema().create_type(stmt)?;
                Ok(None)
            }
            DdlStatement::DropType(stmt) => {
                self.schema().drop_type(stmt)?;
                Ok(None)
            }
            DdlStatement::CreateIndex(stmt) => {
                self.schema().create_index(stmt)?;
                Ok(None)
            }
            DdlStatement::DropIndex(stmt) => {
                self.schema().drop_index(stmt)?;
                Ok(None)
            }
            DdlStatement::CreateSchema(stmt) => {
                self.schema().create_schema(stmt)?;
                Ok(None)
            }
            DdlStatement::DropSchema(stmt) => {
                self.schema().drop_schema(stmt)?;
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
        }
    }

    pub(crate) fn execute_drop_view(
        &mut self,
        stmt: crate::ast::DropViewStmt,
        _ctx: &mut ExecutionContext<'_>,
    ) -> Result<Option<QueryResult>, DbError> {
        self.schema().drop_view(stmt)?;
        Ok(None)
    }

    pub(crate) fn execute_create_table(
        &mut self,
        mut stmt: CreateTableStmt,
        ctx: &mut ExecutionContext<'_>,
    ) -> Result<Option<QueryResult>, DbError> {
        if stmt.name.name.starts_with('#') {
            let logical = stmt.name.name.clone();
            let physical = format!("__temp_{}", logical.trim_start_matches('#'));
            ctx.session.temp_map
                .insert(normalize_identifier(&logical), physical.clone());
            stmt.name.schema = Some("dbo".to_string());
            stmt.name.name = physical;
        }
        self.schema().create_table(stmt)?;
        Ok(None)
    }

    pub(crate) fn execute_drop_table(
        &mut self,
        mut stmt: DropTableStmt,
        ctx: &mut ExecutionContext<'_>,
    ) -> Result<Option<QueryResult>, DbError> {
        if stmt.name.name.starts_with('#') {
            let key = normalize_identifier(&stmt.name.name);
            if let Some(mapped) = ctx.session.temp_map.remove(&key) {
                stmt.name.schema = Some("dbo".to_string());
                stmt.name.name = mapped;
            }
        } else if stmt.name.name.starts_with('@') {
            if let Some(mapped) = ctx.resolve_table_name(&stmt.name.name) {
                stmt.name.schema = Some("dbo".to_string());
                stmt.name.name = mapped;
            }
        }
        self.schema().drop_table(stmt)?;
        Ok(None)
    }

    pub(crate) fn execute_truncate_table(
        &mut self,
        mut stmt: TruncateTableStmt,
        ctx: &mut ExecutionContext<'_>,
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

        self.schema().alter_table(stmt)?;

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

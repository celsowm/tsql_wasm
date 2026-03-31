use super::ScriptExecutor;
use crate::ast::{DeleteStmt, InsertStmt, UpdateStmt};
use crate::catalog::{Catalog, TableDef};
use crate::error::DbError;
use crate::executor::context::ExecutionContext;
use crate::executor::mutation::MutationExecutor;
use crate::executor::query::QueryExecutor;
use crate::executor::result::QueryResult;
use crate::storage::{Storage, StoredRow};

impl<'a> ScriptExecutor<'a> {
    pub(crate) fn execute_insert(
        &mut self,
        stmt: InsertStmt,
        ctx: &mut ExecutionContext,
    ) -> Result<Option<QueryResult>, DbError> {
        if ctx.is_readonly_table_var(&stmt.table.name) {
            return Err(DbError::Execution(format!(
                "table-valued parameter '{}' is READONLY",
                stmt.table.name
            )));
        }
        let mut mut_exec = MutationExecutor {
            catalog: self.catalog,
            storage: self.storage,
            clock: self.clock,
        };
        mut_exec.execute_insert_with_context(stmt, ctx)
    }

    pub(crate) fn execute_select_into(
        &mut self,
        mut stmt: crate::ast::SelectStmt,
        ctx: &mut ExecutionContext,
    ) -> Result<Option<QueryResult>, DbError> {
        let into_table = stmt.into_table.take();
        let result = QueryExecutor {
            catalog: self.catalog as &dyn Catalog,
            storage: self.storage as &dyn Storage,
            clock: self.clock,
        }
        .execute_select(stmt, ctx)?;

        if let Some(target) = into_table {
            let schema_name = target.schema_or_dbo();
            if self.catalog.find_table(schema_name, &target.name).is_some() {
                return Err(DbError::Semantic(format!(
                    "Table '{}.{}' already exists",
                    schema_name, target.name
                )));
            }

            let schema_id = self
                .catalog
                .get_schema_id(schema_name)
                .ok_or_else(|| DbError::Semantic(format!("schema '{}' not found", schema_name)))?;

            let mut columns = Vec::new();
            for (i, name) in result.columns.iter().enumerate() {
                columns.push(crate::catalog::ColumnDef {
                    id: self.catalog.alloc_column_id(),
                    name: name.clone(),
                    data_type: result.column_types[i].clone(),
                    nullable: true,
                    primary_key: false,
                    unique: false,
                    identity: None,
                    default: None,
                    default_constraint_name: None,
                    check: None,
                    check_constraint_name: None,
                    computed_expr: None,
                });
            }

            let table_id = self.catalog.alloc_table_id();
            let table = TableDef {
                id: table_id,
                schema_id,
                name: target.name.clone(),
                columns,
                check_constraints: vec![],
                foreign_keys: vec![],
            };
            self.catalog.get_tables_mut().push(table);
            self.storage.ensure_table(table_id);

            for row_values in &result.rows {
                let row = StoredRow {
                    values: row_values.clone(),
                    deleted: false,
                };
                self.storage.insert_row(table_id, row.clone())?;
                self.push_dirty_insert(ctx, &target.name, &row);
            }
        }

        Ok(Some(result))
    }

    pub(crate) fn execute_update(
        &mut self,
        stmt: UpdateStmt,
        ctx: &mut ExecutionContext,
    ) -> Result<Option<QueryResult>, DbError> {
        if ctx.is_readonly_table_var(&stmt.table.name) {
            return Err(DbError::Execution(format!(
                "table-valued parameter '{}' is READONLY",
                stmt.table.name
            )));
        }
        let mut mut_exec = MutationExecutor {
            catalog: self.catalog,
            storage: self.storage,
            clock: self.clock,
        };
        mut_exec.execute_update_with_context(stmt, ctx)
    }

    pub(crate) fn execute_delete(
        &mut self,
        stmt: DeleteStmt,
        ctx: &mut ExecutionContext,
    ) -> Result<Option<QueryResult>, DbError> {
        if ctx.is_readonly_table_var(&stmt.table.name) {
            return Err(DbError::Execution(format!(
                "table-valued parameter '{}' is READONLY",
                stmt.table.name
            )));
        }
        let mut mut_exec = MutationExecutor {
            catalog: self.catalog,
            storage: self.storage,
            clock: self.clock,
        };
        mut_exec.execute_delete_with_context(stmt, ctx)
    }
}

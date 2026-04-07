pub(crate) mod merge;
pub(crate) mod cte;

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
    pub(crate) fn execute_dml(
        &mut self,
        dml: crate::ast::DmlStatement,
        ctx: &mut ExecutionContext<'_>,
    ) -> crate::error::StmtResult<Option<QueryResult>> {
        use crate::ast::DmlStatement;
        use crate::error::StmtOutcome;
        match dml {
            DmlStatement::Insert(stmt) => self.execute_insert(stmt, ctx).map(StmtOutcome::Ok),
            DmlStatement::Select(stmt) => self.execute_select_into(stmt, ctx).map(StmtOutcome::Ok),
            DmlStatement::Update(stmt) => self.execute_update(stmt, ctx).map(StmtOutcome::Ok),
            DmlStatement::Delete(stmt) => self.execute_delete(stmt, ctx).map(StmtOutcome::Ok),
            DmlStatement::Merge(stmt) => self.execute_merge(stmt, ctx).map(StmtOutcome::Ok),
            DmlStatement::SelectAssign(stmt) => {
                self.execute_select_assign(stmt, ctx).map(StmtOutcome::Ok)
            }
            DmlStatement::SetOp(stmt) => {
                let left_outcome = self.execute(*stmt.left, ctx)?;
                let right_outcome = self.execute(*stmt.right, ctx)?;

                match (left_outcome, right_outcome) {
                    (StmtOutcome::Ok(Some(left)), StmtOutcome::Ok(Some(right))) => {
                        let result = crate::executor::engine::execute_set_op(left, right, stmt.op)?;
                        Ok(StmtOutcome::Ok(Some(result)))
                    }
                    (StmtOutcome::Break, _) | (_, StmtOutcome::Break) => Ok(StmtOutcome::Break),
                    (StmtOutcome::Continue, _) | (_, StmtOutcome::Continue) => {
                        Ok(StmtOutcome::Continue)
                    }
                    (StmtOutcome::Return(v), _) | (_, StmtOutcome::Return(v)) => {
                        Ok(StmtOutcome::Return(v))
                    }
                    _ => Err(DbError::Execution(
                        "set operations require both sides to return results".into(),
                    )),
                }
            }
        }
    }

    pub(crate) fn execute_insert(
        &mut self,
        stmt: InsertStmt,
        ctx: &mut ExecutionContext<'_>,
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
        ctx: &mut ExecutionContext<'_>,
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
                return Err(DbError::duplicate_table(schema_name, &target.name));
            }

            let schema_id = self
                .catalog
                .get_schema_id(schema_name)
                .ok_or_else(|| DbError::schema_not_found(schema_name))?;

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
                    ansi_padding_on: true,
                });
            }

            let table_id = self.catalog.alloc_table_id();
            let table = TableDef {
                id: table_id,
                schema_id,
                schema_name: schema_name.to_string(),
                name: target.name.clone(),
                columns,
                check_constraints: vec![],
                foreign_keys: vec![],
            };
            self.catalog.register_table(table);
            self.storage.ensure_table(table_id);

            for row_values in &result.rows {
                let row = StoredRow {
                    values: row_values.clone(),
                    deleted: false,
                };
                self.storage.insert_row(table_id, row.clone())?;
                if let Some(db) = &ctx.session.dirty_buffer {
                    db.lock().push_op(
                        ctx.session_id(),
                        target.name.to_string(),
                        crate::executor::dirty_buffer::DirtyOp::Insert { row: row.clone() },
                    );
                }
            }
        }

        Ok(Some(result))
    }

    pub(crate) fn execute_update(
        &mut self,
        stmt: UpdateStmt,
        ctx: &mut ExecutionContext<'_>,
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
        ctx: &mut ExecutionContext<'_>,
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

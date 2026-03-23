use crate::ast::{DeleteStmt, FromClause, JoinType};
use crate::catalog::TableDef;
use crate::error::DbError;
use crate::storage::StoredRow;

use super::super::context::ExecutionContext;
use super::super::evaluator::eval_predicate;
use super::super::model::single_row_context;

use super::MutationExecutor;

impl<'a> MutationExecutor<'a> {
    pub(crate) fn execute_delete_with_context(
        &mut self,
        mut stmt: DeleteStmt,
        ctx: &mut ExecutionContext,
    ) -> Result<(), DbError> {
        if let Some(mapped) = ctx.resolve_table_name(&stmt.table.name) {
            stmt.table.name = mapped;
            if stmt.table.schema.is_none() {
                stmt.table.schema = Some("dbo".to_string());
            }
        }
        let schema = stmt.table.schema_or_dbo().to_string();
        let table_name = stmt.table.name.clone();

        let table = self
            .catalog
            .find_table(&schema, &table_name)
            .ok_or_else(|| {
                DbError::Semantic(format!("table '{}.{}' not found", schema, table_name))
            })?
            .clone();

        let table_id = table.id;
        let mut rows = self.storage.get_rows(table_id)?;

        if let Some(from_clause) = &stmt.from {
            return self.execute_delete_with_from(
                &table, table_id, &mut rows, &stmt, from_clause, ctx,
            );
        }

        for row in rows.iter_mut().filter(|r| !r.deleted) {
            let joined = single_row_context(&table, row.clone());
            let matches = if let Some(selection) = &stmt.selection {
                eval_predicate(
                    selection,
                    &joined,
                    ctx,
                    self.catalog,
                    self.storage,
                    self.clock,
                )?
            } else {
                true
            };
            if matches {
                row.deleted = true;
            }
        }

        self.storage.update_rows(table_id, rows)?;
        Ok(())
    }

    fn execute_delete_with_from(
        &mut self,
        table: &TableDef,
        table_id: u32,
        rows: &mut Vec<StoredRow>,
        stmt: &DeleteStmt,
        from_clause: &FromClause,
        ctx: &mut ExecutionContext,
    ) -> Result<(), DbError> {
        let mut delete_indices = Vec::new();

        for i in 0..rows.len() {
            if rows[i].deleted {
                continue;
            }

            let mut combined_ctx: super::super::model::JoinedRow =
                single_row_context(table, rows[i].clone());

            let mut all_match = true;
            for from_table_ref in &from_clause.tables {
                let resolved_name = ctx
                    .resolve_table_name(&from_table_ref.name.name)
                    .unwrap_or_else(|| from_table_ref.name.name.clone());
                let from_schema = from_table_ref.name.schema_or_dbo();
                let from_table = self
                    .catalog
                    .find_table(from_schema, &resolved_name)
                    .ok_or_else(|| {
                        DbError::Semantic(format!(
                            "table '{}.{}' not found",
                            from_schema, resolved_name
                        ))
                    })?
                    .clone();
                let from_rows = self.storage.get_rows(from_table.id)?;
                let non_deleted: Vec<_> = from_rows.iter().filter(|r| !r.deleted).collect();
                if non_deleted.is_empty() {
                    all_match = false;
                    break;
                }
                let alias = from_table_ref
                    .alias
                    .clone()
                    .unwrap_or_else(|| resolved_name.clone());
                combined_ctx.push(super::super::model::ContextTable {
                    table: from_table.clone(),
                    alias: alias.clone(),
                    row: Some(non_deleted[0].clone()),
                });
            }

            if !all_match {
                continue;
            }

            for join_clause in &from_clause.joins {
                let resolved_name = ctx
                    .resolve_table_name(&join_clause.table.name.name)
                    .unwrap_or_else(|| join_clause.table.name.name.clone());
                let join_schema = join_clause.table.name.schema_or_dbo();
                let join_table = self
                    .catalog
                    .find_table(join_schema, &resolved_name)
                    .ok_or_else(|| {
                        DbError::Semantic(format!(
                            "table '{}.{}' not found",
                            join_schema, resolved_name
                        ))
                    })?
                    .clone();
                let join_rows = self.storage.get_rows(join_table.id)?;

                let mut found_match = false;
                let alias = join_clause
                    .table
                    .alias
                    .clone()
                    .unwrap_or_else(|| resolved_name.clone());
                for join_row in join_rows.iter().filter(|r| !r.deleted) {
                    let mut test_ctx = combined_ctx.clone();
                    test_ctx.push(super::super::model::ContextTable {
                        table: join_table.clone(),
                        alias: alias.clone(),
                        row: Some(join_row.clone()),
                    });
                    if let Some(ref on_expr) = join_clause.on {
                        if eval_predicate(
                            on_expr,
                            &test_ctx,
                            ctx,
                            self.catalog,
                            self.storage,
                            self.clock,
                        )? {
                            combined_ctx = test_ctx;
                            found_match = true;
                            break;
                        }
                    }
                }
                match join_clause.join_type {
                    JoinType::Inner => {
                        if !found_match {
                            all_match = false;
                            break;
                        }
                    }
                    JoinType::Left => {}
                    _ => {}
                }
            }

            if !all_match {
                continue;
            }

            let matches = if let Some(selection) = &stmt.selection {
                eval_predicate(
                    selection,
                    &combined_ctx,
                    ctx,
                    self.catalog,
                    self.storage,
                    self.clock,
                )?
            } else {
                true
            };

            if matches {
                delete_indices.push(i);
            }
        }

        for idx in delete_indices {
            rows[idx].deleted = true;
        }

        self.storage.update_rows(table_id, rows.to_vec())?;
        Ok(())
    }
}

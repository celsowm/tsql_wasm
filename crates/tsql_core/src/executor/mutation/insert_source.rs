use crate::ast::InsertSource;
use crate::catalog::TableDef;
use crate::error::{DbError, StmtOutcome};
use crate::executor::query::plan::RelationalQuery;
use crate::executor::result::QueryResult;
use crate::storage::StoredRow;

use super::super::context::ExecutionContext;
use super::super::query::QueryExecutor;
use super::super::script::ScriptExecutor;
use super::MutationExecutor;

impl<'a> MutationExecutor<'a> {
    pub(crate) fn collect_insert_rows(
        &mut self,
        table: &TableDef,
        insert_columns: &[String],
        source: &InsertSource,
        ctx: &mut ExecutionContext<'_>,
        rowcount_limit: Option<usize>,
    ) -> Result<Vec<StoredRow>, DbError> {
        match source {
            InsertSource::DefaultValues => {
                Ok(vec![self.build_insert_row(table, &[], vec![], ctx)?])
            }
            InsertSource::Values(values) => self.collect_insert_rows_from_values(
                table,
                insert_columns,
                values,
                ctx,
                rowcount_limit,
            ),
            InsertSource::Select(select_stmt) => {
                let query_result = QueryExecutor {
                    catalog: self.catalog as &dyn crate::catalog::Catalog,
                    storage: self.storage,
                    clock: self.clock,
                }
                .execute_select(RelationalQuery::from(*select_stmt.clone()), ctx)?;
                self.collect_insert_rows_from_query_result(
                    table,
                    insert_columns,
                    query_result,
                    ctx,
                    rowcount_limit,
                )
            }
            InsertSource::Exec(exec_stmt) => {
                let outcome = ScriptExecutor {
                    catalog: self.catalog,
                    storage: self.storage,
                    clock: self.clock,
                }
                .execute(*exec_stmt.clone(), ctx)?;
                let query_result = match outcome {
                    StmtOutcome::Ok(Some(r)) => r,
                    StmtOutcome::Ok(None) => {
                        return Err(DbError::Execution(
                            "INSERT EXEC source returned no result".into(),
                        ))
                    }
                    other => {
                        other.into_result()?;
                        unreachable!()
                    }
                };
                self.collect_insert_rows_from_query_result(
                    table,
                    insert_columns,
                    query_result,
                    ctx,
                    rowcount_limit,
                )
            }
        }
    }

    pub(crate) fn collect_insert_rows_from_values(
        &mut self,
        table: &TableDef,
        insert_columns: &[String],
        values: &[Vec<crate::ast::Expr>],
        ctx: &mut ExecutionContext<'_>,
        rowcount_limit: Option<usize>,
    ) -> Result<Vec<StoredRow>, DbError> {
        let mut rows = Vec::new();
        for value_row in values.iter().cloned() {
            if let Some(limit) = rowcount_limit {
                if rows.len() >= limit {
                    break;
                }
            }
            rows.push(self.build_insert_row(table, insert_columns, value_row, ctx)?);
        }
        Ok(rows)
    }

    pub(crate) fn collect_insert_rows_from_query_result(
        &mut self,
        table: &TableDef,
        insert_columns: &[String],
        query_result: QueryResult,
        ctx: &mut ExecutionContext<'_>,
        rowcount_limit: Option<usize>,
    ) -> Result<Vec<StoredRow>, DbError> {
        if insert_columns.len() != query_result.columns.len() {
            return Err(DbError::Execution(format!(
                "insert column count ({}) does not match source column count ({})",
                insert_columns.len(),
                query_result.columns.len()
            )));
        }

        let mut rows = Vec::new();
        for row_values in query_result.rows {
            if let Some(limit) = rowcount_limit {
                if rows.len() >= limit {
                    break;
                }
            }
            rows.push(self.build_row_from_values(table, insert_columns, row_values, ctx)?);
        }
        Ok(rows)
    }

    pub(crate) fn commit_insert_rows(
        &mut self,
        table: &TableDef,
        table_id: u32,
        rows: Vec<StoredRow>,
        ctx: &mut ExecutionContext<'_>,
        collect_rows: bool,
    ) -> Result<Vec<StoredRow>, DbError> {
        let mut inserted_rows_for_output = Vec::new();

        for row in rows {
            crate::executor::mutation::validation::enforce_unique_on_insert(
                table,
                self.storage,
                table_id,
                &row,
            )?;
            crate::executor::mutation::validation::enforce_foreign_keys_on_insert(
                table,
                self.catalog,
                self.storage,
                &row,
            )?;
            crate::executor::mutation::validation::enforce_checks_on_row(
                table,
                &row,
                ctx,
                self.catalog,
                self.storage,
                self.clock,
            )?;
            self.storage.insert_row(table_id, row.clone())?;
            self.push_dirty_insert(ctx, &table.name, &row);
            if collect_rows {
                inserted_rows_for_output.push(row);
            }
        }

        Ok(inserted_rows_for_output)
    }
}

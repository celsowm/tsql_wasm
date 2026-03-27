use crate::ast::WithCteStmt;
use crate::catalog::{Catalog, TableDef};
use crate::error::DbError;
use crate::executor::context::ExecutionContext;
use crate::executor::query::QueryExecutor;
use crate::executor::result::QueryResult;
use crate::storage::{Storage, StoredRow};
use super::ScriptExecutor;

impl<'a> ScriptExecutor<'a> {
    pub(crate) fn execute_with_cte(
        &mut self,
        stmt: WithCteStmt,
        ctx: &mut ExecutionContext,
    ) -> Result<Option<QueryResult>, DbError> {
        ctx.ctes = super::super::cte::CteStorage::new();

        for cte_def in &stmt.ctes {
            let result = QueryExecutor {
                catalog: self.catalog as &dyn Catalog,
                storage: self.storage as &dyn Storage,
                clock: self.clock,
            }
            .execute_select(cte_def.query.clone(), ctx)?;

            let table_def = TableDef {
                id: 0,
                schema_id: 1,
                name: cte_def.name.clone(),
                columns: result
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(i, name)| crate::catalog::ColumnDef {
                        id: (i + 1) as u32,
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
                    })
                    .collect(),
                check_constraints: vec![],
                foreign_keys: vec![],
            };

            let rows: Vec<StoredRow> = result
                .rows
                .into_iter()
                .map(|values| StoredRow {
                    values,
                    deleted: false,
                })
                .collect();

            ctx.ctes.insert(&cte_def.name, table_def, rows);
        }

        self.execute_batch(&[(*stmt.body).clone()], ctx)
    }
}

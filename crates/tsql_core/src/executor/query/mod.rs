pub(crate) mod binder;
pub(crate) mod from_tree;
pub(crate) mod plan;
pub(crate) mod pipeline;
pub(crate) mod projection;
pub(crate) mod transformer;
pub(crate) mod scan;

use crate::catalog::Catalog;
use crate::error::DbError;
use crate::storage::Storage;

use super::clock::Clock;
use super::context::ExecutionContext;
use super::model::JoinedRow;
use plan::RelationalQuery;

pub struct QueryExecutor<'a> {
    pub catalog: &'a dyn Catalog,
    pub storage: &'a dyn Storage,
    pub clock: &'a dyn Clock,
}

impl<'a> QueryExecutor<'a> {
    pub fn execute_select(
        &self,
        query: RelationalQuery,
        ctx: &mut ExecutionContext,
    ) -> Result<super::result::QueryResult, DbError> {
        let into_table = query.into_table.clone();
        let result = self.execute_select_internal(&query, ctx)?;
        let mut result = result;

        if ctx.options.rowcount > 0 && result.rows.len() > ctx.options.rowcount as usize {
            result.rows.truncate(ctx.options.rowcount as usize);
        }

        if into_table.is_some() {
            return Err(DbError::Execution("SELECT INTO is handled by ScriptExecutor".into()));
        }

        Ok(result)
    }

    fn execute_select_internal(
        &self,
        query: &RelationalQuery,
        ctx: &mut ExecutionContext,
    ) -> Result<super::result::QueryResult, DbError> {
        from_tree::enforce_query_governor_cost_limit(query, ctx)?;
        let source_eval = self.execute_source(query, ctx)?;
        pipeline::execute_rows_to_result(self, query, source_eval.rows, ctx)
    }

    pub fn execute_to_joined_rows(
        &self,
        query: RelationalQuery,
        ctx: &mut ExecutionContext,
    ) -> Result<Vec<JoinedRow>, DbError> {
        self.execute_source(&query, ctx).map(|eval| eval.rows)
    }

    fn execute_source(
        &self,
        query: &RelationalQuery,
        ctx: &mut ExecutionContext,
    ) -> Result<from_tree::FromEval, DbError> {
        let mut source_eval = if let Some(from_clause) = query.from_clause.clone() {
            from_tree::execute_from_clause(self, from_clause, ctx)?
        } else {
            from_tree::FromEval {
                rows: vec![vec![]],
                shape: vec![],
            }
        };

        for apply_clause in &query.applies {
            source_eval.rows =
                transformer::execute_apply(source_eval.rows, apply_clause, ctx, |s, c| {
                    self.execute_select(s.into(), c)
                })?;
            source_eval.shape = source_eval.rows.first().cloned().unwrap_or_default();
        }

        if let Some(where_clause) = &query.filter.selection {
            let bound_where = super::binder::bind_expr(where_clause, &source_eval.shape, ctx)
                .unwrap_or_else(|_| super::binder::BoundExpr::Dynamic(where_clause.clone()));

            let mut filtered = Vec::new();
            for row in source_eval.rows {
                if super::value_ops::truthy(&super::binder::eval_bound_expr(
                    &bound_where,
                    &row,
                    ctx,
                    self.catalog,
                    self.storage,
                    self.clock,
                )?) {
                    filtered.push(row);
                }
            }
            source_eval.rows = filtered;
        }

        Ok(source_eval)
    }

    fn bind_table(
        &self,
        tref: crate::ast::TableRef,
        catalog: &dyn Catalog,
        ctx: &mut ExecutionContext,
    ) -> Result<super::model::BoundTable, DbError> {
        binder::bind_table(catalog, self.storage, self.clock, tref, ctx, |s, c| {
            self.execute_select(s.into(), c)
        })
    }
}

use crate::ast::SelectAssignStmt;
use crate::error::DbError;
use crate::executor::context::ExecutionContext;
use crate::executor::evaluator::eval_expr;
use crate::executor::query::QueryExecutor;
use crate::executor::result::QueryResult;
use crate::executor::value_ops::coerce_value_to_type_with_dateformat;
use crate::catalog::Catalog;
use crate::storage::Storage;
use crate::ast::SelectStmt;
use super::super::ScriptExecutor;

impl<'a> ScriptExecutor<'a> {
    pub(crate) fn execute_select_assign(
        &mut self,
        stmt: SelectAssignStmt,
        ctx: &mut ExecutionContext<'_>,
    ) -> Result<Option<QueryResult>, DbError> {
        if stmt.targets.is_empty() {
            return Ok(None);
        }
        if stmt.from.is_none() {
            for t in stmt.targets {
                let val = eval_expr(
                    &t.expr,
                    &[],
                    ctx,
                    self.catalog,
                    self.storage,
                    self.clock,
                )?;
                if let Some((ty, var)) = ctx.session.variables.get_mut(&t.variable) {
                    *var = coerce_value_to_type_with_dateformat(val, ty, &ctx.options.dateformat)?;
                } else {
                    return Err(DbError::invalid_identifier(&t.variable));
                }
            }
            return Ok(None);
        }

        let q = SelectStmt {
            from: stmt.from,
            joins: stmt.joins,
            applies: vec![],
            projection: stmt
                .targets
                .iter()
                .map(|t| crate::ast::SelectItem {
                    expr: t.expr.clone(),
                    alias: None,
                })
                .collect(),
            into_table: None,
            distinct: false,
            top: None,
            selection: stmt.selection,
            group_by: vec![],
            having: None,
            order_by: vec![],
            offset: None,
            fetch: None,
        };
        let result = QueryExecutor {
            catalog: self.catalog as &dyn Catalog,
            storage: self.storage as &dyn Storage,
            clock: self.clock,
        }
        .execute_select(q, ctx)?;
        if let Some(last) = result.rows.last() {
            for (idx, t) in stmt.targets.iter().enumerate() {
                if let Some((ty, var)) = ctx.session.variables.get_mut(&t.variable) {
                    *var = coerce_value_to_type_with_dateformat(
                        last[idx].clone(),
                        ty,
                        &ctx.options.dateformat,
                    )?;
                } else {
                    return Err(DbError::invalid_identifier(&t.variable));
                }
            }
        }
        Ok(None)
    }
}

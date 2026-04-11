use super::super::ScriptExecutor;
use crate::ast::{JoinClause, SelectAssignStmt, SelectItem, TableRef};
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::executor::context::ExecutionContext;
use crate::executor::evaluator::eval_expr;
use crate::executor::query::plan::{FilterPlan, PaginationPlan, ProjectionPlan, RelationalQuery, SortPlan};
use crate::executor::query::QueryExecutor;
use crate::executor::result::QueryResult;
use crate::executor::value_ops::coerce_value_to_type_with_dateformat;
use crate::storage::Storage;

fn build_from_clause(from: Option<TableRef>, joins: &[JoinClause]) -> Option<crate::ast::FromNode> {
    let mut node = from.map(crate::ast::FromNode::Table)?;
    for join in joins {
        node = crate::ast::FromNode::Join {
            left: Box::new(node),
            join_type: join.join_type,
            right: Box::new(crate::ast::FromNode::Table(join.table.clone())),
            on: join.on.clone(),
        };
    }
    Some(node)
}

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

        let q = RelationalQuery {
            from_clause: build_from_clause(stmt.from, &stmt.joins),
            applies: vec![],
            projection: ProjectionPlan {
                items: stmt
                    .targets
                    .iter()
                    .map(|t| SelectItem {
                        expr: t.expr.clone(),
                        alias: None,
                    })
                    .collect(),
                distinct: false,
            },
            filter: FilterPlan {
                selection: stmt.selection,
                group_by: vec![],
                having: None,
            },
            sort: SortPlan { order_by: vec![] },
            pagination: PaginationPlan {
                top: None,
                offset: None,
                fetch: None,
            },
            into_table: None,
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

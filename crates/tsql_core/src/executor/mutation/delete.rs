use std::collections::HashSet;

use crate::ast::{DeleteStmt, FromNode, SelectItem, TableFactor};
use crate::error::DbError;

use super::super::context::ExecutionContext;
use super::super::query::QueryExecutor;
use super::super::query::plan::RelationalQuery;
use super::super::result::QueryResult;

use super::MutationExecutor;
use super::output::build_output_result;
use super::validation::enforce_foreign_keys_on_delete;

impl<'a> MutationExecutor<'a> {
    pub(crate) fn execute_delete_with_context(
        &mut self,
        mut stmt: DeleteStmt,
        ctx: &mut ExecutionContext<'_>,
    ) -> Result<Option<QueryResult>, DbError> {
        if let Some(mapped) = ctx.resolve_table_name(&stmt.table.name) {
            stmt.table.name = mapped;
            if stmt.table.schema.is_none() {
                stmt.table.schema = Some("dbo".to_string());
            }
        }
        let table = if let Some(from) = &stmt.from {
            let target_name = &stmt.table.name;
            let mut found = None;
            for tref in &from.tables {
                let tname = tref.factor.as_object_name().map(|o| o.name.as_str()).unwrap_or("");
                let alias = tref.alias.as_ref().map(|s| s.as_str()).unwrap_or(tname);
                if alias.eq_ignore_ascii_case(target_name) || (!tref.factor.is_derived() && tname.eq_ignore_ascii_case(target_name)) {
                    let schema = tref.factor.as_object_name().map(|o| o.schema_or_dbo()).unwrap_or("dbo");
                    let t = match self.catalog.find_table(schema, tname) {
                        Some(t) => t,
                        None => {
                            if let Some(mapped) = ctx.resolve_table_name(tname) {
                                self.catalog.find_table("dbo", &mapped).ok_or_else(|| {
                                    DbError::table_not_found("dbo", &mapped)
                                })?
                            } else {
                                return Err(DbError::table_not_found(schema, tname));
                            }
                        }
                    };
                    found = Some(t.clone());
                    break;
                }
            }
            if found.is_none() {
                for jcl in &from.joins {
                    let tname = jcl.table.factor.as_object_name().map(|o| o.name.as_str()).unwrap_or("");
                    let alias = jcl.table.alias.as_ref().map(|s| s.as_str()).unwrap_or(tname);
                    if alias.eq_ignore_ascii_case(target_name) || (!jcl.table.factor.is_derived() && tname.eq_ignore_ascii_case(target_name)) {
                        let schema = jcl.table.factor.as_object_name().map(|o| o.schema_or_dbo()).unwrap_or("dbo");
                        let t = match self.catalog.find_table(schema, tname) {
                            Some(t) => t,
                            None => {
                                if let Some(mapped) = ctx.resolve_table_name(tname) {
                                    self.catalog.find_table("dbo", &mapped).ok_or_else(|| {
                                        DbError::table_not_found("dbo", &mapped)
                                    })?
                                } else {
                                    return Err(DbError::table_not_found(schema, tname));
                                }
                            }
                        };
                        found = Some(t.clone());
                        break;
                    }
                }
            }

            if let Some(f) = found {
                f
            } else {
                let schema = stmt.table.schema_or_dbo().to_string();
                let table_name = stmt.table.name.clone();
                self.catalog.find_table(&schema, &table_name).ok_or_else(|| {
                    DbError::table_not_found(&schema, &table_name)
                })?.clone()
            }
        } else {
            let schema = stmt.table.schema_or_dbo().to_string();
            let table_name = stmt.table.name.clone();
            self.catalog.find_table(&schema, &table_name).ok_or_else(|| {
                DbError::table_not_found(&schema, &table_name)
            })?.clone()
        };

        let table_id = table.id;
        let target_alias = stmt.table.name.clone();

        // Check for INSTEAD OF DELETE trigger
        let instead_of_triggers = if ctx.frame.skip_instead_of {
            vec![]
        } else {
            self.find_triggers(&table, crate::ast::TriggerEvent::Delete)
                .into_iter()
                .filter(|t| t.is_instead_of)
                .collect::<Vec<_>>()
        };

        let query = RelationalQuery {
            from_clause: build_from_node_for_delete(stmt.from.as_ref(), &stmt.table),
            applies: stmt.from.as_ref().map(|f| f.applies.clone()).unwrap_or_default(),
            projection: super::super::query::plan::ProjectionPlan {
                items: vec![SelectItem {
                    expr: crate::ast::Expr::Wildcard,
                    alias: None,
                }],
                distinct: false,
            },
            into_table: None,
            filter: super::super::query::plan::FilterPlan {
                selection: stmt.selection.clone(),
                group_by: vec![],
                having: None,
            },
            sort: super::super::query::plan::SortPlan { order_by: vec![] },
            pagination: super::super::query::plan::PaginationPlan {
                top: stmt.top.clone(),
                offset: None,
                fetch: None,
            },
        };

        let query_executor = QueryExecutor {
            catalog: self.catalog,
            storage: self.storage,
            clock: self.clock,
        };

        let joined_rows = query_executor.execute_to_joined_rows(query, ctx)?;

        if !instead_of_triggers.is_empty() {
            let mut deleted_rows = Vec::new();
            let mut deleted_indices = HashSet::new();
            let rowcount_limit = if ctx.options.rowcount == 0 {
                None
            } else {
                Some(ctx.options.rowcount as usize)
            };
            let mut deleted_count = 0usize;

            for joined_row in joined_rows {
                if let Some(limit) = rowcount_limit {
                    if deleted_count >= limit {
                        break;
                    }
                }
                let target_ctx = joined_row
                    .iter()
                    .find(|ct| {
                        ct.table.id == table_id && ct.alias.eq_ignore_ascii_case(&target_alias)
                    })
                    .or_else(|| joined_row.iter().find(|ct| ct.table.id == table_id))
                    .ok_or_else(|| {
                        DbError::Execution("target table not found in join context".into())
                    })?;

                if let (Some(stored_row), Some(idx)) = (&target_ctx.row, target_ctx.storage_index) {
                    if !deleted_indices.contains(&idx) {
                        deleted_rows.push(stored_row.clone());
                        deleted_indices.insert(idx);
                        deleted_count += 1;
                    }
                }
            }

            self.execute_triggers(
                &table,
                crate::ast::TriggerEvent::Delete,
                true,
                &[],
                &deleted_rows,
                ctx,
            )?;

            if let Some(output) = stmt.output {
                let output_rows: Vec<&crate::storage::StoredRow> = deleted_rows.iter().collect();
                let result = build_output_result(&output, &table, &[], &output_rows)?;
                if let Some(target) = stmt.output_into {
                    if let Some(result) = result.as_ref() {
                        self.insert_output_into(&target, result, ctx)?;
                    } else {
                        return Err(DbError::Execution("OUTPUT INTO produced no result".into()));
                    }
                    return Ok(None);
                }
                return Ok(result);
            }
            return Ok(None);
        }

        let has_after_triggers = !self.find_triggers(&table, crate::ast::TriggerEvent::Delete)
            .into_iter()
            .filter(|t| !t.is_instead_of)
            .collect::<Vec<_>>()
            .is_empty();

        let collect_rows = stmt.output.is_some() || has_after_triggers;
        let mut deleted_indices = HashSet::new();
        let mut deleted_rows_for_output = Vec::new();
        let rowcount_limit = if ctx.options.rowcount == 0 {
            None
        } else {
            Some(ctx.options.rowcount as usize)
        };
        let mut deleted_count = 0usize;

        for joined_row in joined_rows {
            if let Some(limit) = rowcount_limit {
                if deleted_count >= limit {
                    break;
                }
            }
            let target_ctx = joined_row
                .iter()
                .find(|ct| {
                    ct.table.id == table_id && ct.alias.eq_ignore_ascii_case(&target_alias)
                })
                .or_else(|| {
                    joined_row.iter().find(|ct| ct.table.id == table_id)
                })
                .ok_or_else(|| DbError::Execution("target table not found in join context".into()))?;

            if let (Some(stored_row), Some(idx)) = (&target_ctx.row, target_ctx.storage_index) {
                if !deleted_indices.contains(&idx) {
                    enforce_foreign_keys_on_delete(&table, self.catalog, self.storage, stored_row)?;
                    deleted_indices.insert(idx);
                    deleted_count += 1;
                    if collect_rows {
                        deleted_rows_for_output.push(stored_row.clone());
                    }
                }
            }
        }

        let mut indices_to_delete: Vec<usize> = deleted_indices.into_iter().collect();
        indices_to_delete.sort_unstable_by(|a, b| b.cmp(a));

        for idx in indices_to_delete {
            self.storage.delete_row(table_id, idx)?;
            self.push_dirty_delete(ctx, &table.name, idx);
        }

        self.execute_triggers(&table, crate::ast::TriggerEvent::Delete, false, &[], &deleted_rows_for_output, ctx)?;

        if let Some(output) = stmt.output {
            let output_rows: Vec<&crate::storage::StoredRow> = deleted_rows_for_output.iter().collect();
            let result = build_output_result(&output, &table, &[], &output_rows)?;
            if let Some(target) = stmt.output_into {
                if let Some(result) = result.as_ref() {
                    self.insert_output_into(&target, result, ctx)?;
                } else {
                    return Err(DbError::Execution("OUTPUT INTO produced no result".into()));
                }
                return Ok(None);
            }
            return Ok(result);
        }

        Ok(None)
    }
}

fn build_from_node_for_delete(
    from: Option<&crate::ast::FromClause>,
    target: &crate::ast::ObjectName,
) -> Option<FromNode> {
    let base = from.and_then(|f| f.tables.first().cloned()).or_else(|| {
        Some(crate::ast::TableRef {
            factor: TableFactor::Named(target.clone()),
            alias: None,
            pivot: None,
            unpivot: None,
            hints: Vec::new(),
        })
    })?;

    let mut node = FromNode::Table(base);
    if let Some(from_clause) = from {
        for extra_table in from_clause.tables.iter().skip(1) {
            node = FromNode::Join {
                left: Box::new(node),
                join_type: crate::ast::JoinType::Cross,
                right: Box::new(FromNode::Table(extra_table.clone())),
                on: None,
            };
        }
        for join in &from_clause.joins {
            node = FromNode::Join {
                left: Box::new(node),
                join_type: join.join_type,
                right: Box::new(FromNode::Table(join.table.clone())),
                on: join.on.clone(),
            };
        }
    }
    Some(node)
}

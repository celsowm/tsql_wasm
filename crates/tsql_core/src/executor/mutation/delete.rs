use std::collections::HashSet;

use crate::ast::{DeleteStmt, SelectItem, SelectStmt, TableFactor};
use crate::error::DbError;

use super::super::context::ExecutionContext;
use super::super::query::QueryExecutor;
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
                                    DbError::Semantic(format!("table '{}' not found", tname))
                                })?
                            } else {
                                return Err(DbError::Semantic(format!("table '{}.{}' not found", schema, tname)));
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
                                        DbError::Semantic(format!("table '{}' not found", tname))
                                    })?
                                } else {
                                    return Err(DbError::Semantic(format!("table '{}.{}' not found", schema, tname)));
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
                    DbError::Semantic(format!("table '{}.{}' not found", schema, table_name))
                })?.clone()
            }
        } else {
            let schema = stmt.table.schema_or_dbo().to_string();
            let table_name = stmt.table.name.clone();
            self.catalog.find_table(&schema, &table_name).ok_or_else(|| {
                DbError::Semantic(format!("table '{}.{}' not found", schema, table_name))
            })?.clone()
        };

        let table_id = table.id;
        let target_alias = stmt.table.name.clone();

        // Check for INSTEAD OF DELETE trigger
        let instead_of_triggers = if ctx.skip_instead_of {
            vec![]
        } else {
            self.find_triggers(&table, crate::ast::TriggerEvent::Delete)
                .into_iter()
                .filter(|t| t.is_instead_of)
                .collect::<Vec<_>>()
        };

        let query_stmt = SelectStmt {
            from: stmt.from.as_ref().and_then(|f| f.tables.get(0).cloned()).or_else(|| {
                    Some(crate::ast::TableRef {
                        factor: TableFactor::Named(stmt.table.clone()),
                        alias: None,
                        pivot: None,
                        unpivot: None,
                        hints: Vec::new(),
                    })
            }),
            joins: {
                let mut all_joins = Vec::new();
                if let Some(from) = &stmt.from {
                    for extra_table in from.tables.iter().skip(1) {
                        all_joins.push(crate::ast::JoinClause {
                            join_type: crate::ast::JoinType::Cross,
                            table: extra_table.clone(),
                            on: None,
                        });
                    }
                    all_joins.extend(from.joins.clone());
                }
                all_joins
            },
            applies: stmt.from.as_ref().map(|f| f.applies.clone()).unwrap_or_default(),
            projection: vec![SelectItem {
                expr: crate::ast::Expr::Wildcard,
                alias: None,
            }],
            into_table: None,
            distinct: false,
            top: stmt.top.clone(),
            selection: stmt.selection.clone(),
            group_by: vec![],
            having: None,
            order_by: vec![],
            offset: None,
            fetch: None,
        };

        let query_executor = QueryExecutor {
            catalog: self.catalog,
            storage: self.storage,
            clock: self.clock,
        };

        let joined_rows = query_executor.execute_to_joined_rows(query_stmt, ctx)?;

        if !instead_of_triggers.is_empty() {
            let mut deleted_rows = Vec::new();
            let mut deleted_indices = HashSet::new();

            for joined_row in joined_rows {
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
                    self.insert_output_into(&target, result.as_ref().unwrap(), ctx)?;
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

        for joined_row in joined_rows {
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
                self.insert_output_into(&target, result.as_ref().unwrap(), ctx)?;
                return Ok(None);
            }
            return Ok(result);
        }

        Ok(None)
    }
}

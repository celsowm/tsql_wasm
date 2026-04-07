use std::collections::HashSet;

use crate::ast::{SelectItem, SelectStmt, TableFactor, UpdateStmt};
use crate::error::DbError;

use super::super::context::ExecutionContext;
use super::super::query::QueryExecutor;
use super::super::result::QueryResult;

use super::MutationExecutor;
use super::output::build_output_result;
use super::validation::{
    apply_assignments, enforce_checks_on_row, enforce_foreign_keys_on_delete,
    enforce_foreign_keys_on_insert, enforce_foreign_keys_on_update, enforce_unique_on_update, validate_row_against_table,
};

impl<'a> MutationExecutor<'a> {
    pub(crate) fn execute_update_with_context(
        &mut self,
        mut stmt: UpdateStmt,
        ctx: &mut ExecutionContext<'_>,
    ) -> Result<Option<QueryResult>, DbError> {
        if let Some(mapped) = ctx.resolve_table_name(&stmt.table.name) {
            stmt.table.name = mapped;
            if stmt.table.schema.is_none() {
                stmt.table.schema = Some("dbo".to_string());
            }
        }
        let (table, resolved_name) = if let Some(from_clause) = &stmt.from {
            let target_name = &stmt.table.name;
            let mut found = None;
            for tref in &from_clause.tables {
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
                    found = Some((t.clone(), tname.to_string()));
                    break;
                }
            }
            if found.is_none() {
                for jcl in &from_clause.joins {
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
                        found = Some((t.clone(), tname.to_string()));
                        break;
                    }
                }
            }

            if let Some(f) = found {
                f
            } else {
                let schema = stmt.table.schema_or_dbo().to_string();
                let table_name = stmt.table.name.clone();
                let t = self.catalog.find_table(&schema, &table_name).ok_or_else(|| {
                    DbError::table_not_found(&schema, &table_name)
                })?;
                (t.clone(), table_name)
            }
        } else {
            let schema = stmt.table.schema_or_dbo().to_string();
            let table_name = stmt.table.name.clone();
            let t = self.catalog.find_table(&schema, &table_name).ok_or_else(|| {
                DbError::table_not_found(&schema, &table_name)
            })?;
            (t.clone(), table_name)
        };

        let table_id = table.id;
        let target_alias = stmt.table.name.clone();

        // Check for INSTEAD OF UPDATE trigger
        let instead_of_triggers = if ctx.frame.skip_instead_of {
            vec![]
        } else {
            self.find_triggers(&table, crate::ast::TriggerEvent::Update)
                .into_iter()
                .filter(|t| t.is_instead_of)
                .collect::<Vec<_>>()
        };

        let query_stmt = SelectStmt {
            from: stmt.from.as_ref().and_then(|f| f.tables.get(0).cloned()).or_else(|| {
                let name = if stmt.from.is_some() {
                    if stmt.from.as_ref().map(|f| f.tables.is_empty()).unwrap_or(true) {
                         TableFactor::Named(stmt.table.clone())
                    } else {
                        TableFactor::Named(crate::ast::ObjectName {
                            schema: table.schema_or_dbo().to_string().into(),
                            name: resolved_name,
                        })
                    }
                } else {
                    TableFactor::Named(stmt.table.clone())
                };
                Some(crate::ast::TableRef {
                    factor: name,
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
            let mut inserted_rows = Vec::new();
            let mut deleted_rows = Vec::new();
            let mut updated_indices = HashSet::new();
            let rowcount_limit = if ctx.options.rowcount == 0 {
                None
            } else {
                Some(ctx.options.rowcount as usize)
            };
            let mut updated_count = 0usize;

            for joined_row in joined_rows {
                if let Some(limit) = rowcount_limit {
                    if updated_count >= limit {
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
                    if !updated_indices.contains(&idx) {
                        let mut new_row = stored_row.clone();
                        apply_assignments(
                            &table,
                            &mut new_row,
                            &stmt.assignments,
                            &joined_row,
                            ctx,
                            self.catalog,
                            self.storage,
                            self.clock,
                        )?;
                        inserted_rows.push(new_row);
                        deleted_rows.push(stored_row.clone());
                        updated_indices.insert(idx);
                        updated_count += 1;
                    }
                }
            }

            self.execute_triggers(
                &table,
                crate::ast::TriggerEvent::Update,
                true,
                &inserted_rows,
                &deleted_rows,
                ctx,
            )?;

            if let Some(output) = stmt.output {
                let inserted: Vec<&crate::storage::StoredRow> = inserted_rows.iter().collect();
                let deleted: Vec<&crate::storage::StoredRow> = deleted_rows.iter().collect();
                let result = build_output_result(&output, &table, &inserted, &deleted)?;
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

        let has_after_triggers = !self.find_triggers(&table, crate::ast::TriggerEvent::Update)
            .into_iter()
            .filter(|t| !t.is_instead_of)
            .collect::<Vec<_>>()
            .is_empty();

        let collect_rows = stmt.output.is_some() || has_after_triggers;
        let mut updated_indices = HashSet::new();
        let mut inserted_rows_for_output = Vec::new();
        let mut deleted_rows_for_output = Vec::new();
        let rowcount_limit = if ctx.options.rowcount == 0 {
            None
        } else {
            Some(ctx.options.rowcount as usize)
        };
        let mut updated_count = 0usize;

        for joined_row in joined_rows {
            if let Some(limit) = rowcount_limit {
                if updated_count >= limit {
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
                if !updated_indices.contains(&idx) {
                    let mut new_row = stored_row.clone();
                    enforce_foreign_keys_on_delete(&table, self.catalog, self.storage, stored_row)?;
                    apply_assignments(
                        &table,
                        &mut new_row,
                        &stmt.assignments,
                        &joined_row,
                        ctx,
                        self.catalog,
                        self.storage,
                        self.clock,
                    )?;
                    enforce_foreign_keys_on_update(&table, self.catalog, self.storage, stored_row, &new_row)?;
                    validate_row_against_table(&table, &new_row.values)?;
                    enforce_foreign_keys_on_insert(&table, self.catalog, self.storage, &new_row)?;
                    enforce_checks_on_row(
                        &table,
                        &new_row,
                        ctx,
                        self.catalog,
                        self.storage,
                        self.clock,
                    )?;
                    enforce_unique_on_update(&table, self.storage, table_id, &new_row, idx)?;

                    self.storage.update_row(table_id, idx, new_row.clone())?;
                    self.push_dirty_update(ctx, &table.name, idx, &new_row);
                    updated_indices.insert(idx);
                    updated_count += 1;

                    if collect_rows {
                        inserted_rows_for_output.push(new_row);
                        deleted_rows_for_output.push(stored_row.clone());
                    }
                }
            }
        }

        self.execute_triggers(&table, crate::ast::TriggerEvent::Update, false, &inserted_rows_for_output, &deleted_rows_for_output, ctx)?;

        if let Some(output) = stmt.output {
            let inserted: Vec<&crate::storage::StoredRow> = inserted_rows_for_output.iter().collect();
            let deleted: Vec<&crate::storage::StoredRow> = deleted_rows_for_output.iter().collect();
            let result = build_output_result(&output, &table, &inserted, &deleted)?;
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

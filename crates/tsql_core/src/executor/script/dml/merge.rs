use crate::ast::MergeStmt;
use crate::error::DbError;
use crate::types::Value;

use crate::executor::context::ExecutionContext;
use crate::executor::evaluator::eval_expr;
use crate::executor::query::QueryExecutor;
use crate::executor::result::QueryResult;
use super::super::ScriptExecutor;

impl<'a> ScriptExecutor<'a> {
    pub(crate) fn execute_merge(
        &mut self,
        stmt: MergeStmt,
        ctx: &mut ExecutionContext<'_>,
    ) -> Result<Option<QueryResult>, DbError> {
        let target_object = stmt.target.name_as_object().ok_or_else(|| {
            DbError::Execution("MERGE target must be a named table".into())
        })?;
        if ctx.is_readonly_table_var(target_object.name.as_str()) {
            return Err(DbError::Execution(format!(
                "table-valued parameter '{}' is READONLY",
                target_object.name
            )));
        }
        let target_name = ctx
            .resolve_table_name(target_object.name.as_str())
            .unwrap_or_else(|| target_object.name.clone());
        let target_schema = target_object.schema_or_dbo();
        let target_table = self
            .catalog
            .find_table(target_schema, &target_name)
            .ok_or_else(|| {
                DbError::Semantic(format!(
                    "table '{}.{}' not found",
                    target_schema, target_name
                ))
            })?
            .clone();

        // Execute source query
        let source_rows = match &stmt.source {
            crate::ast::MergeSource::Table(source_ref) => {
                let resolved = ctx
                    .resolve_table_name(
                        source_ref
                            .name_as_object()
                            .ok_or_else(|| DbError::Execution("MERGE source must be a named table".into()))?
                            .name
                            .as_str(),
                    )
                    .unwrap_or_else(|| {
                        source_ref
                            .name_as_object()
                            .map(|o| o.name.clone())
                            .unwrap_or_else(|| "source".to_string())
                    });
                let source_name = source_ref.name_as_object().ok_or_else(|| {
                    DbError::Execution("MERGE source must be a named table".into())
                })?;
                let source_schema = source_name.schema_or_dbo();
                let source_table = self
                    .catalog
                    .find_table(source_schema, &resolved)
                    .ok_or_else(|| {
                        DbError::Semantic(format!(
                            "table '{}.{}' not found",
                            source_schema, resolved
                        ))
                    })?
                    .clone();
                let mut rows = Vec::new();
                for row in self.storage.scan_rows(source_table.id)? {
                    let row = row?;
                    if row.deleted {
                        continue;
                    }
                    rows.push(row.values.clone());
                }
                rows
            }
            crate::ast::MergeSource::Subquery(select_stmt, _alias) => {
                let qe = QueryExecutor {
                    catalog: self.catalog,
                    storage: self.storage,
                    clock: self.clock,
                };
                let result = qe.execute_select(select_stmt.clone(), ctx)?;
                result.rows
            }
        };

        let target_alias = stmt
            .target
            .alias
            .clone()
            .unwrap_or_else(|| target_name.clone());

        let target_rows = self
            .storage
            .scan_rows(target_table.id)?
            .collect::<Result<Vec<_>, DbError>>()?;
        let mut source_matched_to_target = vec![false; source_rows.len()];
        let mut target_row_matched = vec![false; target_rows.len()];
        let mut updated_target_rows = target_rows.clone();
        let mut merge_output_rows: Vec<crate::executor::mutation::MergeOutputRow> = Vec::new();
        let mut inserted_rows_for_trigger = Vec::new();
        let mut deleted_rows_for_trigger = Vec::new();

        // Process target rows against source
        for i in 0..target_rows.len() {
            if target_rows[i].deleted {
                continue;
            }

            for (s_idx, source_row) in source_rows.iter().enumerate() {
                // Build combined context for ON condition evaluation
                let mut combined_ctx: crate::executor::model::JoinedRow =
                    vec![crate::executor::model::ContextTable {
                        table: target_table.clone(),
                        alias: target_alias.clone(),
                        row: Some(target_rows[i].clone()),
                        storage_index: Some(i),
                    }];

                // Add source row context
                let source_alias = match &stmt.source {
                    crate::ast::MergeSource::Table(ref t) => {
                        let name = t.name_as_object().ok_or_else(|| {
                            DbError::Execution("MERGE source must be a named table".into())
                        })?;
                        t.alias.clone().unwrap_or_else(|| name.name.clone())
                    }
                    crate::ast::MergeSource::Subquery(_, ref alias) => {
                        alias.clone().unwrap_or_else(|| "source".to_string())
                    }
                };

                // Create a temporary table def for source row
                let source_table_def = crate::catalog::TableDef {
                    id: 0,
                    schema_id: 0,
                    schema_name: "sys".to_string(), // Placeholder for synthetic table
                    name: source_alias.clone(),
                    columns: target_table
                        .columns
                        .iter()
                        .enumerate()
                        .map(|(idx, col)| crate::catalog::ColumnDef {
                            id: (idx + 1) as u32,
                            name: col.name.clone(),
                            data_type: col.data_type.clone(),
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

                combined_ctx.push(crate::executor::model::ContextTable {
                    table: source_table_def,
                    alias: source_alias,
                    row: Some(crate::storage::StoredRow {
                        values: source_row.clone(),
                        deleted: false,
                    }),
                    storage_index: Some(s_idx),
                });

                let on_matches_val = crate::executor::evaluator::eval_expr(
                    &stmt.on_condition,
                    &combined_ctx,
                    ctx,
                    self.catalog,
                    self.storage,
                    self.clock,
                )?;

                let on_matches = match on_matches_val {
                    Value::Bit(b) => b,
                    Value::Null => false,
                    _ => crate::executor::value_ops::truthy(&on_matches_val),
                };

                if !on_matches {
                    continue;
                }

                source_matched_to_target[s_idx] = true;
                target_row_matched[i] = true;

                // Apply WHEN MATCHED clauses
                let mut matched_action_taken = false;
                for when_clause in &stmt.when_clauses {
                    match when_clause.when {
                        crate::ast::MergeWhen::Matched => {
                            if let Some(cond) = &when_clause.condition {
                                let cond_val = crate::executor::evaluator::eval_predicate(
                                    cond,
                                    &combined_ctx,
                                    ctx,
                                    self.catalog,
                                    self.storage,
                                    self.clock,
                                )?;
                                if !cond_val {
                                    continue;
                                }
                            }

                            target_row_matched[i] = true;
                            source_matched_to_target[s_idx] = true;
                            matched_action_taken = true;

                            match &when_clause.action {
                                crate::ast::MergeAction::Update { assignments } => {
                                    let old_values = target_rows[i].values.clone();
                                    let mut temp_row = target_rows[i].clone();
                                    for assign in assignments {
                                        let col_idx = target_table
                                            .columns
                                            .iter()
                                            .position(|c| {
                                                c.name.eq_ignore_ascii_case(&assign.column)
                                            })
                                            .ok_or_else(|| {
                                                DbError::Semantic(format!(
                                                    "column '{}' not found",
                                                    assign.column
                                                ))
                                            })?;
                                        let val = eval_expr(
                                            &assign.expr,
                                            &combined_ctx,
                                            ctx,
                                            self.catalog,
                                            self.storage,
                                            self.clock,
                                        )?;
                                        temp_row.values[col_idx] = val;
                                    }
                                    crate::executor::mutation::validation::enforce_foreign_keys_on_delete(&target_table, self.catalog, self.storage, &target_rows[i])?;
                                    crate::executor::mutation::validation::enforce_foreign_keys_on_insert(&target_table, self.catalog, self.storage, &temp_row)?;
                                    updated_target_rows[i] = temp_row.clone();
                                    if stmt.output.is_some() {
                                        merge_output_rows.push(
                                            crate::executor::mutation::MergeOutputRow {
                                                inserted_values: Some(temp_row.values.clone()),
                                                deleted_values: Some(old_values.clone()),
                                            },
                                        );
                                    }
                                    inserted_rows_for_trigger.push(temp_row);
                                    deleted_rows_for_trigger.push(target_rows[i].clone());
                                }
                                crate::ast::MergeAction::Delete => {
                                    crate::executor::mutation::validation::enforce_foreign_keys_on_delete(&target_table, self.catalog, self.storage, &target_rows[i])?;
                                    if stmt.output.is_some() {
                                        merge_output_rows.push(
                                            crate::executor::mutation::MergeOutputRow {
                                                inserted_values: None,
                                                deleted_values: Some(target_rows[i].values.clone()),
                                            },
                                        );
                                    }
                                    deleted_rows_for_trigger.push(target_rows[i].clone());
                                    updated_target_rows[i].deleted = true;
                                }
                                crate::ast::MergeAction::Insert { .. } => {
                                    return Err(DbError::Execution(
                                        "INSERT in WHEN MATCHED is not supported".into(),
                                    ));
                                }
                            }
                            break;
                        }
                        _ => {}
                    }
                }
                if matched_action_taken {
                    break; // Target row matched with source and action taken, don't look for more source matches for this target row
                }
            }

            if !target_row_matched[i] {
                // WHEN NOT MATCHED BY SOURCE
                let combined_ctx: crate::executor::model::JoinedRow =
                    vec![crate::executor::model::ContextTable {
                        table: target_table.clone(),
                        alias: target_alias.clone(),
                        row: Some(target_rows[i].clone()),
                        storage_index: Some(i),
                    }];

                for when_clause in &stmt.when_clauses {
                    if matches!(when_clause.when, crate::ast::MergeWhen::NotMatchedBySource) {
                        if let Some(cond) = &when_clause.condition {
                            let cond_val = crate::executor::evaluator::eval_predicate(
                                cond,
                                &combined_ctx,
                                ctx,
                                self.catalog,
                                self.storage,
                                self.clock,
                            )?;
                            if !cond_val {
                                continue;
                            }
                        }

                        target_row_matched[i] = true; // Mark it as "processed" by a clause

                        match &when_clause.action {
                            crate::ast::MergeAction::Update { assignments } => {
                                let old_values = target_rows[i].values.clone();
                                let mut temp_row = target_rows[i].clone();
                                for assign in assignments {
                                    let col_idx = target_table
                                        .columns
                                        .iter()
                                        .position(|c| c.name.eq_ignore_ascii_case(&assign.column))
                                        .ok_or_else(|| {
                                            DbError::Semantic(format!(
                                                "column '{}' not found",
                                                assign.column
                                            ))
                                        })?;
                                    let val = eval_expr(
                                        &assign.expr,
                                        &combined_ctx,
                                        ctx,
                                        self.catalog,
                                        self.storage,
                                        self.clock,
                                    )?;
                                    temp_row.values[col_idx] = val;
                                }
                                crate::executor::mutation::validation::enforce_foreign_keys_on_delete(
                                    &target_table,
                                    self.catalog,
                                    self.storage,
                                    &target_rows[i],
                                )?;
                                crate::executor::mutation::validation::enforce_foreign_keys_on_insert(
                                    &target_table,
                                    self.catalog,
                                    self.storage,
                                    &temp_row,
                                )?;
                                updated_target_rows[i] = temp_row.clone();
                                if stmt.output.is_some() {
                                    merge_output_rows.push(
                                        crate::executor::mutation::MergeOutputRow {
                                            inserted_values: Some(
                                                updated_target_rows[i].values.clone(),
                                            ),
                                            deleted_values: Some(old_values.clone()),
                                        },
                                    );
                                }
                                inserted_rows_for_trigger.push(temp_row);
                                deleted_rows_for_trigger.push(target_rows[i].clone());
                            }
                            crate::ast::MergeAction::Delete => {
                                crate::executor::mutation::validation::enforce_foreign_keys_on_delete(
                                    &target_table,
                                    self.catalog,
                                    self.storage,
                                    &target_rows[i],
                                )?;
                                if stmt.output.is_some() {
                                    merge_output_rows.push(
                                        crate::executor::mutation::MergeOutputRow {
                                            inserted_values: None,
                                            deleted_values: Some(target_rows[i].values.clone()),
                                        },
                                    );
                                }
                                deleted_rows_for_trigger.push(target_rows[i].clone());
                                updated_target_rows[i].deleted = true;
                            }
                            _ => {
                                return Err(DbError::Execution(
                                    "Invalid action for NOT MATCHED BY SOURCE".into(),
                                ))
                            }
                        }
                        break;
                    }
                }
            }
        }

        // Ensure all matched rows are updated in storage before NOT MATCHED
        self.storage.clear_table(target_table.id)?;
        if let Some(db) = &ctx.session.dirty_buffer {
                db.lock().push_op(
                ctx.session_id(),
                target_table.name.clone(),
                crate::executor::dirty_buffer::DirtyOp::Truncate,
            );
        }
        for row in updated_target_rows {
            if !row.deleted {
                self.storage.insert_row(target_table.id, row.clone())?;
                self.push_dirty_insert(ctx, &target_table.name, &row);
            }
        }

        // Process WHEN NOT MATCHED (source rows not matched to target)
        let source_alias = match &stmt.source {
            crate::ast::MergeSource::Table(ref t) => {
                let name = t.name_as_object().ok_or_else(|| {
                    DbError::Execution("MERGE source must be a named table".into())
                })?;
                t.alias.clone().unwrap_or_else(|| name.name.clone())
            }
            crate::ast::MergeSource::Subquery(_, ref alias) => {
                alias.clone().unwrap_or_else(|| "source".to_string())
            }
        };

        let source_table_def = crate::catalog::TableDef {
            id: 0,
            schema_id: 0,
            schema_name: "sys".to_string(), // Placeholder for synthetic table
            name: source_alias.clone(),
            columns: target_table
                .columns
                .iter()
                .enumerate()
                .map(|(idx, col)| crate::catalog::ColumnDef {
                    id: (idx + 1) as u32,
                    name: col.name.clone(),
                    data_type: col.data_type.clone(),
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

        for (s_idx, source_row) in source_rows.iter().enumerate() {
            if source_matched_to_target[s_idx] {
                continue;
            }

            let source_ctx: crate::executor::model::JoinedRow =
                vec![crate::executor::model::ContextTable {
                    table: source_table_def.clone(),
                    alias: source_alias.clone(),
                    row: Some(crate::storage::StoredRow {
                        values: source_row.clone(),
                        deleted: false,
                    }),
                    storage_index: Some(s_idx),
                }];

            for when_clause in &stmt.when_clauses {
                match when_clause.when {
                    crate::ast::MergeWhen::NotMatched => {
                        if let Some(cond) = &when_clause.condition {
                            let cond_val = crate::executor::evaluator::eval_predicate(
                                cond,
                                &source_ctx,
                                ctx,
                                self.catalog,
                                self.storage,
                                self.clock,
                            )?;
                            if !cond_val {
                                continue;
                            }
                        }
                        source_matched_to_target[s_idx] = true; // Action taken for this source row
                        match &when_clause.action {
                            crate::ast::MergeAction::Insert { columns, values } => {
                                let mut final_values =
                                    vec![Value::Null; target_table.columns.len()];

                                for (col_name, val_expr) in columns.iter().zip(values.iter()) {
                                    let col_idx = target_table
                                        .columns
                                        .iter()
                                        .position(|c| c.name.eq_ignore_ascii_case(col_name))
                                        .ok_or_else(|| {
                                            DbError::Semantic(format!(
                                                "column '{}' not found",
                                                col_name
                                            ))
                                        })?;

                                    let val = eval_expr(
                                        val_expr,
                                        &source_ctx,
                                        ctx,
                                        self.catalog,
                                        self.storage,
                                        self.clock,
                                    )?;
                                    final_values[col_idx] = val;
                                }

                                let mut temp_row = crate::storage::StoredRow {
                                    values: final_values.clone(),
                                    deleted: false,
                                };

                                for (idx, col) in target_table.columns.iter().enumerate() {
                                    if matches!(final_values[idx], Value::Null) {
                                        if col.identity.is_some() {
                                            let next_val = self
                                                .catalog
                                                .next_identity_value(target_table.id, &col.name)?;
                                            ctx.set_last_identity(next_val);
                                            final_values[idx] = match &col.data_type {
                                                crate::types::DataType::Int => {
                                                    Value::Int(next_val as i32)
                                                }
                                                crate::types::DataType::BigInt => {
                                                    Value::BigInt(next_val)
                                                }
                                                _ => Value::Int(next_val as i32),
                                            };
                                        } else if let Some(ref default_expr) = col.default {
                                            final_values[idx] = eval_expr(
                                                default_expr,
                                                &[],
                                                ctx,
                                                self.catalog,
                                                self.storage,
                                                self.clock,
                                            )?;
                                        }
                                    }
                                }
                                temp_row.values = final_values.clone();
                                crate::executor::mutation::validation::enforce_foreign_keys_on_insert(
                                    &target_table,
                                    self.catalog,
                                    self.storage,
                                    &temp_row,
                                )?;
                                if stmt.output.is_some() {
                                    merge_output_rows.push(
                                        crate::executor::mutation::MergeOutputRow {
                                            inserted_values: Some(final_values.clone()),
                                            deleted_values: None,
                                        },
                                    );
                                }
                                inserted_rows_for_trigger.push(temp_row.clone());
                                self.storage.insert_row(target_table.id, temp_row.clone())?;
                                self.push_dirty_insert(ctx, &target_table.name, &temp_row);
                            }
                            _ => {
                                return Err(DbError::Execution(
                                    "only INSERT is allowed in WHEN NOT MATCHED".into(),
                                ));
                            }
                        }
                        break;
                    }
                    _ => {}
                }
            }
        }

        let mut mut_exec = crate::executor::mutation::MutationExecutor {
            catalog: self.catalog,
            storage: self.storage,
            clock: self.clock,
        };

        if !inserted_rows_for_trigger.is_empty() {
            mut_exec.execute_triggers(
                &target_table,
                crate::ast::TriggerEvent::Insert,
                false,
                &inserted_rows_for_trigger,
                &[],
                ctx,
            )?;
        }
        if !deleted_rows_for_trigger.is_empty() {
            mut_exec.execute_triggers(
                &target_table,
                crate::ast::TriggerEvent::Delete,
                false,
                &[],
                &deleted_rows_for_trigger,
                ctx,
            )?;
        }
        // UPDATE trigger is fired if both inserted and deleted rows for triggers are present and match by index?
        // Actually SQL Server fires UPDATE trigger for MERGE when MATCHED ... UPDATE occurs.
        // We can just fire INSERT and DELETE triggers as a simplification if we don't track which rows were updated.
        // But we do track them. Let's fire UPDATE triggers too if appropriate.
        // For now, firing INSERT/DELETE triggers based on what happened is a good start.

        if let Some(ref output) = stmt.output {
            let result = crate::executor::mutation::build_output_result_merge(
                output,
                &target_table,
                &merge_output_rows,
            )?;
            if let Some(ref target) = stmt.output_into {
                if let Some(result) = result.as_ref() {
                    mut_exec.insert_output_into(target, result, ctx)?;
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

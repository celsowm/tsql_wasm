use crate::ast::MergeStmt;
use crate::error::DbError;
use crate::types::Value;

use super::super::context::ExecutionContext;
use super::super::evaluator::eval_expr;
use super::super::query::QueryExecutor;
use super::super::result::QueryResult;
use super::ScriptExecutor;

impl<'a> ScriptExecutor<'a> {
    pub(crate) fn execute_merge(
        &mut self,
        stmt: MergeStmt,
        ctx: &mut ExecutionContext,
    ) -> Result<Option<QueryResult>, DbError> {
        let target_name = ctx
            .resolve_table_name(&stmt.target.name.name)
            .unwrap_or_else(|| stmt.target.name.name.clone());
        let target_schema = stmt.target.name.schema_or_dbo();
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
                    .resolve_table_name(&source_ref.name.name)
                    .unwrap_or_else(|| source_ref.name.name.clone());
                let source_schema = source_ref.name.schema_or_dbo();
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
                let rows = self.storage.get_rows(source_table.id)?;
                rows.into_iter()
                    .filter(|r| !r.deleted)
                    .map(|r| {
                        let mut row = Vec::new();
                        for val in &r.values {
                            row.push(val.clone());
                        }
                        row
                    })
                    .collect::<Vec<Vec<crate::types::Value>>>()
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

        let target_rows = self.storage.get_rows(target_table.id)?;
        let mut source_matched_to_target = vec![false; source_rows.len()];
        let mut target_row_matched = vec![false; target_rows.len()];
        let mut updated_target_rows = target_rows.clone();
        let mut merge_output_rows: Vec<super::super::mutation::MergeOutputRow> = Vec::new();

        // Process target rows against source
        for i in 0..target_rows.len() {
            if target_rows[i].deleted {
                continue;
            }

            for (s_idx, source_row) in source_rows.iter().enumerate() {
                // Build combined context for ON condition evaluation
                let mut combined_ctx: super::super::model::JoinedRow = vec![super::super::model::ContextTable {
                    table: target_table.clone(),
                    alias: target_alias.clone(),
                    row: Some(target_rows[i].clone()),
                    storage_index: Some(i),
                }];

                // Add source row context
                let source_alias = match &stmt.source {
                    crate::ast::MergeSource::Table(ref t) => {
                        t.alias.clone().unwrap_or_else(|| t.name.name.clone())
                    }
                    crate::ast::MergeSource::Subquery(_, ref alias) => {
                        alias.clone().unwrap_or_else(|| "source".to_string())
                    }
                };

                // Create a temporary table def for source row
                let source_table_def = crate::catalog::TableDef {
                    id: 0,
                    schema_id: 0,
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

                combined_ctx.push(super::super::model::ContextTable {
                    table: source_table_def,
                    alias: source_alias,
                    row: Some(crate::storage::StoredRow {
                        values: source_row.clone(),
                        deleted: false,
                    }),
                    storage_index: Some(s_idx),
                });

                let on_matches_val = super::super::evaluator::eval_expr(
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
                    _ => super::super::value_ops::truthy(&on_matches_val),
                };

                if !on_matches {
                    continue;
                }

                target_row_matched[i] = true;
                source_matched_to_target[s_idx] = true;

                // Apply WHEN MATCHED clauses
                for when_clause in &stmt.when_clauses {
                    match when_clause.when {
                        crate::ast::MergeWhen::Matched => {
                            if let Some(cond) = &when_clause.condition {
                                let cond_val = super::super::evaluator::eval_predicate(
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
                                    super::super::mutation::validation::enforce_foreign_keys_on_delete(&target_table, self.catalog, self.storage, &target_rows[i])?;
                                    super::super::mutation::validation::enforce_foreign_keys_on_insert(&target_table, self.catalog, self.storage, &temp_row)?;
                                    updated_target_rows[i] = temp_row;
                                    if stmt.output.is_some() {
                                        merge_output_rows.push(super::super::mutation::MergeOutputRow {
                                            inserted_values: Some(updated_target_rows[i].values.clone()),
                                            deleted_values: Some(old_values),
                                        });
                                    }
                                }
                                crate::ast::MergeAction::Delete => {
                                    super::super::mutation::validation::enforce_foreign_keys_on_delete(&target_table, self.catalog, self.storage, &target_rows[i])?;
                                    if stmt.output.is_some() {
                                        merge_output_rows.push(super::super::mutation::MergeOutputRow {
                                            inserted_values: None,
                                            deleted_values: Some(target_rows[i].values.clone()),
                                        });
                                    }
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
                break; // Target row matched with source, don't look for more source matches for this target row
            }

            if !target_row_matched[i] {
                // WHEN NOT MATCHED BY SOURCE
                let combined_ctx: super::super::model::JoinedRow = vec![super::super::model::ContextTable {
                    table: target_table.clone(),
                    alias: target_alias.clone(),
                    row: Some(target_rows[i].clone()),
                    storage_index: Some(i),
                }];

                for when_clause in &stmt.when_clauses {
                    if matches!(when_clause.when, crate::ast::MergeWhen::NotMatchedBySource) {
                        if let Some(cond) = &when_clause.condition {
                            let cond_val = super::super::evaluator::eval_predicate(
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
                                super::super::mutation::validation::enforce_foreign_keys_on_delete(&target_table, self.catalog, self.storage, &target_rows[i])?;
                                super::super::mutation::validation::enforce_foreign_keys_on_insert(&target_table, self.catalog, self.storage, &temp_row)?;
                                updated_target_rows[i] = temp_row;
                                if stmt.output.is_some() {
                                    merge_output_rows.push(super::super::mutation::MergeOutputRow {
                                        inserted_values: Some(updated_target_rows[i].values.clone()),
                                        deleted_values: Some(old_values),
                                    });
                                }
                            }
                            crate::ast::MergeAction::Delete => {
                                super::super::mutation::validation::enforce_foreign_keys_on_delete(&target_table, self.catalog, self.storage, &target_rows[i])?;
                                if stmt.output.is_some() {
                                    merge_output_rows.push(super::super::mutation::MergeOutputRow {
                                        inserted_values: None,
                                        deleted_values: Some(target_rows[i].values.clone()),
                                    });
                                }
                                updated_target_rows[i].deleted = true;
                            }
                            _ => return Err(DbError::Execution("Invalid action for NOT MATCHED BY SOURCE".into())),
                        }
                        break;
                    }
                }
            }
        }

        // Ensure all matched rows are updated in storage before NOT MATCHED
        let final_matched_rows: Vec<crate::storage::StoredRow> = updated_target_rows
            .into_iter()
            .filter(|row| !row.deleted)
            .collect();
        self.storage.clear_table(target_table.id)?;
        for row in final_matched_rows {
            self.storage.insert_row(target_table.id, row)?;
        }

        // Process WHEN NOT MATCHED (source rows not matched to target)
        let source_alias = match &stmt.source {
            crate::ast::MergeSource::Table(ref t) => {
                t.alias.clone().unwrap_or_else(|| t.name.name.clone())
            }
            crate::ast::MergeSource::Subquery(_, ref alias) => {
                alias.clone().unwrap_or_else(|| "source".to_string())
            }
        };

        let source_table_def = crate::catalog::TableDef {
            id: 0,
            schema_id: 0,
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

            let source_ctx: super::super::model::JoinedRow = vec![super::super::model::ContextTable {
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
                                super::super::mutation::validation::enforce_foreign_keys_on_insert(&target_table, self.catalog, self.storage, &temp_row)?;
                                if stmt.output.is_some() {
                                    merge_output_rows.push(super::super::mutation::MergeOutputRow {
                                        inserted_values: Some(final_values.clone()),
                                        deleted_values: None,
                                    });
                                }
                                self.storage.insert_row(target_table.id, temp_row)?;
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


        if let Some(ref output) = stmt.output {
            return super::super::mutation::build_output_result_merge(
                output,
                &target_table,
                &merge_output_rows,
            );
        }

        Ok(None)
    }
}

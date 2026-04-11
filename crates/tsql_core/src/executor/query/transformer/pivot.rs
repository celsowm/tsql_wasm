use crate::ast::BinaryOp;
use crate::catalog::{Catalog, ColumnDef, TableDef};
use crate::error::DbError;
use crate::storage::{Storage, StoredRow};
use crate::types::Value;

use crate::executor::clock::Clock;
use crate::executor::context::ExecutionContext;
use crate::executor::model::{ContextTable, JoinedRow};
use crate::executor::physical::PhysicalPivot;
use crate::executor::string_norm::normalize_identifier;

pub(crate) fn execute_pivot(
    _catalog: &dyn Catalog,
    _storage: &dyn Storage,
    _clock: &dyn Clock,
    rows: Vec<JoinedRow>,
    pivot: &PhysicalPivot,
    ctx: &mut ExecutionContext,
) -> Result<Vec<JoinedRow>, DbError> {
    if rows.is_empty() {
        return Ok(rows);
    }

    let spec = &pivot.spec;

    let mut grouping_cols = Vec::new();
    {
        let first_row = &rows[0];
        for ct in first_row {
            for (col_idx, col) in ct.table.columns.iter().enumerate() {
                let name = &col.name;
                if !name.eq_ignore_ascii_case(&spec.aggregate_col) && !name.eq_ignore_ascii_case(&spec.pivot_col) {
                    grouping_cols.push((ct.alias.clone(), name.clone(), col_idx));
                }
            }
        }
    }

    use std::collections::BTreeMap;
    let mut groups: BTreeMap<Vec<Value>, Vec<JoinedRow>> = BTreeMap::new();

    for row in &rows {
        let mut key = Vec::new();
        for (alias, col_name, _) in &grouping_cols {
            let mut val = Value::Null;
            for ct in row {
                if ct.alias.eq_ignore_ascii_case(alias) {
                    if let Some(pos) = ct.table.columns.iter().position(|c| c.name.eq_ignore_ascii_case(col_name)) {
                        if let Some(r) = &ct.row {
                            val = r.values[pos].clone();
                        }
                        break;
                    }
                }
            }
            key.push(val);
        }
        groups.entry(key).or_default().push(row.clone());
    }

    let mut result_rows = Vec::new();
    let mut pivot_columns = Vec::new();
    {
        let first_row = &rows[0];
        for (alias, col_name, _) in &grouping_cols {
            let original_col = first_row
                .iter()
                .find(|ct| ct.alias == *alias)
                .and_then(|ct| ct.table.columns.iter().find(|c| c.name == *col_name))
                .ok_or_else(|| DbError::Execution(format!(
                    "pivot grouping column '{}' not found",
                    col_name
                )))?;
            pivot_columns.push(original_col.clone());
        }
    }
    for val_str in &spec.pivot_values {
        pivot_columns.push(ColumnDef {
            id: (pivot_columns.len() + 1) as u32,
            name: val_str.clone(),
            data_type: crate::types::DataType::SqlVariant,
            nullable: true,
            primary_key: false,
            unique: false,
            identity: None,
            default: None,
            default_constraint_name: None,
            check: None,
            check_constraint_name: None,
            computed_expr: None,
            ansi_padding_on: true,
        });
    }

    let pivot_table_def = TableDef {
        id: 0,
        schema_id: 1,
        schema_name: "dbo".to_string(),
        name: pivot.alias.clone(),
        columns: pivot_columns,
        check_constraints: vec![],
        foreign_keys: vec![],
    };

    for (key, group_rows) in groups {
        let mut row_values = key;

        for val_str in &spec.pivot_values {
            let matching_rows: Vec<JoinedRow> = group_rows
                .iter()
                .filter(|r| {
                    let mut pv = Value::Null;
                    for ct in r.iter() {
                        if let Some(pos) = ct.table.columns.iter().position(|c| c.name.eq_ignore_ascii_case(&spec.pivot_col)) {
                            if let Some(row) = &ct.row {
                                pv = row.values[pos].clone();
                                break;
                            }
                        }
                    }
                    if pv == Value::Null {
                        return false;
                    }
                    pv.to_string_value().eq_ignore_ascii_case(val_str)
                })
                .collect::<Vec<_>>()
                .iter()
                .cloned()
                .cloned()
                .collect();

            if matching_rows.is_empty() {
                row_values.push(Value::Null);
                continue;
            }

            let agg_values: Vec<Value> = matching_rows
                .iter()
                .map(|r| {
                    let mut av = Value::Null;
                    for ct in r.iter() {
                        if let Some(pos) = ct.table.columns.iter().position(|c| c.name.eq_ignore_ascii_case(&spec.aggregate_col)) {
                            if let Some(row) = &ct.row {
                                av = row.values[pos].clone();
                                break;
                            }
                        }
                    }
                    av
                })
                .collect();

            let agg_result = apply_aggregate_to_values(&spec.aggregate_func, agg_values, ctx)?;
            row_values.push(agg_result);
        }

        result_rows.push(vec![ContextTable {
            table: pivot_table_def.clone(),
            alias: pivot.alias.clone(),
            row: Some(StoredRow {
                values: row_values,
                deleted: false,
            }),
            storage_index: Some(0),
        }]);
    }

    Ok(result_rows)
}

fn apply_aggregate_to_values(
    func: &str,
    values: Vec<Value>,
    ctx: &mut ExecutionContext,
) -> Result<Value, DbError> {
    let upper = normalize_identifier(func);
    match upper.as_str() {
        "SUM" => {
            let mut sum: Option<Value> = None;
            for v in values {
                if v.is_null() {
                    continue;
                }
                match sum {
                    None => sum = Some(v),
                    Some(s) => sum = Some(super::super::super::operators::eval_binary(
                        &BinaryOp::Add,
                        s,
                        v,
                        ctx.metadata.ansi_nulls,
                        ctx.options.concat_null_yields_null,
                        ctx.options.arithabort,
                        ctx.options.ansi_warnings,
                    )?),
                }
            }
            Ok(sum.unwrap_or(Value::Null))
        }
        "AVG" => {
            let mut sum: Option<Value> = None;
            let mut count = 0i64;
            for v in values {
                if v.is_null() {
                    continue;
                }
                count += 1;
                match sum {
                    None => sum = Some(v),
                    Some(s) => sum = Some(super::super::super::operators::eval_binary(
                        &BinaryOp::Add,
                        s,
                        v,
                        ctx.metadata.ansi_nulls,
                        ctx.options.concat_null_yields_null,
                        ctx.options.arithabort,
                        ctx.options.ansi_warnings,
                    )?),
                }
            }
            if count == 0 {
                return Ok(Value::Null);
            }
            let sum = match sum {
                Some(v) => v,
                None => return Ok(Value::Null),
            };
            super::super::super::operators::eval_binary(
                &BinaryOp::Divide,
                sum,
                Value::BigInt(count),
                ctx.metadata.ansi_nulls,
                ctx.options.concat_null_yields_null,
                ctx.options.arithabort,
                ctx.options.ansi_warnings,
            )
        }
        "COUNT" => Ok(Value::Int(values.iter().filter(|v| !v.is_null()).count() as i32)),
        "MIN" => {
            let mut min = None;
            for v in values {
                if v.is_null() {
                    continue;
                }
                match min {
                    None => min = Some(v),
                    Some(ref m) if crate::executor::value_ops::compare_values(&v, m) == std::cmp::Ordering::Less => {
                        min = Some(v)
                    }
                    _ => {}
                }
            }
            Ok(min.unwrap_or(Value::Null))
        }
        "MAX" => {
            let mut max = None;
            for v in values {
                if v.is_null() {
                    continue;
                }
                match max {
                    None => max = Some(v),
                    Some(ref m) if crate::executor::value_ops::compare_values(&v, m) == std::cmp::Ordering::Greater => {
                        max = Some(v)
                    }
                    _ => {}
                }
            }
            Ok(max.unwrap_or(Value::Null))
        }
        "COUNT_BIG" => Ok(Value::BigInt(values.iter().filter(|v| !v.is_null()).count() as i64)),
        "STRING_AGG" => {
            let mut result = String::new();
            let mut first = true;
            for v in values {
                if v.is_null() {
                    continue;
                }
                if !first {
                    result.push(',');
                }
                result.push_str(&v.to_string_value());
                first = false;
            }
            Ok(Value::NVarChar(result))
        }
        _ => Err(DbError::Execution(format!(
            "Aggregate function {} not supported in PIVOT yet",
            func
        ))),
    }
}

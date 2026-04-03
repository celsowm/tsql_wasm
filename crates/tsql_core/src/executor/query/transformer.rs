use crate::ast::{ApplyType, BinaryOp, SelectStmt};
use crate::catalog::{Catalog, TableDef, ColumnDef};
use crate::error::DbError;
use crate::storage::{Storage, StoredRow};
use crate::types::Value;

use super::super::clock::Clock;
use super::super::context::ExecutionContext;
use super::super::model::{JoinedRow, ContextTable};
use super::super::planner::{PhysicalPivot, PhysicalUnpivot};
use super::super::result::QueryResult;

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
    
    // 1. Identify grouping columns
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

    // 2. Group rows using Value keys
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

    // 3. Define output table schema
    let mut result_rows = Vec::new();
    let mut pivot_columns = Vec::new();
    {
        let first_row = &rows[0];
        for (alias, col_name, _) in &grouping_cols {
            let original_col = first_row.iter().find(|ct| ct.alias == *alias)
                .and_then(|ct| ct.table.columns.iter().find(|c| c.name == *col_name))
                .unwrap();
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
        });
    }

    let pivot_table_def = TableDef {
        id: 0,
        schema_id: 1,
        name: pivot.alias.clone(),
        columns: pivot_columns,
        check_constraints: vec![],
        foreign_keys: vec![],
    };

    for (key, group_rows) in groups {
        let mut row_values = key;
        
        for val_str in &spec.pivot_values {
            let matching_rows: Vec<JoinedRow> = group_rows.iter()
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
                    if pv == Value::Null { return false; }
                    pv.to_string_value().eq_ignore_ascii_case(val_str)
                })
                .collect::<Vec<_>>().iter().cloned().cloned().collect();
            
            if matching_rows.is_empty() {
                row_values.push(Value::Null);
                continue;
            }

            let agg_values: Vec<Value> = matching_rows.iter()
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
            
            let agg_result = apply_aggregate_to_values(&spec.aggregate_func, agg_values, ctx.ansi_nulls)?;
            row_values.push(agg_result);
        }

        result_rows.push(vec![ContextTable {
            table: pivot_table_def.clone(),
            alias: pivot.alias.clone(),
            row: Some(StoredRow { values: row_values, deleted: false }),
            storage_index: Some(0),
        }]);
    }

    Ok(result_rows)
}

fn apply_aggregate_to_values(func: &str, values: Vec<Value>, ansi_nulls: bool) -> Result<Value, DbError> {
    let upper = func.to_uppercase();
    match upper.as_str() {
        "SUM" => {
            let mut sum: Option<Value> = None;
            for v in values {
                if v.is_null() { continue; }
                match sum {
                    None => sum = Some(v),
                    Some(s) => sum = Some(super::super::operators::eval_binary(&BinaryOp::Add, s, v, ansi_nulls)?),
                }
            }
            Ok(sum.unwrap_or(Value::Null))
        }
        "AVG" => {
            let mut sum: Option<Value> = None;
            let mut count = 0i64;
            for v in values {
                if v.is_null() { continue; }
                count += 1;
                match sum {
                    None => sum = Some(v),
                    Some(s) => sum = Some(super::super::operators::eval_binary(&BinaryOp::Add, s, v, ansi_nulls)?),
                }
            }
            if count == 0 { return Ok(Value::Null); }
            super::super::operators::eval_binary(&BinaryOp::Divide, sum.unwrap(), Value::BigInt(count), ansi_nulls)
        }
        "COUNT" => Ok(Value::Int(values.iter().filter(|v| !v.is_null()).count() as i32)),
        "MIN" => {
            let mut min = None;
            for v in values {
                if v.is_null() { continue; }
                match min {
                    None => min = Some(v),
                    Some(ref m) if crate::executor::value_ops::compare_values(&v, m) == std::cmp::Ordering::Less => min = Some(v),
                    _ => {}
                }
            }
            Ok(min.unwrap_or(Value::Null))
        }
        "MAX" => {
            let mut max = None;
            for v in values {
                if v.is_null() { continue; }
                match max {
                    None => max = Some(v),
                    Some(ref m) if crate::executor::value_ops::compare_values(&v, m) == std::cmp::Ordering::Greater => max = Some(v),
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
                if v.is_null() { continue; }
                if !first {
                    result.push(',');
                }
                result.push_str(&v.to_string_value());
                first = false;
            }
            Ok(Value::NVarChar(result))
        }
        _ => Err(DbError::Execution(format!("Aggregate function {} not supported in PIVOT yet", func))),
    }
}

pub(crate) fn execute_unpivot(
    rows: Vec<JoinedRow>,
    unpivot: &PhysicalUnpivot,
    _ctx: &mut ExecutionContext,
) -> Result<Vec<JoinedRow>, DbError> {
    if rows.is_empty() {
        return Ok(rows);
    }

    let spec = &unpivot.spec;
    let mut result_rows = Vec::new();
    let first_row = &rows[0];

    let mut fixed_cols = Vec::new();
    for ct in first_row {
        for (col_idx, col) in ct.table.columns.iter().enumerate() {
            if !spec.column_list.iter().any(|c| c.eq_ignore_ascii_case(&col.name)) {
                fixed_cols.push((ct.alias.clone(), col.clone(), col_idx));
            }
        }
    }

    let mut output_columns = Vec::new();
    for (_, col, _) in &fixed_cols {
        output_columns.push(col.clone());
    }
    output_columns.push(ColumnDef {
        id: (output_columns.len() + 1) as u32,
        name: spec.pivot_col.clone(),
        data_type: crate::types::DataType::VarChar { max_len: 128 },
        nullable: false,
        primary_key: false,
        unique: false,
        identity: None,
        default: None,
        default_constraint_name: None,
        check: None,
        check_constraint_name: None,
        computed_expr: None,
    });
    output_columns.push(ColumnDef {
        id: (output_columns.len() + 1) as u32,
        name: spec.value_col.clone(),
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
    });

    let unpivot_table_def = TableDef {
        id: 0,
        schema_id: 1,
        name: unpivot.alias.clone(),
        columns: output_columns,
        check_constraints: vec![],
        foreign_keys: vec![],
    };

    for row in rows {
        for col_to_unpivot in &spec.column_list {
            let mut val = Value::Null;
            for ct in &row {
                if let Some(pos) = ct.table.columns.iter().position(|c| c.name.eq_ignore_ascii_case(col_to_unpivot)) {
                    if let Some(r) = &ct.row {
                        val = r.values[pos].clone();
                        break;
                    }
                }
            }

            if matches!(val, Value::Null) {
                continue;
            }

            let mut new_values = Vec::new();
            for (alias, _col, idx) in &fixed_cols {
                // Find correct table by alias
                let mut found_val = Value::Null;
                for ct in &row {
                    if ct.alias.eq_ignore_ascii_case(alias) {
                        if let Some(r) = &ct.row {
                            found_val = r.values[*idx].clone();
                        }
                        break;
                    }
                }
                new_values.push(found_val);
            }
            new_values.push(Value::VarChar(col_to_unpivot.clone()));
            new_values.push(val);

            result_rows.push(vec![ContextTable {
                table: unpivot_table_def.clone(),
                alias: unpivot.alias.clone(),
                row: Some(StoredRow { values: new_values, deleted: false }),
                storage_index: Some(0),
            }]);
        }
    }

    Ok(result_rows)
}

pub(crate) fn execute_apply(
    rows: Vec<JoinedRow>,
    apply: &crate::ast::ApplyClause,
    ctx: &mut ExecutionContext,
    query_executor_proxy: impl Fn(SelectStmt, &mut ExecutionContext) -> Result<QueryResult, DbError>,
) -> Result<Vec<JoinedRow>, DbError> {
    let mut result_rows = Vec::new();

    for left_row in &rows {
        // Push left row context so the subquery can reference outer columns
        ctx.push_apply_row(left_row.clone());
        let sub_result = query_executor_proxy(apply.subquery.clone(), ctx)?;
        ctx.pop_apply_row();

        if sub_result.rows.is_empty() {
            if apply.apply_type == ApplyType::Outer {
                // OUTER APPLY: emit left row with NULLs for the apply columns
                let mut combined = left_row.clone();
                let null_table = TableDef {
                    id: 0,
                    schema_id: 1,
                    name: apply.alias.clone(),
                    columns: sub_result
                        .columns
                        .iter()
                        .enumerate()
                        .map(|(i, cname)| ColumnDef {
                            id: (i + 1) as u32,
                            name: cname.clone(),
                            data_type: sub_result.column_types[i].clone(),
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
                    check_constraints: vec![], foreign_keys: vec![],

                };
                combined.push(ContextTable {
                    table: null_table,
                    alias: apply.alias.clone(),
                    row: None,
                    storage_index: None,
                });
                result_rows.push(combined);
            }
            // CROSS APPLY: skip (no rows emitted)
        } else {
            let apply_table = TableDef {
                id: 0,
                schema_id: 1,
                name: apply.alias.clone(),
                columns: sub_result
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(i, cname)| ColumnDef {
                        id: (i + 1) as u32,
                        name: cname.clone(),
                        data_type: sub_result.column_types[i].clone(),
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
                check_constraints: vec![], foreign_keys: vec![],

            };
            for (idx, sub_row_values) in sub_result.rows.iter().enumerate() {
                let mut combined = left_row.clone();
                combined.push(ContextTable {
                    table: apply_table.clone(),
                    alias: apply.alias.clone(),
                    row: Some(StoredRow {
                        values: sub_row_values.clone(),
                        deleted: false,
                    }),
                    storage_index: Some(idx),
                });
                result_rows.push(combined);
            }
        }
    }

    Ok(result_rows)
}

use crate::ast::{OutputColumn, OutputSource};
use crate::catalog::TableDef;
use crate::error::DbError;
use crate::storage::StoredRow;
use crate::types::Value;

use super::super::result::QueryResult;

fn build_output_columns(output: &[OutputColumn]) -> Vec<String> {
    output
        .iter()
        .map(|col| {
            col.alias.clone().unwrap_or_else(|| {
                let source_prefix = match col.source {
                    OutputSource::Inserted => "INSERTED.",
                    OutputSource::Deleted => "DELETED.",
                };
                format!("{}{}", source_prefix, col.column)
            })
        })
        .collect()
}

fn expand_wildcards(output: &[OutputColumn], table: &TableDef) -> Vec<OutputColumn> {
    let mut expanded = Vec::new();
    for col in output {
        if col.is_wildcard {
            for tcol in &table.columns {
                if tcol.computed_expr.is_some() {
                    continue;
                }
                expanded.push(OutputColumn {
                    source: col.source,
                    column: tcol.name.clone(),
                    alias: None,
                    is_wildcard: false,
                });
            }
        } else {
            expanded.push(col.clone());
        }
    }
    expanded
}

fn extract_output_value(
    output_col: &OutputColumn,
    table: &TableDef,
    row: &StoredRow,
) -> Result<Value, DbError> {
    let col_idx = table
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case(&output_col.column))
        .ok_or_else(|| DbError::Semantic(format!("column '{}' not found", output_col.column)))?;
    Ok(row.values[col_idx].clone())
}

fn resolve_col_idx(col_name: &str, table: &TableDef) -> Result<usize, DbError> {
    table
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case(col_name))
        .ok_or_else(|| DbError::Semantic(format!("column '{}' not found", col_name)))
}

pub fn build_output_result(
    output: &[OutputColumn],
    table: &TableDef,
    inserted_rows: &[&StoredRow],
    deleted_rows: &[&StoredRow],
) -> Result<Option<QueryResult>, DbError> {
    if output.is_empty() {
        return Ok(None);
    }

    let expanded = expand_wildcards(output, table);
    let columns = build_output_columns(&expanded);
    let mut rows = Vec::new();

    if inserted_rows.is_empty() && deleted_rows.is_empty() {
        let n = columns.len();
        return Ok(Some(QueryResult {
            columns,
            column_types: vec![crate::types::DataType::VarChar { max_len: 4000 }; n],
            rows,
        }));
    }

    if !inserted_rows.is_empty() && !deleted_rows.is_empty() {
        for (inserted, deleted) in inserted_rows.iter().zip(deleted_rows.iter()) {
            let mut row = Vec::new();
            for col in &expanded {
                let val = match col.source {
                    OutputSource::Inserted => extract_output_value(col, table, inserted)?,
                    OutputSource::Deleted => extract_output_value(col, table, deleted)?,
                };
                row.push(val);
            }
            rows.push(row);
        }
        let column_types = derive_column_types(&rows, columns.len());
        return Ok(Some(QueryResult {
            columns,
            column_types,
            rows,
        }));
    }

    if !inserted_rows.is_empty() {
        for inserted in inserted_rows {
            let mut row = Vec::new();
            for col in &expanded {
                let val = match col.source {
                    OutputSource::Inserted => extract_output_value(col, table, inserted)?,
                    OutputSource::Deleted => Value::Null,
                };
                row.push(val);
            }
            rows.push(row);
        }
        let column_types = derive_column_types(&rows, columns.len());
        return Ok(Some(QueryResult {
            columns,
            column_types,
            rows,
        }));
    }

    for deleted in deleted_rows {
        let mut row = Vec::new();
        for col in &expanded {
            let val = match col.source {
                OutputSource::Inserted => Value::Null,
                OutputSource::Deleted => extract_output_value(col, table, deleted)?,
            };
            row.push(val);
        }
        rows.push(row);
    }

    let column_types = derive_column_types(&rows, columns.len());
    Ok(Some(QueryResult {
        columns,
        column_types,
        rows,
    }))
}

pub struct MergeOutputRow {
    pub inserted_values: Option<Vec<Value>>,
    pub deleted_values: Option<Vec<Value>>,
}

pub fn build_output_result_merge(
    output: &[OutputColumn],
    table: &TableDef,
    merge_rows: &[MergeOutputRow],
) -> Result<Option<QueryResult>, DbError> {
    if output.is_empty() {
        return Ok(None);
    }

    let expanded = expand_wildcards(output, table);
    let columns = build_output_columns(&expanded);
    let mut rows = Vec::new();

    for merge_row in merge_rows {
        let mut row = Vec::new();
        for col in &expanded {
            let val = match col.source {
                OutputSource::Inserted => match &merge_row.inserted_values {
                    Some(inserted) => inserted[resolve_col_idx(&col.column, table)?].clone(),
                    None => Value::Null,
                },
                OutputSource::Deleted => match &merge_row.deleted_values {
                    Some(deleted) => deleted[resolve_col_idx(&col.column, table)?].clone(),
                    None => Value::Null,
                },
            };
            row.push(val);
        }
        rows.push(row);
    }

    let column_types = derive_column_types(&rows, columns.len());
    Ok(Some(QueryResult {
        columns,
        column_types,
        rows,
    }))
}

fn derive_column_types(rows: &[Vec<Value>], num_cols: usize) -> Vec<crate::types::DataType> {
    let mut column_types = Vec::with_capacity(num_cols);
    if !rows.is_empty() {
        for val in &rows[0] {
            column_types.push(val.data_type().unwrap_or(crate::types::DataType::VarChar { max_len: 4000 }));
        }
    } else {
        column_types = vec![crate::types::DataType::VarChar { max_len: 4000 }; num_cols];
    }
    column_types
}

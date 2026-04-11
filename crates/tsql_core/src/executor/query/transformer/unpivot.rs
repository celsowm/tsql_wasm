use crate::catalog::{ColumnDef, TableDef};
use crate::error::DbError;
use crate::storage::StoredRow;
use crate::types::Value;

use crate::executor::context::ExecutionContext;
use crate::executor::model::{ContextTable, JoinedRow};
use crate::executor::physical::PhysicalUnpivot;

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
        ansi_padding_on: true,
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
        ansi_padding_on: true,
    });

    let unpivot_table_def = TableDef {
        id: 0,
        schema_id: 1,
        schema_name: "dbo".to_string(),
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
                row: Some(StoredRow {
                    values: new_values,
                    deleted: false,
                }),
                storage_index: Some(0),
            }]);
        }
    }

    Ok(result_rows)
}

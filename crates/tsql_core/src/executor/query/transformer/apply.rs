use crate::catalog::{ColumnDef, TableDef};
use crate::error::DbError;
use crate::storage::StoredRow;

use super::super::QueryExecutor;
use crate::executor::context::ExecutionContext;
use crate::executor::model::{ContextTable, JoinedRow};
use crate::executor::result::QueryResult;

pub(crate) fn execute_apply(
    rows: Vec<JoinedRow>,
    apply: &crate::ast::ApplyClause,
    ctx: &mut ExecutionContext,
    executor: &QueryExecutor<'_>,
) -> Result<Vec<JoinedRow>, DbError> {
    let mut result_rows = Vec::new();

    for left_row in &rows {
        ctx.push_apply_row(left_row.clone());
        let sub_result = executor.execute_select(apply.subquery.clone().into(), ctx)?;
        ctx.pop_apply_row();

        if sub_result.rows.is_empty() {
            if apply.apply_type == crate::ast::ApplyType::Outer {
                let mut combined = left_row.clone();
                combined.push(
                    ContextTable {
                        table: build_virtual_table(&apply.alias, &sub_result),
                        alias: apply.alias.clone(),
                        row: None,
                        storage_index: None,
                    }
                    .null_row(),
                );
                result_rows.push(combined);
            }
        } else {
            let apply_table = build_virtual_table(&apply.alias, &sub_result);
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

fn build_virtual_table(alias: &str, sub_result: &QueryResult) -> TableDef {
    TableDef {
        id: 0,
        schema_id: 1,
        schema_name: "dbo".to_string(),
        name: alias.to_string(),
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
                ansi_padding_on: true,
            })
            .collect(),
        check_constraints: vec![],
        foreign_keys: vec![],
    }
}

mod tvf;
mod values;
mod views;

use crate::ast::{SelectStmt, TableFactor, TableRef};
use crate::catalog::{Catalog, ColumnDef, TableDef};
use crate::error::DbError;
use crate::storage::StoredRow;

use super::super::clock::Clock;
use super::super::context::ExecutionContext;
use super::super::model::BoundTable;
use super::super::query::QueryExecutor;
use super::super::result::QueryResult;

pub(crate) fn bind_table(
    catalog: &dyn Catalog,
    storage: &dyn crate::storage::Storage,
    clock: &dyn Clock,
    tref: TableRef,
    ctx: &mut ExecutionContext,
    executor: &QueryExecutor<'_>,
) -> Result<BoundTable, DbError> {
    if let TableFactor::Derived(ref select) = tref.factor {
        return bind_derived_subquery(tref.alias.clone(), *select.clone(), ctx, executor);
    }

    if let Some(bound_tvf) = tvf::bind_builtin_tvf(catalog, storage, clock, &tref, ctx)? {
        return Ok(bound_tvf);
    }
    if let Some(bound_tvf) = tvf::bind_inline_tvf(catalog, storage, clock, &tref, ctx, executor)? {
        return Ok(bound_tvf);
    }
    if let Some(bound_view) = views::bind_view(catalog, storage, clock, &tref, ctx, executor)? {
        return Ok(bound_view);
    }
    if let Some(obj) = tref.name_as_object() {
        let schema = obj.schema_or_dbo();
        if let Some(synonym) = catalog.find_synonym(schema, &obj.name) {
            if ctx.frame.depth > 16 {
                return Err(DbError::Execution(
                    "Synonym recursion limit exceeded".into(),
                ));
            }
            ctx.frame.depth += 1;
            let mut resolved_tref = tref.clone();
            resolved_tref.factor = TableFactor::Named(synonym.base_object.clone());
            let res = bind_table(catalog, storage, clock, resolved_tref, ctx, executor);
            ctx.frame.depth -= 1;
            return res;
        }
    }
    values::bind_plain_table(tref, catalog, ctx)
}

fn bind_derived_subquery(
    alias: Option<String>,
    select: SelectStmt,
    ctx: &mut ExecutionContext,
    executor: &QueryExecutor<'_>,
) -> Result<BoundTable, DbError> {
    let alias =
        alias.ok_or_else(|| DbError::Semantic("subquery in FROM must have an alias".into()))?;
    let result = executor.execute_select(select.into(), ctx)?;
    Ok(query_result_to_bound_table(alias.clone(), alias, result))
}

pub(super) fn query_result_to_bound_table(
    alias: String,
    table_name: String,
    result: QueryResult,
) -> BoundTable {
    let table_def = TableDef {
        id: 0,
        schema_id: 1,
        schema_name: "dbo".to_string(),
        name: table_name,
        columns: result
            .columns
            .iter()
            .enumerate()
            .map(|(i, cname)| ColumnDef {
                id: (i + 1) as u32,
                name: cname.clone(),
                data_type: result.column_types[i].clone(),
                nullable: true,
                primary_key: false,
                unique: false,
                identity: None,
                default: None,
                default_constraint_name: None,
                check: None,
                check_constraint_name: None,
                computed_expr: None,
                collation: None,
                is_clustered: false,
                ansi_padding_on: true,
            })
            .collect(),
        check_constraints: vec![],
        foreign_keys: vec![],
    };
    let rows = result
        .rows
        .into_iter()
        .map(|values| StoredRow {
            values,
            deleted: false,
        })
        .collect::<Vec<_>>();
    BoundTable {
        alias,
        table: table_def,
        virtual_rows: Some(rows),
    }
}

use crate::ast::{Expr, RoutineParam};
use crate::catalog::{Catalog, TableDef, TableTypeDef};
use crate::error::DbError;

pub(crate) fn find_param_def<'a>(params: &'a [RoutineParam], name: &str) -> Option<&'a RoutineParam> {
    params.iter().find(|p| p.name.eq_ignore_ascii_case(name))
}

pub(crate) fn resolve_table_identifier(expr: &Expr) -> Result<&str, DbError> {
    match expr {
        Expr::Identifier(name) => Ok(name.as_str()),
        _ => Err(DbError::Execution(
            "table-valued parameter arguments must be table variables or temp-table identifiers"
                .into(),
        )),
    }
}

pub(crate) fn validate_table_matches_type(
    _catalog: &dyn Catalog,
    table: &TableDef,
    tdef: &TableTypeDef,
) -> Result<(), DbError> {
    if table.columns.len() != tdef.columns.len() {
        return Err(DbError::Execution(format!(
            "TVP type mismatch for '{}.{}': expected {} columns, got {}",
            tdef.schema,
            tdef.name,
            tdef.columns.len(),
            table.columns.len()
        )));
    }
    for (idx, (actual, expected)) in table.columns.iter().zip(tdef.columns.iter()).enumerate() {
        let expected_ty =
            crate::executor::type_mapping::data_type_spec_to_runtime(&expected.data_type);
        if actual.data_type != expected_ty {
            return Err(DbError::Execution(format!(
                "TVP type mismatch at column {} ('{}'): expected {:?}, got {:?}",
                idx + 1,
                expected.name,
                expected_ty,
                actual.data_type
            )));
        }
    }
    Ok(())
}

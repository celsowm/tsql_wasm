use crate::ast::{ObjectName, SelectStmt, TableFactor, TableRef};

pub(crate) fn normalize_object_name(name: &ObjectName) -> String {
    format!(
        "{}.{}",
        name.schema.as_deref().unwrap_or("dbo").to_uppercase(),
        name.name.to_uppercase()
    )
}

pub(crate) fn normalize_table_ref(table: &TableRef) -> String {
    match &table.factor {
        TableFactor::Named(o) => normalize_object_name(o),
        TableFactor::Derived(_) => "(DERIVED)".to_string(),
    }
}

pub(crate) fn select_from_name(stmt: &SelectStmt) -> String {
    stmt.from
        .as_ref()
        .map(normalize_table_ref)
        .unwrap_or_else(|| "<none>".to_string())
}

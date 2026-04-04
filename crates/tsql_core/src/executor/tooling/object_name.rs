use crate::ast::{ObjectName, SelectStmt, TableFactor, TableRef};

use super::super::string_norm::normalize_identifier;

pub(crate) fn normalize_object_name(name: &ObjectName) -> String {
    format!(
        "{}.{}",
        normalize_identifier(name.schema.as_deref().unwrap_or("dbo")),
        normalize_identifier(&name.name)
    )
}

pub(crate) fn normalize_table_ref(table: &TableRef) -> String {
    match &table.factor {
        TableFactor::Named(o) => normalize_object_name(o),
        TableFactor::Derived(_) => "(DERIVED)".to_string(),
        TableFactor::Values { .. } => "(VALUES)".to_string(),
    }
}

pub(crate) fn select_from_name(stmt: &SelectStmt) -> String {
    stmt.from
        .as_ref()
        .map(normalize_table_ref)
        .unwrap_or_else(|| "<none>".to_string())
}

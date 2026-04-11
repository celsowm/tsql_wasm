use crate::ast::{FromNode, ObjectName, SelectStmt, TableFactor, TableRef};

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
    stmt.from_clause
        .as_ref()
        .map(normalize_from_node)
        .unwrap_or_else(|| "<none>".to_string())
}

pub(crate) fn normalize_from_node(node: &FromNode) -> String {
    match node {
        FromNode::Table(table) => normalize_table_ref(table),
        FromNode::Aliased { source, alias } => {
            format!(
                "({}) AS {}",
                normalize_from_node(source),
                normalize_identifier(alias)
            )
        }
        FromNode::Join {
            left,
            join_type,
            right,
            ..
        } => {
            let jt = match join_type {
                crate::ast::JoinType::Inner => "INNER JOIN",
                crate::ast::JoinType::Left => "LEFT JOIN",
                crate::ast::JoinType::Right => "RIGHT JOIN",
                crate::ast::JoinType::Full => "FULL OUTER JOIN",
                crate::ast::JoinType::Cross => "CROSS JOIN",
            };
            format!(
                "({} {} {})",
                normalize_from_node(left),
                jt,
                normalize_from_node(right)
            )
        }
    }
}

use super::VirtualTable;
use super::virtual_table_def;
use crate::ast::{FromNode, SelectStmt, Statement, TableFactor};
use crate::catalog::Catalog;
use crate::storage::StoredRow;
use crate::types::{DataType, Value};

pub(super) struct ViewTableUsage;
pub(super) struct ViewColumnUsage;

impl VirtualTable for ViewTableUsage {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "VIEW_TABLE_USAGE",
            vec![
                ("VIEW_CATALOG", DataType::VarChar { max_len: 128 }, false),
                ("VIEW_SCHEMA", DataType::VarChar { max_len: 128 }, false),
                ("VIEW_NAME", DataType::VarChar { max_len: 128 }, false),
                ("TABLE_CATALOG", DataType::VarChar { max_len: 128 }, false),
                ("TABLE_SCHEMA", DataType::VarChar { max_len: 128 }, false),
                ("TABLE_NAME", DataType::VarChar { max_len: 128 }, false),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog) -> Vec<StoredRow> {
        let mut rows = Vec::new();
        for view in catalog.get_views() {
            let Some(select) = view_select(&view.query) else {
                continue;
            };
            let mut tables = collect_view_table_names(select);
            tables.sort();
            tables.dedup();
            for (table_schema, table_name) in tables {
                rows.push(StoredRow {
                    values: vec![
                        Value::VarChar(DB_CATALOG.to_string()),
                        Value::VarChar(view.schema.clone()),
                        Value::VarChar(view.name.clone()),
                        Value::VarChar(DB_CATALOG.to_string()),
                        Value::VarChar(table_schema),
                        Value::VarChar(table_name),
                    ],
                    deleted: false,
                });
            }
        }
        rows.sort_by(|a, b| compare_rows(&a.values, &b.values));
        rows
    }
}

impl VirtualTable for ViewColumnUsage {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "VIEW_COLUMN_USAGE",
            vec![
                ("VIEW_CATALOG", DataType::VarChar { max_len: 128 }, false),
                ("VIEW_SCHEMA", DataType::VarChar { max_len: 128 }, false),
                ("VIEW_NAME", DataType::VarChar { max_len: 128 }, false),
                ("TABLE_CATALOG", DataType::VarChar { max_len: 128 }, false),
                ("TABLE_SCHEMA", DataType::VarChar { max_len: 128 }, false),
                ("TABLE_NAME", DataType::VarChar { max_len: 128 }, false),
                ("COLUMN_NAME", DataType::VarChar { max_len: 128 }, false),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog) -> Vec<StoredRow> {
        vec![]
    }
}

fn view_select(statement: &Statement) -> Option<&SelectStmt> {
    match statement {
        Statement::Dml(crate::ast::DmlStatement::Select(select)) => Some(select),
        _ => None,
    }
}

fn collect_view_table_names(select: &SelectStmt) -> Vec<(String, String)> {
    let mut tables = Vec::new();
    collect_from_node(select.from_clause.as_ref(), &mut tables);
    for apply in &select.applies {
        collect_view_table_names_into(&apply.subquery, &mut tables);
    }
    tables
}

fn collect_view_table_names_into(select: &SelectStmt, out: &mut Vec<(String, String)>) {
    collect_from_node(select.from_clause.as_ref(), out);
    for apply in &select.applies {
        collect_view_table_names_into(&apply.subquery, out);
    }
}

fn collect_from_node(node: Option<&FromNode>, out: &mut Vec<(String, String)>) {
    let Some(node) = node else {
        return;
    };
    match node {
        FromNode::Table(table) => {
            if let TableFactor::Named(name) = &table.factor {
                out.push((name.schema_or_dbo().to_string(), name.name.clone()));
            }
        }
        FromNode::Aliased { source, .. } => collect_from_node(Some(source), out),
        FromNode::Join { left, right, .. } => {
            collect_from_node(Some(left), out);
            collect_from_node(Some(right), out);
        }
    }
}

fn compare_rows(left: &[Value], right: &[Value]) -> std::cmp::Ordering {
    format_values(left).cmp(&format_values(right))
}

fn format_values(values: &[Value]) -> String {
    values
        .iter()
        .map(|v| match v {
            Value::Null => "NULL".to_string(),
            Value::VarChar(v) | Value::NVarChar(v) | Value::Char(v) => v.clone(),
            Value::Int(v) => v.to_string(),
            Value::BigInt(v) => v.to_string(),
            Value::SmallInt(v) => v.to_string(),
            Value::TinyInt(v) => v.to_string(),
            Value::Bit(v) => v.to_string(),
            other => format!("{:?}", other),
        })
        .collect::<Vec<_>>()
        .join("\u{1f}")
}

const DB_CATALOG: &str = "tsql_wasm";

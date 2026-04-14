use super::virtual_table_def;
use super::VirtualTable;
use crate::ast::{FromNode, SelectStmt, Statement, TableFactor};
use crate::catalog::Catalog;
use crate::storage::StoredRow;
use crate::types::{DataType, Value};
use std::collections::HashSet;

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

    fn rows(&self, catalog: &dyn Catalog) -> Vec<StoredRow> {
        let mut rows = Vec::new();
        for view in catalog.get_views() {
            let Some(select) = view_select(&view.query) else {
                continue;
            };
            let mut seen = HashSet::new();
            collect_view_column_usage(
                select,
                catalog,
                &view.schema,
                &view.name,
                &[],
                &mut seen,
                &mut rows,
            );
        }

        rows.sort_by(|a, b| compare_rows(&a.values, &b.values));
        rows
    }
}

fn view_select(statement: &Statement) -> Option<&SelectStmt> {
    match statement {
        Statement::Dml(crate::ast::DmlStatement::Select(select)) => Some(select),
        _ => None,
    }
}

#[derive(Clone)]
struct SourceTable {
    schema: String,
    name: String,
    match_names: Vec<String>,
    columns: Vec<(String, DataType, bool)>,
}

impl SourceTable {
    fn matches_name(&self, name: &str) -> bool {
        self.match_names
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(name))
    }

    fn has_column(&self, name: &str) -> bool {
        self.columns
            .iter()
            .any(|(column, _, _)| column.eq_ignore_ascii_case(name))
    }

    fn column_info(&self, name: &str) -> Option<(String, DataType, bool)> {
        self.columns
            .iter()
            .find(|(column, _, _)| column.eq_ignore_ascii_case(name))
            .cloned()
    }
}

fn collect_view_column_usage(
    select: &SelectStmt,
    catalog: &dyn Catalog,
    view_schema: &str,
    view_name: &str,
    outer_scope: &[SourceTable],
    seen: &mut HashSet<(String, String, String)>,
    rows: &mut Vec<StoredRow>,
) {
    let mut local_scope = Vec::new();
    if let Some(from) = &select.from_clause {
        collect_sources_from_node(
            from,
            catalog,
            &mut local_scope,
            seen,
            rows,
            view_schema,
            view_name,
        );
    }

    let mut scope = local_scope.clone();
    scope.extend_from_slice(outer_scope);

    for item in &select.projection {
        collect_expr_view_column_usage(
            &item.expr,
            &scope,
            catalog,
            view_schema,
            view_name,
            seen,
            rows,
        );
    }

    if let Some(selection) = &select.selection {
        collect_expr_view_column_usage(
            selection,
            &scope,
            catalog,
            view_schema,
            view_name,
            seen,
            rows,
        );
    }

    for expr in &select.group_by {
        collect_expr_view_column_usage(expr, &scope, catalog, view_schema, view_name, seen, rows);
    }

    if let Some(having) = &select.having {
        collect_expr_view_column_usage(having, &scope, catalog, view_schema, view_name, seen, rows);
    }

    for order_by in &select.order_by {
        collect_expr_view_column_usage(
            &order_by.expr,
            &scope,
            catalog,
            view_schema,
            view_name,
            seen,
            rows,
        );
    }

    if let Some(offset) = &select.offset {
        collect_expr_view_column_usage(offset, &scope, catalog, view_schema, view_name, seen, rows);
    }

    if let Some(fetch) = &select.fetch {
        collect_expr_view_column_usage(fetch, &scope, catalog, view_schema, view_name, seen, rows);
    }

    if let Some(top) = &select.top {
        collect_expr_view_column_usage(
            &top.value,
            &scope,
            catalog,
            view_schema,
            view_name,
            seen,
            rows,
        );
    }

    if let Some(set_op) = &select.set_op {
        collect_view_column_usage(
            &set_op.right,
            catalog,
            view_schema,
            view_name,
            &[],
            seen,
            rows,
        );
    }

    for apply in &select.applies {
        collect_view_column_usage(
            &apply.subquery,
            catalog,
            view_schema,
            view_name,
            &scope,
            seen,
            rows,
        );
    }
}

fn collect_sources_from_node(
    node: &FromNode,
    catalog: &dyn Catalog,
    out: &mut Vec<SourceTable>,
    seen: &mut HashSet<(String, String, String)>,
    rows: &mut Vec<StoredRow>,
    view_schema: &str,
    view_name: &str,
) {
    match node {
        FromNode::Table(table) => match &table.factor {
            TableFactor::Named(name) => {
                if let Some(def) = catalog.find_table(name.schema_or_dbo(), &name.name) {
                    let source = SourceTable {
                        schema: name.schema_or_dbo().to_string(),
                        name: name.name.clone(),
                        match_names: vec![table.alias.clone().unwrap_or_else(|| name.name.clone())],
                        columns: def
                            .columns
                            .iter()
                            .map(|c| (c.name.clone(), c.data_type.clone(), c.nullable))
                            .collect(),
                    };
                    out.push(source);
                }
            }
            TableFactor::Derived(select) => {
                collect_view_column_usage(select, catalog, view_schema, view_name, &[], seen, rows);
            }
            TableFactor::Values { .. } => {}
        },
        FromNode::Aliased { source, alias } => {
            let mut nested = Vec::new();
            collect_sources_from_node(
                source,
                catalog,
                &mut nested,
                seen,
                rows,
                view_schema,
                view_name,
            );
            if nested.len() == 1 {
                nested[0].match_names = vec![alias.clone()];
            }
            out.extend(nested);
        }
        FromNode::Join {
            left, right, on, ..
        } => {
            collect_sources_from_node(left, catalog, out, seen, rows, view_schema, view_name);
            collect_sources_from_node(right, catalog, out, seen, rows, view_schema, view_name);
            if let Some(on_expr) = on {
                let scope = out.clone();
                collect_expr_view_column_usage(
                    on_expr,
                    &scope,
                    catalog,
                    view_schema,
                    view_name,
                    seen,
                    rows,
                );
            }
        }
    }
}

fn collect_expr_view_column_usage(
    expr: &crate::ast::Expr,
    scope: &[SourceTable],
    catalog: &dyn Catalog,
    view_schema: &str,
    view_name: &str,
    seen: &mut HashSet<(String, String, String)>,
    rows: &mut Vec<StoredRow>,
) {
    match expr {
        crate::ast::Expr::Identifier(name) => {
            if let Some(source) = resolve_unqualified_source(name, scope) {
                if let Some((column_name, _, _)) = source.column_info(name) {
                    push_view_column_usage_row(
                        view_schema,
                        view_name,
                        &source.schema,
                        &source.name,
                        &column_name,
                        seen,
                        rows,
                    );
                }
            }
        }
        crate::ast::Expr::QualifiedIdentifier(parts) => {
            if let Some((source, column_name)) = resolve_qualified_source(parts, scope) {
                push_view_column_usage_row(
                    view_schema,
                    view_name,
                    &source.schema,
                    &source.name,
                    &column_name,
                    seen,
                    rows,
                );
            }
        }
        crate::ast::Expr::FunctionCall { args, .. } => {
            for arg in args {
                collect_expr_view_column_usage(
                    arg,
                    scope,
                    catalog,
                    view_schema,
                    view_name,
                    seen,
                    rows,
                );
            }
        }
        crate::ast::Expr::Binary { left, right, .. } => {
            collect_expr_view_column_usage(
                left,
                scope,
                catalog,
                view_schema,
                view_name,
                seen,
                rows,
            );
            collect_expr_view_column_usage(
                right,
                scope,
                catalog,
                view_schema,
                view_name,
                seen,
                rows,
            );
        }
        crate::ast::Expr::Unary { expr, .. }
        | crate::ast::Expr::IsNull(expr)
        | crate::ast::Expr::IsNotNull(expr)
        | crate::ast::Expr::Cast { expr, .. }
        | crate::ast::Expr::TryCast { expr, .. }
        | crate::ast::Expr::Convert { expr, .. }
        | crate::ast::Expr::TryConvert { expr, .. } => {
            collect_expr_view_column_usage(
                expr,
                scope,
                catalog,
                view_schema,
                view_name,
                seen,
                rows,
            );
        }
        crate::ast::Expr::Case {
            operand,
            when_clauses,
            else_result,
        } => {
            if let Some(operand) = operand {
                collect_expr_view_column_usage(
                    operand,
                    scope,
                    catalog,
                    view_schema,
                    view_name,
                    seen,
                    rows,
                );
            }
            for clause in when_clauses {
                collect_expr_view_column_usage(
                    &clause.condition,
                    scope,
                    catalog,
                    view_schema,
                    view_name,
                    seen,
                    rows,
                );
                collect_expr_view_column_usage(
                    &clause.result,
                    scope,
                    catalog,
                    view_schema,
                    view_name,
                    seen,
                    rows,
                );
            }
            if let Some(else_result) = else_result {
                collect_expr_view_column_usage(
                    else_result,
                    scope,
                    catalog,
                    view_schema,
                    view_name,
                    seen,
                    rows,
                );
            }
        }
        crate::ast::Expr::InList { expr, list, .. } => {
            collect_expr_view_column_usage(
                expr,
                scope,
                catalog,
                view_schema,
                view_name,
                seen,
                rows,
            );
            for item in list {
                collect_expr_view_column_usage(
                    item,
                    scope,
                    catalog,
                    view_schema,
                    view_name,
                    seen,
                    rows,
                );
            }
        }
        crate::ast::Expr::Between {
            expr, low, high, ..
        } => {
            collect_expr_view_column_usage(
                expr,
                scope,
                catalog,
                view_schema,
                view_name,
                seen,
                rows,
            );
            collect_expr_view_column_usage(low, scope, catalog, view_schema, view_name, seen, rows);
            collect_expr_view_column_usage(
                high,
                scope,
                catalog,
                view_schema,
                view_name,
                seen,
                rows,
            );
        }
        crate::ast::Expr::Like { expr, pattern, .. } => {
            collect_expr_view_column_usage(
                expr,
                scope,
                catalog,
                view_schema,
                view_name,
                seen,
                rows,
            );
            collect_expr_view_column_usage(
                pattern,
                scope,
                catalog,
                view_schema,
                view_name,
                seen,
                rows,
            );
        }
        crate::ast::Expr::WindowFunction {
            args,
            partition_by,
            order_by,
            ..
        } => {
            for arg in args {
                collect_expr_view_column_usage(
                    arg,
                    scope,
                    catalog,
                    view_schema,
                    view_name,
                    seen,
                    rows,
                );
            }
            for expr in partition_by {
                collect_expr_view_column_usage(
                    expr,
                    scope,
                    catalog,
                    view_schema,
                    view_name,
                    seen,
                    rows,
                );
            }
            for order in order_by {
                collect_expr_view_column_usage(
                    &order.expr,
                    scope,
                    catalog,
                    view_schema,
                    view_name,
                    seen,
                    rows,
                );
            }
        }
        crate::ast::Expr::Subquery(subquery) => {
            collect_view_column_usage(subquery, catalog, view_schema, view_name, scope, seen, rows);
        }
        crate::ast::Expr::Exists { subquery, .. } => {
            collect_view_column_usage(subquery, catalog, view_schema, view_name, scope, seen, rows);
        }
        crate::ast::Expr::InSubquery { expr, subquery, .. } => {
            collect_expr_view_column_usage(
                expr,
                scope,
                catalog,
                view_schema,
                view_name,
                seen,
                rows,
            );
            collect_view_column_usage(subquery, catalog, view_schema, view_name, scope, seen, rows);
        }
        crate::ast::Expr::Integer(_)
        | crate::ast::Expr::FloatLiteral(_)
        | crate::ast::Expr::BinaryLiteral(_)
        | crate::ast::Expr::String(_)
        | crate::ast::Expr::UnicodeString(_)
        | crate::ast::Expr::Wildcard
        | crate::ast::Expr::QualifiedWildcard(_)
        | crate::ast::Expr::Null => {}
    }
}

fn resolve_unqualified_source<'a>(name: &str, scope: &'a [SourceTable]) -> Option<&'a SourceTable> {
    let mut matches = scope.iter().filter(|source| source.has_column(name));
    let first = matches.next()?;
    if matches.next().is_none() {
        Some(first)
    } else {
        None
    }
}

fn resolve_qualified_source<'a>(
    parts: &[String],
    scope: &'a [SourceTable],
) -> Option<(&'a SourceTable, String)> {
    if parts.len() < 2 {
        return None;
    }

    let column_name = parts.last()?.clone();
    let table_name = parts.get(parts.len().saturating_sub(2))?.clone();

    let source = if parts.len() >= 3 {
        let schema_name = parts.get(parts.len().saturating_sub(3))?;
        scope.iter().find(|source| {
            source.schema.eq_ignore_ascii_case(schema_name)
                && source.name.eq_ignore_ascii_case(&table_name)
                && source.has_column(&column_name)
        })
    } else {
        scope
            .iter()
            .find(|source| source.matches_name(&table_name) && source.has_column(&column_name))
    }?;

    Some((source, column_name))
}

fn push_view_column_usage_row(
    view_schema: &str,
    view_name: &str,
    table_schema: &str,
    table_name: &str,
    column_name: &str,
    seen: &mut HashSet<(String, String, String)>,
    rows: &mut Vec<StoredRow>,
) {
    let key = (
        table_schema.to_ascii_lowercase(),
        table_name.to_ascii_lowercase(),
        column_name.to_ascii_lowercase(),
    );
    if !seen.insert(key) {
        return;
    }

    rows.push(StoredRow {
        values: vec![
            Value::VarChar(DB_CATALOG.to_string()),
            Value::VarChar(view_schema.to_string()),
            Value::VarChar(view_name.to_string()),
            Value::VarChar(DB_CATALOG.to_string()),
            Value::VarChar(table_schema.to_string()),
            Value::VarChar(table_name.to_string()),
            Value::VarChar(column_name.to_string()),
        ],
        deleted: false,
    });
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

const DB_CATALOG: &str = "iridium_sql";



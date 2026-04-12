use super::super::tooling::formatting::format_expr;
use super::virtual_table_def;
use super::VirtualTable;
use crate::ast::{Expr, SelectStmt, TableFactor};
use crate::catalog::{Catalog, RoutineKind};
use crate::storage::StoredRow;
use crate::types::{DataType, Value};

pub(super) struct RoutineColumns;

impl VirtualTable for RoutineColumns {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "ROUTINE_COLUMNS",
            vec![
                ("TABLE_CATALOG", DataType::VarChar { max_len: 128 }, false),
                ("TABLE_SCHEMA", DataType::VarChar { max_len: 128 }, false),
                ("TABLE_NAME", DataType::VarChar { max_len: 128 }, false),
                ("COLUMN_NAME", DataType::VarChar { max_len: 128 }, false),
                ("ORDINAL_POSITION", DataType::Int, false),
                ("COLUMN_DEFAULT", DataType::VarChar { max_len: 128 }, true),
                ("IS_NULLABLE", DataType::VarChar { max_len: 3 }, false),
                ("DATA_TYPE", DataType::VarChar { max_len: 128 }, false),
                ("CHARACTER_MAXIMUM_LENGTH", DataType::Int, true),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog) -> Vec<StoredRow> {
        let mut rows = Vec::new();
        for routine in catalog.get_routines() {
            let RoutineKind::Function {
                body: crate::ast::FunctionBody::InlineTable(select),
                ..
            } = &routine.kind
            else {
                continue;
            };

            let Some(source) = single_source_table(select, catalog) else {
                continue;
            };

            let mut ordinal = 1;
            for item in &select.projection {
                match &item.expr {
                    Expr::Wildcard => {
                        for (name, ty, nullable) in &source.columns {
                            rows.push(routine_column_row(
                                &routine.schema,
                                &routine.name,
                                name,
                                ordinal,
                                nullable,
                                ty,
                            ));
                            ordinal += 1;
                        }
                    }
                    Expr::QualifiedWildcard(parts) => {
                        if wildcard_matches_source(parts, &source) {
                            for (name, ty, nullable) in &source.columns {
                                rows.push(routine_column_row(
                                    &routine.schema,
                                    &routine.name,
                                    name,
                                    ordinal,
                                    nullable,
                                    ty,
                                ));
                                ordinal += 1;
                            }
                        }
                    }
                    expr => {
                        let column_name = item
                            .alias
                            .clone()
                            .unwrap_or_else(|| default_column_name(expr));
                        let resolved = resolve_source_column(expr, &source);
                        let (nullable, data_type, char_max_length) = resolved
                            .as_ref()
                            .map(|(_, ty, nullable)| (*nullable, ty.clone(), char_max_length(ty)))
                            .unwrap_or_else(|| {
                                (true, DataType::VarChar { max_len: 128 }, Value::Int(128))
                            });

                        rows.push(StoredRow {
                            values: vec![
                                Value::VarChar(DB_CATALOG.to_string()),
                                Value::VarChar(routine.schema.clone()),
                                Value::VarChar(routine.name.clone()),
                                Value::VarChar(column_name),
                                Value::Int(ordinal),
                                Value::Null,
                                Value::VarChar(if nullable { "YES" } else { "NO" }.to_string()),
                                Value::VarChar(data_type_name(&data_type)),
                                char_max_length,
                            ],
                            deleted: false,
                        });
                        ordinal += 1;
                    }
                }
            }
        }
        rows.sort_by(|a, b| compare_rows(&a.values, &b.values));
        rows
    }
}

#[derive(Clone)]
struct SourceTable {
    columns: Vec<(String, DataType, bool)>,
    name: String,
}

fn single_source_table(select: &SelectStmt, catalog: &dyn Catalog) -> Option<SourceTable> {
    let from = select.from_clause.as_ref()?;
    let table = match from {
        crate::ast::FromNode::Table(table) => table,
        _ => return None,
    };

    let TableFactor::Named(name) = &table.factor else {
        return None;
    };
    let def = catalog.find_table(name.schema_or_dbo(), &name.name)?;
    Some(SourceTable {
        columns: def
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.data_type.clone(), c.nullable))
            .collect(),
        name: name.name.clone(),
    })
}

fn wildcard_matches_source(parts: &[String], source: &SourceTable) -> bool {
    if parts.is_empty() {
        return false;
    }
    parts.len() == 1 && parts[0].eq_ignore_ascii_case(&source.name)
}

fn resolve_source_column(expr: &Expr, source: &SourceTable) -> Option<(String, DataType, bool)> {
    match expr {
        Expr::Identifier(name) => source
            .columns
            .iter()
            .find(|(col_name, _, _)| col_name.eq_ignore_ascii_case(name))
            .cloned(),
        Expr::QualifiedIdentifier(parts) => parts.last().and_then(|name| {
            source
                .columns
                .iter()
                .find(|(col_name, _, _)| col_name.eq_ignore_ascii_case(name))
                .cloned()
        }),
        _ => None,
    }
}

fn default_column_name(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(name) => name.clone(),
        Expr::QualifiedIdentifier(parts) => {
            parts.last().cloned().unwrap_or_else(|| format_expr(expr))
        }
        _ => format_expr(expr),
    }
}

fn routine_column_row(
    schema: &str,
    routine_name: &str,
    column_name: &str,
    ordinal: i32,
    nullable: &bool,
    ty: &DataType,
) -> StoredRow {
    StoredRow {
        values: vec![
            Value::VarChar(DB_CATALOG.to_string()),
            Value::VarChar(schema.to_string()),
            Value::VarChar(routine_name.to_string()),
            Value::VarChar(column_name.to_string()),
            Value::Int(ordinal),
            Value::Null,
            Value::VarChar(if *nullable { "YES" } else { "NO" }.to_string()),
            Value::VarChar(data_type_name(ty)),
            char_max_length(ty),
        ],
        deleted: false,
    }
}

fn data_type_name(dt: &DataType) -> String {
    match dt {
        DataType::Bit => "bit",
        DataType::TinyInt => "tinyint",
        DataType::SmallInt => "smallint",
        DataType::Int => "int",
        DataType::BigInt => "bigint",
        DataType::Float => "float",
        DataType::Decimal { .. } => "decimal",
        DataType::Money => "money",
        DataType::SmallMoney => "smallmoney",
        DataType::Char { .. } => "char",
        DataType::VarChar { .. } => "varchar",
        DataType::NChar { .. } => "nchar",
        DataType::NVarChar { .. } => "nvarchar",
        DataType::Binary { .. } => "binary",
        DataType::VarBinary { .. } => "varbinary",
        DataType::Date => "date",
        DataType::Time => "time",
        DataType::DateTime => "datetime",
        DataType::DateTime2 => "datetime2",
        DataType::UniqueIdentifier => "uniqueidentifier",
        DataType::SqlVariant => "sql_variant",
        DataType::Xml => "xml",
    }
    .to_string()
}

fn char_max_length(dt: &DataType) -> Value {
    match dt {
        DataType::Char { len } | DataType::NChar { len } => Value::Int(*len as i32),
        DataType::VarChar { max_len } | DataType::NVarChar { max_len } => {
            Value::Int(*max_len as i32)
        }
        DataType::Binary { len } => Value::Int(*len as i32),
        DataType::VarBinary { max_len } => Value::Int(*max_len as i32),
        DataType::Xml => Value::Int(-1),
        _ => Value::Null,
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

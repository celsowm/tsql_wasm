use super::super::super::VirtualTable;
use super::super::super::{system_type_id, type_max_length, virtual_table_def};
use crate::ast::{DmlStatement, Expr, SelectItem, Statement};
use crate::catalog::Catalog;
use crate::executor::context::ExecutionContext;
use crate::storage::StoredRow;
use crate::types::{DataType, Value};

pub(crate) struct SysColumns;
pub(crate) struct SysAllColumns;
pub(crate) struct SysViewColumns;

impl VirtualTable for SysColumns {
    fn definition(&self) -> crate::catalog::TableDef {
        column_table_def("columns", false)
    }

    fn rows(&self, catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        column_rows(catalog, false)
    }
}

impl VirtualTable for SysAllColumns {
    fn definition(&self) -> crate::catalog::TableDef {
        column_table_def("all_columns", true)
    }

    fn rows(&self, catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        column_rows(catalog, true)
    }
}

impl VirtualTable for SysViewColumns {
    fn definition(&self) -> crate::catalog::TableDef {
        view_column_table_def()
    }

    fn rows(&self, catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        view_column_rows(catalog)
    }
}

fn column_table_def(name: &str, include_sparse: bool) -> crate::catalog::TableDef {
    let mut cols = vec![
        ("object_id", DataType::Int, false),
        ("column_id", DataType::Int, false),
        ("name", DataType::VarChar { max_len: 128 }, false),
        ("user_type_id", DataType::Int, false),
        ("system_type_id", DataType::TinyInt, false),
        ("max_length", DataType::SmallInt, false),
        ("precision", DataType::TinyInt, false),
        ("scale", DataType::TinyInt, false),
        ("is_nullable", DataType::Bit, false),
        ("is_computed", DataType::Bit, false),
        ("is_xml_document", DataType::Bit, false),
        ("is_column_set", DataType::Bit, false),
        ("xml_collection_id", DataType::Int, true),
        ("generated_always_type", DataType::TinyInt, false),
        ("graph_type", DataType::Int, true),
        ("default_object_id", DataType::Int, false),
        ("is_dropped_ledger_column", DataType::Bit, false),
        ("vector_dimensions", DataType::Int, true),
        ("vector_base_type", DataType::TinyInt, true),
        (
            "vector_base_type_desc",
            DataType::VarChar { max_len: 10 },
            true,
        ),
    ];
    if include_sparse {
        cols.push(("is_sparse", DataType::Bit, false));
    }
    virtual_table_def(name, cols)
}

fn column_rows(catalog: &dyn Catalog, include_sparse: bool) -> Vec<StoredRow> {
    fn precision_scale(dt: &DataType) -> (u8, u8) {
        match dt {
            DataType::Bit => (1, 0),
            DataType::TinyInt => (3, 0),
            DataType::SmallInt => (5, 0),
            DataType::Int => (10, 0),
            DataType::BigInt => (19, 0),
            DataType::Float => (53, 0),
            DataType::Decimal { precision, scale } => (*precision, *scale),
            DataType::Money => (19, 4),
            DataType::SmallMoney => (10, 4),
            _ => (0, 0),
        }
    }

    fn vector_metadata(dt: &DataType) -> (Value, Value, Value) {
        match dt {
            DataType::Vector { dimensions } => (
                Value::Int(*dimensions as i32),
                Value::TinyInt(0),
                Value::VarChar("float32".to_string()),
            ),
            _ => (Value::Null, Value::Null, Value::Null),
        }
    }

    let mut rows = Vec::new();
    for t in catalog.get_tables() {
        for c in &t.columns {
            let (precision, scale) = precision_scale(&c.data_type);
            let (vector_dimensions, vector_base_type, vector_base_type_desc) =
                vector_metadata(&c.data_type);
            let mut values = vec![
                Value::Int(t.id as i32),
                Value::Int(c.id as i32),
                Value::VarChar(c.name.clone()),
                Value::TinyInt(system_type_id(&c.data_type) as u8),
                Value::TinyInt(system_type_id(&c.data_type) as u8),
                Value::SmallInt(type_max_length(&c.data_type)),
                Value::TinyInt(precision),
                Value::TinyInt(scale),
                Value::Bit(c.nullable),
                Value::Bit(c.computed_expr.is_some()),
                Value::Bit(false),
                Value::Bit(false),
                Value::Null,
                Value::TinyInt(0),
                Value::Null,
                Value::Int(if c.default.is_some() {
                    let table_bucket = (t.id % 100_000) as i32;
                    3_000_000 + table_bucket * 1_000 + c.id as i32
                } else {
                    0
                }),
                Value::Bit(false),
                vector_dimensions,
                vector_base_type,
                vector_base_type_desc,
            ];
            if include_sparse {
                values.push(Value::Bit(false));
            }
            rows.push(StoredRow {
                values,
                deleted: false,
            });
        }
    }

    for tt in catalog.get_table_types() {
        for (i, c) in tt.columns.iter().enumerate() {
            let dt = crate::executor::type_mapping::data_type_spec_to_runtime(&c.data_type);
            let (precision, scale) = precision_scale(&dt);
            let (vector_dimensions, vector_base_type, vector_base_type_desc) = vector_metadata(&dt);
            let mut values = vec![
                Value::Int(tt.object_id),
                Value::Int((i + 1) as i32),
                Value::VarChar(c.name.clone()),
                Value::TinyInt(system_type_id(&dt) as u8),
                Value::TinyInt(system_type_id(&dt) as u8),
                Value::SmallInt(type_max_length(&dt)),
                Value::TinyInt(precision),
                Value::TinyInt(scale),
                Value::Bit(c.nullable),
                Value::Bit(c.computed_expr.is_some()),
                Value::Bit(false),
                Value::Bit(false),
                Value::Null,
                Value::TinyInt(0),
                Value::Null,
                Value::Int(0),
                Value::Bit(false),
                vector_dimensions,
                vector_base_type,
                vector_base_type_desc,
            ];
            if include_sparse {
                values.push(Value::Bit(false));
            }
            rows.push(StoredRow {
                values,
                deleted: false,
            });
        }
    }
    rows
}

fn view_column_table_def() -> crate::catalog::TableDef {
    virtual_table_def(
        "view_columns",
        vec![
            ("object_id", DataType::Int, false),
            ("column_id", DataType::Int, false),
            ("name", DataType::VarChar { max_len: 128 }, false),
            ("user_type_id", DataType::Int, false),
            ("system_type_id", DataType::TinyInt, false),
            ("max_length", DataType::SmallInt, false),
            ("precision", DataType::TinyInt, false),
            ("scale", DataType::TinyInt, false),
            ("is_nullable", DataType::Bit, false),
            ("is_computed", DataType::Bit, false),
            ("is_xml_document", DataType::Bit, false),
            ("is_column_set", DataType::Bit, false),
            ("xml_collection_id", DataType::Int, true),
            ("generated_always_type", DataType::TinyInt, false),
            ("graph_type", DataType::Int, true),
            ("default_object_id", DataType::Int, false),
            ("is_dropped_ledger_column", DataType::Bit, false),
            ("vector_dimensions", DataType::Int, true),
            ("vector_base_type", DataType::TinyInt, true),
            (
                "vector_base_type_desc",
                DataType::VarChar { max_len: 10 },
                true,
            ),
        ],
    )
}

fn view_column_rows(catalog: &dyn Catalog) -> Vec<StoredRow> {
    let mut rows = Vec::new();
    for v in catalog.get_views() {
        let object_id = if v.object_id != 0 {
            v.object_id
        } else {
            catalog.object_id(&v.schema, &v.name).unwrap_or(0)
        };
        let Some(select) = extract_view_select(&v.query) else {
            continue;
        };
        for (i, item) in select.projection.iter().enumerate() {
            let name = extract_column_name(item);
            rows.push(StoredRow {
                values: vec![
                    Value::Int(object_id),
                    Value::Int((i + 1) as i32),
                    Value::VarChar(name),
                    Value::Int(0),      // user_type_id - unknown for view columns
                    Value::TinyInt(0),  // system_type_id - unknown
                    Value::SmallInt(0), // max_length - unknown
                    Value::TinyInt(0),  // precision - unknown
                    Value::TinyInt(0),  // scale - unknown
                    Value::Bit(true),   // is_nullable - true by default
                    Value::Bit(false),  // is_computed
                    Value::Bit(false),  // is_xml_document
                    Value::Bit(false),  // is_column_set
                    Value::Null,        // xml_collection_id
                    Value::TinyInt(0),  // generated_always_type
                    Value::Null,        // graph_type
                    Value::Int(0),      // default_object_id
                    Value::Bit(false),  // is_dropped_ledger_column
                    Value::Null,        // vector_dimensions
                    Value::Null,        // vector_base_type
                    Value::Null,        // vector_base_type_desc
                ],
                deleted: false,
            });
        }
    }
    rows
}

fn extract_view_select(stmt: &Statement) -> Option<&crate::ast::SelectStmt> {
    match stmt {
        Statement::Dml(DmlStatement::Select(select)) => Some(select),
        _ => None,
    }
}

fn extract_column_name(item: &SelectItem) -> String {
    if let Some(ref alias) = item.alias {
        return alias.clone();
    }
    match &item.expr {
        Expr::Identifier(name) => name.clone(),
        Expr::QualifiedIdentifier(parts) => parts.last().cloned().unwrap_or_default(),
        Expr::FunctionCall { name, .. } => name.clone(),
        Expr::Binary { .. } => String::new(),
        Expr::Unary { expr, .. } => extract_name_from_expr(expr),
        Expr::Case { .. } => String::new(),
        Expr::Cast { expr, .. } => extract_name_from_expr(expr),
        Expr::Convert { expr, .. } => extract_name_from_expr(expr),
        Expr::TryCast { expr, .. } => extract_name_from_expr(expr),
        Expr::TryConvert { expr, .. } => extract_name_from_expr(expr),
        _ => String::new(),
    }
}

fn extract_name_from_expr(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(name) => name.clone(),
        Expr::QualifiedIdentifier(parts) => parts.last().cloned().unwrap_or_default(),
        _ => String::new(),
    }
}

pub(crate) struct SysComputedColumns;

impl VirtualTable for SysComputedColumns {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "computed_columns",
            vec![
                ("object_id", DataType::Int, false),
                ("column_id", DataType::Int, false),
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("is_computed", DataType::Bit, false),
                ("is_persisted", DataType::Bit, false),
                ("definition", DataType::NVarChar { max_len: 4000 }, true),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        let mut rows = Vec::new();
        for table in catalog.get_tables() {
            for col in &table.columns {
                if let Some(_expr) = &col.computed_expr {
                    rows.push(StoredRow {
                        values: vec![
                            Value::Int(table.id as i32),
                            Value::Int(col.id as i32),
                            Value::VarChar(col.name.clone()),
                            Value::Bit(true),
                            Value::Bit(false),
                            Value::NVarChar(
                                col.computed_expr.as_ref().map_or(String::new(), |e| {
                                    crate::executor::tooling::formatting::format_expr(e)
                                }),
                            ),
                        ],
                        deleted: false,
                    });
                }
            }
        }
        rows
    }
}

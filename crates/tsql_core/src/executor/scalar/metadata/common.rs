use crate::ast::Expr;
use crate::catalog::{Catalog, IndexDef, RoutineDef, TableDef, TriggerDef, ViewDef};
use crate::error::DbError;
use crate::storage::Storage;
use crate::types::{DataType, Value};

use crate::executor::clock::Clock;
use crate::executor::context::ExecutionContext;
use crate::executor::evaluator::eval_expr;
use crate::executor::model::ContextTable;
use crate::executor::tooling::{
    format_routine_definition, format_trigger_definition, format_view_definition,
};

pub(super) enum ResolvedObject<'a> {
    Table(&'a TableDef),
    Routine(&'a RoutineDef),
    View,
    Trigger,
}

pub(super) fn parse_object_parts(raw: &str) -> (Option<&str>, &str) {
    let cleaned = raw.trim().trim_matches('[').trim_matches(']');
    let parts: Vec<&str> = cleaned
        .split('.')
        .map(|p| p.trim().trim_matches('[').trim_matches(']'))
        .filter(|p| !p.is_empty())
        .collect();
    match parts.as_slice() {
        [] => (None, cleaned),
        [name] => (None, *name),
        [schema, name] => (Some(*schema), *name),
        [.., schema, name] => (Some(*schema), *name),
    }
}

pub(super) fn value_to_object_id(
    value: &Value,
    catalog: &dyn Catalog,
    default_schema: Option<&str>,
) -> Option<i32> {
    match value {
        Value::Int(v) => Some(*v),
        Value::BigInt(v) => Some(*v as i32),
        Value::SmallInt(v) => Some(*v as i32),
        Value::TinyInt(v) => Some(*v as i32),
        Value::VarChar(_) | Value::NVarChar(_) | Value::Char(_) | Value::NChar(_) => {
            let raw = value.to_string_value();
            let (schema, name) = parse_object_parts(&raw);
            let schema = schema.or(default_schema).unwrap_or("dbo");
            catalog.object_id(schema, name)
        }
        _ => None,
    }
}

pub(super) fn schema_name_by_id(catalog: &dyn Catalog, schema_id: u32) -> Option<String> {
    catalog
        .get_schemas()
        .iter()
        .find(|s| s.id == schema_id)
        .map(|s| s.name.clone())
}

pub(super) fn table_by_object_id(catalog: &dyn Catalog, object_id: i32) -> Option<&TableDef> {
    catalog
        .get_tables()
        .iter()
        .find(|t| t.id as i32 == object_id)
}

pub(super) fn routine_by_object_id(catalog: &dyn Catalog, object_id: i32) -> Option<&RoutineDef> {
    catalog
        .get_routines()
        .iter()
        .find(|r| r.object_id == object_id)
}

pub(super) fn view_by_object_id(catalog: &dyn Catalog, object_id: i32) -> Option<&ViewDef> {
    catalog
        .get_views()
        .iter()
        .find(|v| v.object_id == object_id)
}

pub(super) fn trigger_by_object_id(catalog: &dyn Catalog, object_id: i32) -> Option<&TriggerDef> {
    catalog
        .get_triggers()
        .iter()
        .find(|t| t.object_id == object_id)
}

pub(super) fn object_schema_name_from_id(catalog: &dyn Catalog, object_id: i32) -> Option<String> {
    if let Some(table) = table_by_object_id(catalog, object_id) {
        return schema_name_by_id(catalog, table.schema_id);
    }
    if let Some(routine) = routine_by_object_id(catalog, object_id) {
        return Some(routine.schema.clone());
    }
    if let Some(view) = view_by_object_id(catalog, object_id) {
        return Some(view.schema.clone());
    }
    if let Some(trigger) = trigger_by_object_id(catalog, object_id) {
        return Some(trigger.schema.clone());
    }
    None
}

pub(super) fn object_name_from_id(catalog: &dyn Catalog, object_id: i32) -> Option<String> {
    if let Some(table) = table_by_object_id(catalog, object_id) {
        return Some(table.name.clone());
    }
    if let Some(routine) = routine_by_object_id(catalog, object_id) {
        return Some(routine.name.clone());
    }
    if let Some(view) = view_by_object_id(catalog, object_id) {
        return Some(view.name.clone());
    }
    if let Some(trigger) = trigger_by_object_id(catalog, object_id) {
        return Some(trigger.name.clone());
    }
    None
}

pub(super) fn object_definition_from_id(catalog: &dyn Catalog, object_id: i32) -> Option<String> {
    if let Some(routine) = routine_by_object_id(catalog, object_id) {
        if !routine.definition_sql.is_empty() {
            return Some(routine.definition_sql.clone());
        }
        return Some(format_routine_definition(routine));
    }
    if let Some(view) = view_by_object_id(catalog, object_id) {
        if !view.definition_sql.is_empty() {
            return Some(view.definition_sql.clone());
        }
        return Some(format_view_definition(view));
    }
    if let Some(trigger) = trigger_by_object_id(catalog, object_id) {
        if !trigger.definition_sql.is_empty() {
            return Some(trigger.definition_sql.clone());
        }
        return Some(format_trigger_definition(trigger));
    }
    None
}

pub(super) fn resolve_object<'a>(
    catalog: &'a dyn Catalog,
    object_id: i32,
) -> Option<ResolvedObject<'a>> {
    if let Some(table) = table_by_object_id(catalog, object_id) {
        return Some(ResolvedObject::Table(table));
    }
    if let Some(routine) = routine_by_object_id(catalog, object_id) {
        return Some(ResolvedObject::Routine(routine));
    }
    if let Some(view) = view_by_object_id(catalog, object_id) {
        let _ = view;
        return Some(ResolvedObject::View);
    }
    if let Some(trigger) = trigger_by_object_id(catalog, object_id) {
        let _ = trigger;
        return Some(ResolvedObject::Trigger);
    }
    None
}

pub(super) fn builtin_type_id(name: &str) -> Option<i32> {
    match name.to_ascii_lowercase().as_str() {
        "bit" => Some(104),
        "tinyint" => Some(48),
        "smallint" => Some(52),
        "int" => Some(56),
        "bigint" => Some(127),
        "float" => Some(62),
        "decimal" => Some(106),
        "money" => Some(60),
        "smallmoney" => Some(59),
        "char" => Some(175),
        "varchar" => Some(167),
        "nchar" => Some(239),
        "nvarchar" => Some(231),
        "binary" => Some(173),
        "varbinary" => Some(165),
        "date" => Some(40),
        "time" => Some(41),
        "datetime" => Some(61),
        "datetime2" => Some(42),
        "uniqueidentifier" => Some(36),
        "sql_variant" => Some(98),
        _ => None,
    }
}

pub(super) fn builtin_type_name(type_id: i32) -> Option<&'static str> {
    match type_id {
        104 => Some("bit"),
        48 => Some("tinyint"),
        52 => Some("smallint"),
        56 => Some("int"),
        127 => Some("bigint"),
        62 => Some("float"),
        106 => Some("decimal"),
        60 => Some("money"),
        59 => Some("smallmoney"),
        175 => Some("char"),
        167 => Some("varchar"),
        239 => Some("nchar"),
        231 => Some("nvarchar"),
        173 => Some("binary"),
        165 => Some("varbinary"),
        40 => Some("date"),
        41 => Some("time"),
        61 => Some("datetime"),
        42 => Some("datetime2"),
        36 => Some("uniqueidentifier"),
        98 => Some("sql_variant"),
        _ => None,
    }
}

pub(super) fn resolve_type_id(catalog: &dyn Catalog, type_name: &str) -> Option<i32> {
    let (schema, name) = parse_object_parts(type_name);
    let schema = schema.unwrap_or("dbo");
    if let Some(id) = catalog.find_table_type(schema, name).map(|t| t.object_id) {
        return Some(id);
    }
    builtin_type_id(name)
}

pub(super) fn resolve_type_name(catalog: &dyn Catalog, type_id: i32) -> Option<String> {
    if let Some(name) = builtin_type_name(type_id) {
        return Some(name.to_string());
    }
    catalog
        .get_table_types()
        .iter()
        .find(|t| t.object_id == type_id)
        .map(|t| format!("{}.{}", t.schema, t.name))
}

pub(super) fn type_precision(name: &str) -> Option<i32> {
    match name.to_ascii_lowercase().as_str() {
        "bit" => Some(1),
        "tinyint" => Some(3),
        "smallint" => Some(5),
        "int" => Some(10),
        "bigint" => Some(19),
        "float" => Some(53),
        "decimal" => Some(38),
        "money" => Some(19),
        "smallmoney" => Some(10),
        _ => None,
    }
}

pub(super) fn type_scale(name: &str) -> Option<i32> {
    match name.to_ascii_lowercase().as_str() {
        "decimal" => Some(0),
        "money" | "smallmoney" => Some(4),
        _ => None,
    }
}

pub(super) fn storage_length(dt: &DataType) -> i32 {
    match dt {
        DataType::Bit => 1,
        DataType::TinyInt => 1,
        DataType::SmallInt => 2,
        DataType::Int => 4,
        DataType::BigInt => 8,
        DataType::Float => 8,
        DataType::Decimal { precision, .. } => match *precision {
            1..=9 => 5,
            10..=19 => 9,
            20..=28 => 13,
            _ => 17,
        },
        DataType::Money => 8,
        DataType::SmallMoney => 4,
        DataType::Char { len } => *len as i32,
        DataType::VarChar { max_len } => *max_len as i32,
        DataType::NChar { len } => (*len as i32) * 2,
        DataType::NVarChar { max_len } => (*max_len as i32) * 2,
        DataType::Binary { len } => *len as i32,
        DataType::VarBinary { max_len } => *max_len as i32,
        DataType::Date => 3,
        DataType::Time => 5,
        DataType::DateTime => 8,
        DataType::DateTime2 => 8,
        DataType::UniqueIdentifier => 16,
        DataType::SqlVariant => 8016,
        DataType::Xml => -1,
    }
}

pub(super) fn table_has_identity(table: &TableDef) -> bool {
    table.columns.iter().any(|c| c.identity.is_some())
}

pub(super) fn table_has_primary_key(table: &TableDef) -> bool {
    table.columns.iter().any(|c| c.primary_key)
}

pub(super) fn table_has_unique_constraint(table: &TableDef) -> bool {
    table.columns.iter().any(|c| c.unique)
}

pub(super) fn table_has_default_constraint(table: &TableDef) -> bool {
    table
        .columns
        .iter()
        .any(|c| c.default.is_some() || c.default_constraint_name.is_some())
}

pub(super) fn table_has_check_constraint(table: &TableDef) -> bool {
    !table.check_constraints.is_empty() || table.columns.iter().any(|c| c.check.is_some())
}

pub(super) fn table_has_foreign_key(table: &TableDef) -> bool {
    !table.foreign_keys.is_empty()
}

pub(super) fn table_has_index(catalog: &dyn Catalog, table: &TableDef) -> bool {
    catalog
        .get_indexes()
        .iter()
        .any(|idx| idx.table_id == table.id)
}

pub(super) fn index_by_id(
    catalog: &dyn Catalog,
    table_id: i32,
    index_id: i32,
) -> Option<&IndexDef> {
    catalog
        .get_indexes()
        .iter()
        .find(|idx| idx.table_id as i32 == table_id && idx.id as i32 == index_id)
}

pub(super) fn index_by_name<'a>(
    catalog: &'a dyn Catalog,
    table_id: i32,
    index_name: &str,
) -> Option<&'a IndexDef> {
    catalog
        .get_indexes()
        .iter()
        .find(|idx| idx.table_id as i32 == table_id && idx.name.eq_ignore_ascii_case(index_name))
}

pub(super) fn table_column_by_ordinal(
    table: &TableDef,
    ordinal: i32,
) -> Option<&crate::catalog::ColumnDef> {
    if ordinal <= 0 {
        return None;
    }
    table.columns.get((ordinal - 1) as usize)
}

pub(super) fn eval_expr_to_value(
    expr: &Expr,
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    eval_expr(expr, row, ctx, catalog, storage, clock)
}

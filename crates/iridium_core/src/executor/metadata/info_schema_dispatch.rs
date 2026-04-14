use super::VirtualTable;
use super::{
    info_schema_columns, info_schema_constraints, info_schema_empty, info_schema_privileges,
    info_schema_routine_columns, info_schema_routines, info_schema_tables, info_schema_types,
    info_schema_views,
};

pub(super) fn lookup(name: &str) -> Option<Box<dyn VirtualTable>> {
    if let Some(vt) = info_schema_tables::lookup(name) {
        return Some(vt);
    }
    if let Some(vt) = info_schema_columns::lookup(name) {
        return Some(vt);
    }
    if let Some(vt) = info_schema_constraints::lookup(name) {
        return Some(vt);
    }
    if let Some(vt) = info_schema_routines::lookup(name) {
        return Some(vt);
    }
    if name.eq_ignore_ascii_case("ROUTINE_COLUMNS") {
        return Some(Box::new(info_schema_routine_columns::RoutineColumns));
    }
    if name.eq_ignore_ascii_case("TABLE_PRIVILEGES") {
        return Some(Box::new(info_schema_privileges::TablePrivileges));
    }
    if name.eq_ignore_ascii_case("COLUMN_PRIVILEGES") {
        return Some(Box::new(info_schema_privileges::ColumnPrivileges));
    }
    if name.eq_ignore_ascii_case("VIEW_TABLE_USAGE") {
        return Some(Box::new(info_schema_views::ViewTableUsage));
    }
    if name.eq_ignore_ascii_case("VIEW_COLUMN_USAGE") {
        return Some(Box::new(info_schema_views::ViewColumnUsage));
    }
    if name.eq_ignore_ascii_case("DOMAINS") {
        return Some(Box::new(info_schema_types::Domains));
    }
    if name.eq_ignore_ascii_case("COLUMN_DOMAIN_USAGE") {
        return Some(Box::new(info_schema_types::ColumnDomainUsage));
    }
    if let Some(vt) = info_schema_empty::lookup(name) {
        return Some(vt);
    }
    None
}

use super::VirtualTable;

pub(crate) fn lookup(name: &str) -> Option<Box<dyn VirtualTable>> {
    super::info_schema_tables::lookup(name)
        .or_else(|| super::info_schema_columns::lookup(name))
        .or_else(|| super::info_schema_routines::lookup(name))
        .or_else(|| super::info_schema_constraints::lookup(name))
        .or_else(|| super::info_schema_empty::lookup(name))
}

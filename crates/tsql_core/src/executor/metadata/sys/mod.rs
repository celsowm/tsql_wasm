mod tables;
mod constraints;
mod routines;
mod objects;
mod indexes;
mod host_info;

use super::VirtualTable;

pub(crate) fn lookup(name: &str) -> Option<Box<dyn VirtualTable>> {
    if name.eq_ignore_ascii_case("schemas") {
        Some(Box::new(tables::SysSchemas))
    } else if name.eq_ignore_ascii_case("databases") {
        Some(Box::new(tables::SysDatabases))
    } else if name.eq_ignore_ascii_case("sysdatabases") {
        Some(Box::new(tables::SysSysDatabases))
    } else if name.eq_ignore_ascii_case("configurations") {
        Some(Box::new(tables::SysConfigurations))
    } else if name.eq_ignore_ascii_case("tables") {
        Some(Box::new(tables::SysTables))
    } else if name.eq_ignore_ascii_case("columns") {
        Some(Box::new(tables::SysColumns))
    } else if name.eq_ignore_ascii_case("types") {
        Some(Box::new(tables::SysTypes))
    } else if name.eq_ignore_ascii_case("indexes") {
        Some(Box::new(indexes::SysIndexes))
    } else if name.eq_ignore_ascii_case("objects") {
        Some(Box::new(objects::SysObjects))
    } else if name.eq_ignore_ascii_case("dm_os_host_info") {
        Some(Box::new(host_info::SysHostInfo))
    } else if name.eq_ignore_ascii_case("check_constraints") {
        Some(Box::new(constraints::SysCheckConstraints))
    } else if name.eq_ignore_ascii_case("routines") {
        Some(Box::new(routines::SysRoutines))
    } else if name.eq_ignore_ascii_case("foreign_keys") {
        Some(Box::new(constraints::SysForeignKeys))
    } else if name.eq_ignore_ascii_case("key_constraints") {
        Some(Box::new(constraints::SysKeyConstraints))
    } else if name.eq_ignore_ascii_case("default_constraints") {
        Some(Box::new(constraints::SysDefaultConstraints))
    } else {
        None
    }
}

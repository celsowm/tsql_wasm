use super::VirtualTable;

pub(super) fn lookup(_name: &str) -> Option<Box<dyn VirtualTable>> {
    None
}

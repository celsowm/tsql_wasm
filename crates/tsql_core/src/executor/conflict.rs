use std::collections::{HashMap, HashSet};

use crate::ast::IsolationLevel;

pub(crate) fn detect_conflicts(
    isolation_level: IsolationLevel,
    base_versions: &HashMap<String, u64>,
    read_tables: &HashSet<String>,
    write_tables: &HashSet<String>,
    current_versions: &HashMap<String, u64>,
) -> bool {
    let has_changed = |table: &str| -> bool {
        let base = base_versions.get(table).copied().unwrap_or(0);
        let now = current_versions.get(table).copied().unwrap_or(0);
        now > base
    };

    match isolation_level {
        IsolationLevel::ReadUncommitted | IsolationLevel::ReadCommitted => false,
        IsolationLevel::Snapshot => write_tables.iter().any(|t| has_changed(t)),
        IsolationLevel::RepeatableRead => read_tables.iter().any(|t| has_changed(t)),
        IsolationLevel::Serializable => {
            read_tables.iter().any(|t| has_changed(t))
                || write_tables.iter().any(|t| has_changed(t))
        }
    }
}

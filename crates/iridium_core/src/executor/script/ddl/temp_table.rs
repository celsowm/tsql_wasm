use crate::ast::{CreateTableStmt, DropTableStmt};
use crate::executor::context::ExecutionContext;
use crate::executor::string_norm::normalize_identifier;

/// Maps a `#temp` table name to its physical name and registers it in the
/// session temp_map. Called before CREATE TABLE for temp tables.
pub(crate) fn map_create_temp_table(
    stmt: &mut CreateTableStmt,
    ctx: &mut ExecutionContext<'_>,
) {
    if !stmt.name.name.starts_with('#') {
        return;
    }
    let logical = stmt.name.name.clone();
    let physical = format!("__temp_{}", logical.trim_start_matches('#'));
    ctx.session
        .temp_map
        .insert(normalize_identifier(&logical), physical.clone());
    stmt.name.schema = Some("dbo".to_string());
    stmt.name.name = physical;
}

/// Resolves a `#temp` or `@tableVar` name to its physical name for DROP TABLE.
/// Returns `true` if the name was resolved (temp table or table var).
pub(crate) fn resolve_drop_table_name(
    stmt: &mut DropTableStmt,
    ctx: &mut ExecutionContext<'_>,
) -> bool {
    if stmt.name.name.starts_with('#') {
        let key = normalize_identifier(&stmt.name.name);
        if let Some(mapped) = ctx.session.temp_map.remove(&key) {
            stmt.name.schema = Some("dbo".to_string());
            stmt.name.name = mapped;
            return true;
        }
    } else if stmt.name.name.starts_with('@') {
        if let Some(mapped) = ctx.resolve_table_name(&stmt.name.name) {
            stmt.name.schema = Some("dbo".to_string());
            stmt.name.name = mapped;
            return true;
        }
    }
    false
}

/// Resolves any table name (regular, temp, or table var) for TRUNCATE / ALTER.
pub(crate) fn resolve_table_name_for_mutation(
    name: &mut crate::ast::ObjectName,
    ctx: &ExecutionContext<'_>,
) -> Option<String> {
    let mapped = ctx.resolve_table_name(&name.name)?;
    name.name = mapped;
    if name.schema.is_none() {
        name.schema = Some("dbo".to_string());
    }
    Some(name.name.clone())
}

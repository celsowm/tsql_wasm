use crate::ast;
use crate::error::DbError;

pub use super::database::{
    CheckpointManager, Database, DatabaseInner, Engine, EngineInner, SqlAnalyzer, StatementExecutor,
};
pub use super::locks::SessionId;
pub use super::projection::deduplicate_projected_rows;
pub use super::result::QueryResult;
pub use super::session::SessionManager;

pub(crate) fn execute_set_op(
    left: QueryResult,
    right: QueryResult,
    op: ast::SetOpKind,
) -> Result<QueryResult, DbError> {
    if left.columns.len() != right.columns.len() {
        return Err(DbError::Execution(
            "set operations require same number of columns".into(),
        ));
    }

    let rows = match op {
        ast::SetOpKind::Union => {
            let mut rows = left.rows;
            rows.extend(right.rows);
            deduplicate_projected_rows(rows)
        }
        ast::SetOpKind::UnionAll => {
            let mut rows = left.rows;
            rows.extend(right.rows);
            rows
        }
        ast::SetOpKind::Intersect => {
            let left_set: std::collections::HashSet<_> = left.rows.iter().cloned().collect();
            right
                .rows
                .into_iter()
                .filter(|r| left_set.contains(r))
                .collect()
        }
        ast::SetOpKind::Except => {
            let right_set: std::collections::HashSet<_> = right.rows.iter().cloned().collect();
            left.rows
                .into_iter()
                .filter(|r| !right_set.contains(r))
                .collect()
        }
    };

    Ok(QueryResult {
        columns: left.columns,
        column_types: left.column_types,
        rows,
        ..Default::default()
    })
}

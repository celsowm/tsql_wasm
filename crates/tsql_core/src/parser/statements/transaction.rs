use crate::ast::{IsolationLevel, Statement};
use crate::error::DbError;

pub(crate) fn parse_begin_transaction(sql: &str) -> Result<Statement, DbError> {
    let upper = sql.to_uppercase();
    let rest = if upper.starts_with("BEGIN TRANSACTION") {
        sql["BEGIN TRANSACTION".len()..].trim()
    } else if upper.starts_with("BEGIN TRAN") {
        sql["BEGIN TRAN".len()..].trim()
    } else {
        return Err(DbError::Parse("invalid BEGIN TRANSACTION syntax".into()));
    };
    // strip WITH MARK 'description' if present
    let upper_rest = rest.to_uppercase();
    let name_part = if let Some(wm_idx) = upper_rest.find("WITH MARK") {
        rest[..wm_idx].trim()
    } else {
        rest
    };
    let name = if name_part.is_empty() {
        None
    } else {
        Some(name_part.to_string())
    };
    Ok(Statement::BeginTransaction(name))
}

pub(crate) fn parse_commit_transaction(sql: &str) -> Result<Statement, DbError> {
    let upper = sql.to_uppercase();
    let rest = if upper.starts_with("COMMIT TRANSACTION") {
        sql["COMMIT TRANSACTION".len()..].trim()
    } else if upper.starts_with("COMMIT TRAN") {
        sql["COMMIT TRAN".len()..].trim()
    } else if upper == "COMMIT" {
        ""
    } else {
        return Err(DbError::Parse("invalid COMMIT syntax".into()));
    };
    let name = if rest.is_empty() {
        None
    } else {
        Some(rest.to_string())
    };
    Ok(Statement::CommitTransaction(name))
}

pub(crate) fn parse_rollback_transaction(sql: &str) -> Result<Statement, DbError> {
    let upper = sql.to_uppercase();
    if upper == "ROLLBACK" || upper == "ROLLBACK TRAN" || upper == "ROLLBACK TRANSACTION" {
        return Ok(Statement::RollbackTransaction(None));
    }

    let savepoint = if upper.starts_with("ROLLBACK TRANSACTION ") {
        Some(sql["ROLLBACK TRANSACTION".len()..].trim().to_string())
    } else if upper.starts_with("ROLLBACK TRAN ") {
        Some(sql["ROLLBACK TRAN".len()..].trim().to_string())
    } else {
        None
    };

    if let Some(name) = savepoint {
        if name.is_empty() {
            return Err(DbError::Parse("ROLLBACK TRANSACTION name is empty".into()));
        }
        Ok(Statement::RollbackTransaction(Some(name)))
    } else {
        Err(DbError::Parse("invalid ROLLBACK syntax".into()))
    }
}

pub(crate) fn parse_save_transaction(sql: &str) -> Result<Statement, DbError> {
    let upper = sql.to_uppercase();
    let rest = if upper.starts_with("SAVE TRANSACTION ") {
        sql["SAVE TRANSACTION".len()..].trim()
    } else if upper.starts_with("SAVE TRAN ") {
        sql["SAVE TRAN".len()..].trim()
    } else {
        return Err(DbError::Parse("invalid SAVE TRANSACTION syntax".into()));
    };
    if rest.is_empty() {
        return Err(DbError::Parse(
            "SAVE TRANSACTION requires a savepoint name".into(),
        ));
    }
    Ok(Statement::SaveTransaction(rest.to_string()))
}

pub(crate) fn parse_set_transaction_isolation(sql: &str) -> Result<Statement, DbError> {
    let prefix = "SET TRANSACTION ISOLATION LEVEL";
    if !sql.to_uppercase().starts_with(prefix) {
        return Err(DbError::Parse(
            "invalid SET TRANSACTION ISOLATION LEVEL syntax".into(),
        ));
    }
    let level_raw = sql[prefix.len()..].trim().to_uppercase();
    let level = match level_raw.as_str() {
        "READ UNCOMMITTED" => IsolationLevel::ReadUncommitted,
        "READ COMMITTED" => IsolationLevel::ReadCommitted,
        "REPEATABLE READ" => IsolationLevel::RepeatableRead,
        "SERIALIZABLE" => IsolationLevel::Serializable,
        "SNAPSHOT" => IsolationLevel::Snapshot,
        _ => {
            return Err(DbError::Parse(format!(
                "unsupported isolation level '{}'",
                level_raw
            )))
        }
    };
    Ok(Statement::SetTransactionIsolationLevel(level))
}

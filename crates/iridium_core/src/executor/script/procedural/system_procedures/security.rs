use crate::error::DbError;
use crate::executor::context::ExecutionContext;
use crate::executor::result::QueryResult;
use crate::executor::script::ScriptExecutor;

pub(crate) fn execute_sp_helpuser(
    exec: &mut ScriptExecutor<'_>,
    ctx: &mut ExecutionContext<'_>,
) -> Result<QueryResult, DbError> {
    let sql = "SELECT p.name AS UserName, p.type_desc AS RoleName, '' AS LoginName, '' AS DefDBName, '' AS DefSchemaName, p.principal_id AS UserId, p.principal_id AS SID FROM sys.database_principals p";
    let batch = crate::parser::parse_batch(sql)?;
    match exec.execute_batch(&batch, ctx)? {
        crate::error::StmtOutcome::Ok(Some(res)) => Ok(res),
        _ => Err(DbError::Execution("Failed to execute sp_helpuser query".into())),
    }
}

pub(crate) fn execute_sp_helprole(
    exec: &mut ScriptExecutor<'_>,
    ctx: &mut ExecutionContext<'_>,
) -> Result<QueryResult, DbError> {
    let sql = "SELECT name AS RoleName, principal_id AS RoleId, 0 AS IsAppRole FROM sys.database_principals WHERE type = 'R'";
    let batch = crate::parser::parse_batch(sql)?;
    match exec.execute_batch(&batch, ctx)? {
        crate::error::StmtOutcome::Ok(Some(res)) => Ok(res),
        _ => Err(DbError::Execution("Failed to execute sp_helprole query".into())),
    }
}

pub(crate) fn execute_sp_helprolemember(
    exec: &mut ScriptExecutor<'_>,
    ctx: &mut ExecutionContext<'_>,
) -> Result<QueryResult, DbError> {
    let sql = "SELECT r.name AS DbRole, m.name AS MemberName, m.principal_id AS MemberSID FROM sys.database_role_members rm JOIN sys.database_principals r ON rm.role_principal_id = r.principal_id JOIN sys.database_principals m ON rm.member_principal_id = m.principal_id";
    let batch = crate::parser::parse_batch(sql)?;
    match exec.execute_batch(&batch, ctx)? {
        crate::error::StmtOutcome::Ok(Some(res)) => Ok(res),
        _ => Err(DbError::Execution("Failed to execute sp_helprolemember query".into())),
    }
}

pub(crate) fn execute_sp_helpsrvrole(
    exec: &mut ScriptExecutor<'_>,
    ctx: &mut ExecutionContext<'_>,
) -> Result<QueryResult, DbError> {
    let sql = "SELECT name AS ServerRole, principal_id AS RoleId FROM sys.server_principals WHERE type = 'R'";
    let batch = crate::parser::parse_batch(sql)?;
    match exec.execute_batch(&batch, ctx)? {
        crate::error::StmtOutcome::Ok(Some(res)) => Ok(res),
        _ => Err(DbError::Execution("Failed to execute sp_helpsrvrole query".into())),
    }
}

pub(crate) fn execute_sp_helpsrvrolemember(
    exec: &mut ScriptExecutor<'_>,
    ctx: &mut ExecutionContext<'_>,
) -> Result<QueryResult, DbError> {
    let sql = "SELECT r.name AS ServerRole, m.name AS MemberName, m.principal_id AS MemberSID FROM sys.server_role_members srm JOIN sys.server_principals r ON srm.role_principal_id = r.principal_id JOIN sys.server_principals m ON srm.member_principal_id = m.principal_id";
    let batch = crate::parser::parse_batch(sql)?;
    match exec.execute_batch(&batch, ctx)? {
        crate::error::StmtOutcome::Ok(Some(res)) => Ok(res),
        _ => Err(DbError::Execution("Failed to execute sp_helpsrvrolemember query".into())),
    }
}

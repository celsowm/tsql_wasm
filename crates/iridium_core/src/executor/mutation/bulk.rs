use super::super::context::ExecutionContext;
use super::super::result::QueryResult;
use super::MutationExecutor;
use crate::ast::BulkInsertStmt;
use crate::ast::InsertBulkStmt;
use crate::error::DbError;

impl<'a> MutationExecutor<'a> {
    pub(crate) fn execute_bulk_insert(
        &mut self,
        _stmt: BulkInsertStmt,
        _ctx: &mut ExecutionContext<'_>,
    ) -> Result<Option<QueryResult>, DbError> {
        // BULK INSERT (loading from a file on the server) is currently a shim.
        // It requires file system access which is restricted in some environments.
        Err(DbError::Execution("BULK INSERT from server-side file is not yet implemented. Use client-side SqlBulkCopy/INSERT BULK instead.".into()))
    }

    pub(crate) fn execute_insert_bulk(
        &mut self,
        stmt: InsertBulkStmt,
        ctx: &mut ExecutionContext<'_>,
    ) -> Result<Option<QueryResult>, DbError> {
        let schema = stmt.table.schema_or_dbo();
        let table_name = &stmt.table.name;

        let _table = self
            .catalog
            .find_table(schema, table_name)
            .ok_or_else(|| DbError::table_not_found(schema, table_name))?;

        // This statement is usually the first part of a TDS Bulk Load operation.
        // The server needs to respond with a DONE token to signal it's ready for 0x07 packets.
        // We set a flag in the context to indicate that the next packet should be 0x07.
        ctx.set_bulk_load_active(true, stmt.table.clone(), stmt.columns.clone());

        Ok(None)
    }
}

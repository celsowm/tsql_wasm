use tokio::io::AsyncWriteExt;

use super::TdsSession;

impl TdsSession {
    pub(crate) async fn handle_sql_batch<W: AsyncWriteExt + Unpin>(
        &mut self,
        data: &[u8],
        writer: &mut W,
    ) -> Result<bool, iridium_core::error::DbError> {
        crate::session::sql_pipeline::handle_sql_batch(self, data, writer).await
    }

    pub(crate) async fn execute_sql<W: AsyncWriteExt + Unpin>(
        &mut self,
        sql: &str,
        writer: &mut W,
    ) -> Result<bool, iridium_core::error::DbError> {
        crate::session::sql_pipeline::execute_sql(self, sql, writer).await
    }
}

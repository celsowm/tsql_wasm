use super::super::types::RpcParam;

#[derive(Debug, Clone)]
pub(crate) enum RpcProcSelector {
    Id(u16),
    Name(String),
}

#[derive(Debug, Clone)]
pub enum RpcRequest {
    Sql(SqlRpcRequest),
    Cursor(CursorRpcRequest),
    Prepare(PrepareRpcRequest),
    Execute(ExecuteRpcRequest),
    Unprepare(UnprepareRpcRequest),
    PrepExec(PrepExecRpcRequest),
    ResetConnection,
    Catalog(CatalogRpcRequest),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogProc {
    Tables,
    Columns,
    SprocColumns,
    PrimaryKeys,
    DescribeCursor,
}

#[derive(Debug, Clone)]
pub struct CatalogRpcRequest {
    pub proc: CatalogProc,
    pub params: Vec<RpcParam>,
}

#[derive(Debug, Clone)]
pub struct SqlRpcRequest {
    pub sql: String,
    pub params: Vec<RpcParam>,
}

#[derive(Debug, Clone)]
pub struct PrepareRpcRequest {
    pub stmt_handle: Option<i32>,
    pub sql: String,
    pub param_decl: String,
    pub params: Vec<RpcParam>,
}

#[derive(Debug, Clone)]
pub struct ExecuteRpcRequest {
    pub stmt_handle: i32,
    pub params: Vec<RpcParam>,
}

#[derive(Debug, Clone)]
pub struct UnprepareRpcRequest {
    pub stmt_handle: i32,
}

#[derive(Debug, Clone)]
pub struct PrepExecRpcRequest {
    pub stmt_handle: Option<i32>,
    pub sql: String,
    pub param_decl: String,
    pub params: Vec<RpcParam>,
}

#[derive(Debug, Clone)]
pub struct CursorRpcRequest {
    pub cursor_op: CursorOp,
    pub cursor_handle: Option<i32>,
    pub scroll_opt: Option<i32>,
    pub cc_opt: Option<i32>,
    pub row_count: Option<i32>,
    pub sql: Option<String>,
    pub param_def: Option<String>,
    pub params: Vec<RpcParam>,
    pub fetch_type: Option<i32>,
    pub row_num: Option<i32>,
    pub n_rows: Option<i32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorOp {
    Open,
    Fetch,
    Close,
    Prepare,
    Execute,
    PrepExec,
    Unprepare,
    Option,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcProc {
    CursorOpen,
    CursorClose,
    CursorFetch,
    CursorPrepare,
    CursorExecute,
    CursorPrepExec,
    CursorUnprepare,
    CursorOption,
    ExecuteSql,
    PrepExec,
    PrepExecRpc,
    Prepare,
    Execute,
    Unprepare,
    ResetConnection,
    SpTables,
    SpColumns,
    SpSprocColumns,
    SpPkeys,
    SpDescribeCursor,
}

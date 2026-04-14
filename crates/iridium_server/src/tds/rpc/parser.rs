use crate::tds::packet::PacketReader;
use std::io;

use super::decode;
use super::types::RpcParam;

#[derive(Debug, Clone)]
enum RpcProcSelector {
    Id(u16),
    Name(String),
}

struct RpcFrameParser<'a> {
    reader: PacketReader<'a>,
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

impl RpcProc {
    fn from_id(id: u16) -> Option<Self> {
        match id {
            1 => Some(RpcProc::CursorOpen),      // sp_cursor
            2 => Some(RpcProc::CursorOpen),      // sp_cursoropen
            3 => Some(RpcProc::CursorPrepare),   // sp_cursorprepare (MS-TDS)
            4 => Some(RpcProc::CursorExecute),   // sp_cursorexecute (MS-TDS)
            5 => Some(RpcProc::CursorPrepExec),  // sp_cursorprepexec (MS-TDS)
            6 => Some(RpcProc::CursorUnprepare), // sp_cursorunprepare (MS-TDS)
            7 => Some(RpcProc::CursorFetch),     // sp_cursorfetch
            8 => Some(RpcProc::CursorOption),    // sp_cursoroption (MS-TDS)
            9 => Some(RpcProc::CursorClose),     // sp_cursorclose (MS-TDS)
            10 => Some(RpcProc::ExecuteSql),     // sp_executesql
            11 => Some(RpcProc::Prepare),        // sp_prepare (MS-TDS)
            12 => Some(RpcProc::Execute),        // sp_execute (MS-TDS)
            13 => Some(RpcProc::PrepExec),       // sp_prepexec
            14 => Some(RpcProc::PrepExecRpc),    // sp_prepexecrpc (MS-TDS)
            15 => Some(RpcProc::Unprepare),      // sp_unprepare (MS-TDS)
            _ => None,
        }
    }

    fn from_name(name: &str) -> Option<Self> {
        let n = normalize_proc_name(name);
        match n.as_str() {
            "sp_cursor" | "sp_cursoropen" => Some(RpcProc::CursorOpen),
            "sp_cursorclose" => Some(RpcProc::CursorClose),
            "sp_cursorfetch" => Some(RpcProc::CursorFetch),
            "sp_cursorprepare" => Some(RpcProc::CursorPrepare),
            "sp_cursorexecute" => Some(RpcProc::CursorExecute),
            "sp_cursorprepexec" => Some(RpcProc::CursorPrepExec),
            "sp_cursorunprepare" => Some(RpcProc::CursorUnprepare),
            "sp_cursoroption" => Some(RpcProc::CursorOption),
            "sp_executesql" => Some(RpcProc::ExecuteSql),
            "sp_prepexec" => Some(RpcProc::PrepExec),
            "sp_prepexecrpc" => Some(RpcProc::PrepExecRpc),
            "sp_prepare" => Some(RpcProc::Prepare),
            "sp_execute" => Some(RpcProc::Execute),
            "sp_unprepare" => Some(RpcProc::Unprepare),
            "sp_reset_connection" => Some(RpcProc::ResetConnection),
            "sp_tables" => Some(RpcProc::SpTables),
            "sp_columns" => Some(RpcProc::SpColumns),
            "sp_sproc_columns" => Some(RpcProc::SpSprocColumns),
            "sp_pkeys" => Some(RpcProc::SpPkeys),
            "sp_describe_cursor" => Some(RpcProc::SpDescribeCursor),
            _ => None,
        }
    }

    pub fn is_cursor(&self) -> bool {
        matches!(
            self,
            RpcProc::CursorOpen
                | RpcProc::CursorClose
                | RpcProc::CursorFetch
                | RpcProc::CursorPrepare
                | RpcProc::CursorExecute
                | RpcProc::CursorPrepExec
                | RpcProc::CursorUnprepare
                | RpcProc::CursorOption
        )
    }
}

impl CursorOp {
    #[allow(dead_code)]
    fn from_id(_id: u16) -> Option<Self> {
        None
    }

    #[allow(dead_code)]
    fn from_name(name: &str) -> Option<Self> {
        let n = normalize_proc_name(name);
        match n.as_str() {
            "sp_cursor" | "sp_cursoropen" => Some(CursorOp::Open),
            "sp_cursorclose" => Some(CursorOp::Close),
            "sp_cursorfetch" => Some(CursorOp::Fetch),
            "sp_cursorprepare" => Some(CursorOp::Prepare),
            "sp_cursorexecute" => Some(CursorOp::Execute),
            "sp_cursorprepexec" => Some(CursorOp::PrepExec),
            "sp_cursorunprepare" => Some(CursorOp::Unprepare),
            "sp_cursoroption" => Some(CursorOp::Option),
            _ => None,
        }
    }
}

/// Parse an RPC request packet (sp_executesql=10, sp_prepexec=13, cursor procedures).
/// Returns None if not a supported RPC call.
pub fn parse_rpc(data: &[u8]) -> io::Result<Option<RpcRequest>> {
    RpcFrameParser::new(data).parse()
}

impl<'a> RpcFrameParser<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self {
            reader: PacketReader::new(data),
        }
    }

    fn parse(mut self) -> io::Result<Option<RpcRequest>> {
        self.skip_all_headers()?;
        let proc_selector = self.read_proc_selector()?;
        self.skip_rpc_flags()?;

        let proc = match &proc_selector {
            RpcProcSelector::Id(id) => RpcProc::from_id(*id),
            RpcProcSelector::Name(name) => RpcProc::from_name(name),
        };

        let proc = match proc {
            Some(p) => p,
            None => {
                log::warn!("Unsupported RPC procedure selector: {:?}", proc_selector);
                return Ok(None);
            }
        };

        if proc.is_cursor() {
            let cursor_op = match proc {
                RpcProc::CursorOpen => CursorOp::Open,
                RpcProc::CursorClose => CursorOp::Close,
                RpcProc::CursorFetch => CursorOp::Fetch,
                RpcProc::CursorPrepare => CursorOp::Prepare,
                RpcProc::CursorExecute => CursorOp::Execute,
                RpcProc::CursorPrepExec => CursorOp::PrepExec,
                RpcProc::CursorUnprepare => CursorOp::Unprepare,
                RpcProc::CursorOption => CursorOp::Option,
                _ => unreachable!(),
            };
            return self.parse_cursor_rpc(cursor_op);
        }

        match proc {
            RpcProc::ExecuteSql => {
                let sql = match self.read_rpc_nvarchar_param()? {
                    Some(s) => s,
                    None => return Ok(None),
                };
                let param_decl = self.read_rpc_nvarchar_param()?.unwrap_or_default();
                let decl_parts = parse_param_decl(&param_decl);
                let params = self.read_rpc_params(&decl_parts)?;
                Ok(Some(RpcRequest::Sql(SqlRpcRequest { sql, params })))
            }
            RpcProc::Prepare => self.parse_prepare_rpc(),
            RpcProc::Execute => self.parse_execute_rpc(),
            RpcProc::Unprepare => self.parse_unprepare_rpc(),
            RpcProc::PrepExec | RpcProc::PrepExecRpc => self.parse_prepexec_rpc(),
            RpcProc::ResetConnection => Ok(Some(RpcRequest::ResetConnection)),
            RpcProc::SpTables => self.parse_catalog_rpc(CatalogProc::Tables),
            RpcProc::SpColumns => self.parse_catalog_rpc(CatalogProc::Columns),
            RpcProc::SpSprocColumns => self.parse_catalog_rpc(CatalogProc::SprocColumns),
            RpcProc::SpPkeys => self.parse_catalog_rpc(CatalogProc::PrimaryKeys),
            RpcProc::SpDescribeCursor => self.parse_catalog_rpc(CatalogProc::DescribeCursor),
            _ => Ok(None),
        }
    }

    fn parse_catalog_rpc(&mut self, catalog_proc: CatalogProc) -> io::Result<Option<RpcRequest>> {
        let mut params: Vec<RpcParam> = vec![];
        let _idx: usize = 0;

        while self.reader.remaining() > 0 {
            let name_len = match self.reader.read_u8() {
                Ok(n) => n as usize,
                Err(_) => break,
            };
            if name_len > 0 && self.reader.remaining() >= name_len * 2 {
                let _ = self.reader.read_bytes(name_len * 2)?;
            }
            if self.reader.remaining() < 2 {
                break;
            }
            let _status = self.reader.read_u8()?;
            let type_id = self.reader.read_u8()?;

            let decoded = decode::read_typed_value(&mut self.reader, type_id)?;
            params.push(RpcParam {
                name: String::new(),
                type_name: decoded.type_name,
                value_sql: decoded.value_sql,
                tvp_rows: decoded.tvp_rows,
            });
        }

        Ok(Some(RpcRequest::Catalog(CatalogRpcRequest {
            proc: catalog_proc,
            params,
        })))
    }

    fn parse_prepare_rpc(&mut self) -> io::Result<Option<RpcRequest>> {
        let mut stmt_handle: Option<i32> = None;
        let mut sql: Option<String> = None;
        let mut param_decl: Option<String> = None;
        let mut params: Vec<RpcParam> = vec![];
        let mut param_idx: usize = 0;

        while self.reader.remaining() > 0 {
            let name_len = match self.reader.read_u8() {
                Ok(n) => n as usize,
                Err(_) => break,
            };
            if name_len > 0 && self.reader.remaining() >= name_len * 2 {
                let _ = self.reader.read_bytes(name_len * 2)?;
            }
            if self.reader.remaining() < 2 {
                break;
            }
            let _status = self.reader.read_u8()?;
            let type_id = self.reader.read_u8()?;

            match param_idx {
                0 => stmt_handle = self.read_int_param(type_id)?,
                1 => sql = self.read_rpc_nvarchar_param()?,
                2 => param_decl = self.read_rpc_nvarchar_param()?,
                _ => {
                    if sql.is_some() && param_decl.is_some() {
                        let decl_parts = parse_param_decl(param_decl.as_ref().unwrap());
                        params = self.read_rpc_params(&decl_parts)?;
                    } else {
                        self.skip_typed_value(type_id)?;
                    }
                }
            }
            param_idx += 1;
        }

        Ok(Some(RpcRequest::Prepare(PrepareRpcRequest {
            stmt_handle,
            sql: sql.unwrap_or_default(),
            param_decl: param_decl.unwrap_or_default(),
            params,
        })))
    }

    fn parse_execute_rpc(&mut self) -> io::Result<Option<RpcRequest>> {
        let mut stmt_handle: Option<i32> = None;
        let mut params: Vec<RpcParam> = vec![];
        let mut param_idx: usize = 0;

        while self.reader.remaining() > 0 {
            let name_len = match self.reader.read_u8() {
                Ok(n) => n as usize,
                Err(_) => break,
            };
            if name_len > 0 && self.reader.remaining() >= name_len * 2 {
                let _ = self.reader.read_bytes(name_len * 2)?;
            }
            if self.reader.remaining() < 2 {
                break;
            }
            let _status = self.reader.read_u8()?;
            let type_id = self.reader.read_u8()?;

            match param_idx {
                0 => stmt_handle = self.read_int_param(type_id)?,
                _ => self.skip_typed_value(type_id)?,
            }
            param_idx += 1;
        }

        Ok(Some(RpcRequest::Execute(ExecuteRpcRequest {
            stmt_handle: stmt_handle.unwrap_or(0),
            params,
        })))
    }

    fn parse_unprepare_rpc(&mut self) -> io::Result<Option<RpcRequest>> {
        let mut stmt_handle: Option<i32> = None;
        let mut param_idx: usize = 0;

        while self.reader.remaining() > 0 {
            let name_len = match self.reader.read_u8() {
                Ok(n) => n as usize,
                Err(_) => break,
            };
            if name_len > 0 && self.reader.remaining() >= name_len * 2 {
                let _ = self.reader.read_bytes(name_len * 2)?;
            }
            if self.reader.remaining() < 2 {
                break;
            }
            let _status = self.reader.read_u8()?;
            let type_id = self.reader.read_u8()?;

            if param_idx == 0 {
                stmt_handle = self.read_int_param(type_id)?;
            } else {
                self.skip_typed_value(type_id)?;
            }
            param_idx += 1;
        }

        Ok(Some(RpcRequest::Unprepare(UnprepareRpcRequest {
            stmt_handle: stmt_handle.unwrap_or(0),
        })))
    }

    fn parse_prepexec_rpc(&mut self) -> io::Result<Option<RpcRequest>> {
        let mut stmt_handle: Option<i32> = None;
        let mut sql: Option<String> = None;
        let mut param_decl: Option<String> = None;
        let mut params: Vec<RpcParam> = vec![];
        let mut param_idx: usize = 0;

        while self.reader.remaining() > 0 {
            let name_len = match self.reader.read_u8() {
                Ok(n) => n as usize,
                Err(_) => break,
            };
            if name_len > 0 && self.reader.remaining() >= name_len * 2 {
                let _ = self.reader.read_bytes(name_len * 2)?;
            }
            if self.reader.remaining() < 2 {
                break;
            }
            let _status = self.reader.read_u8()?;
            let type_id = self.reader.read_u8()?;

            match param_idx {
                0 => stmt_handle = self.read_int_param(type_id)?,
                1 => sql = self.read_rpc_nvarchar_param()?,
                2 => param_decl = self.read_rpc_nvarchar_param()?,
                _ => {
                    if sql.is_some() && param_decl.is_some() {
                        let decl_parts = parse_param_decl(param_decl.as_ref().unwrap());
                        params = self.read_rpc_params(&decl_parts)?;
                    } else {
                        self.skip_typed_value(type_id)?;
                    }
                }
            }
            param_idx += 1;
        }

        Ok(Some(RpcRequest::PrepExec(PrepExecRpcRequest {
            stmt_handle,
            sql: sql.unwrap_or_default(),
            param_decl: param_decl.unwrap_or_default(),
            params,
        })))
    }

    fn parse_cursor_rpc(&mut self, cursor_op: CursorOp) -> io::Result<Option<RpcRequest>> {
        let mut cursor_handle: Option<i32> = None;
        let mut scroll_opt: Option<i32> = None;
        let mut cc_opt: Option<i32> = None;
        let mut row_count: Option<i32> = None;
        let mut sql: Option<String> = None;
        let mut param_def: Option<String> = None;
        let mut params: Vec<RpcParam> = vec![];
        let mut fetch_type: Option<i32> = None;
        let mut row_num: Option<i32> = None;
        let mut n_rows: Option<i32> = None;
        let mut param_idx: usize = 0;

        while self.reader.remaining() > 0 {
            let name_len = match self.reader.read_u8() {
                Ok(n) => n as usize,
                Err(_) => break,
            };
            if name_len > 0 && self.reader.remaining() >= name_len * 2 {
                let _ = self.reader.read_bytes(name_len * 2)?;
            }
            if self.reader.remaining() < 2 {
                break;
            }
            let _status = self.reader.read_u8()?;
            let type_id = self.reader.read_u8()?;

            match cursor_op {
                CursorOp::Open => match param_idx {
                    0 => cursor_handle = self.read_int_param(type_id)?,
                    1 => sql = self.read_rpc_nvarchar_param()?,
                    2 => scroll_opt = self.read_int_param(type_id)?,
                    3 => cc_opt = self.read_int_param(type_id)?,
                    4 => row_count = self.read_int_param(type_id)?,
                    5 => {
                        param_def = self.read_rpc_nvarchar_param()?;
                        if let Some(ref pd) = param_def {
                            let decl_parts = parse_param_decl(pd);
                            params = self.read_rpc_params(&decl_parts)?;
                        }
                    }
                    _ => self.skip_typed_value(type_id)?,
                },
                CursorOp::Fetch => match param_idx {
                    0 => cursor_handle = self.read_int_param(type_id)?,
                    1 => fetch_type = self.read_int_param(type_id)?,
                    2 => row_num = self.read_int_param(type_id)?,
                    3 => n_rows = self.read_int_param(type_id)?,
                    _ => self.skip_typed_value(type_id)?,
                },
                CursorOp::Close | CursorOp::Unprepare => {
                    if param_idx == 0 {
                        cursor_handle = self.read_int_param(type_id)?;
                    } else {
                        self.skip_typed_value(type_id)?;
                    }
                }
                CursorOp::Prepare => match param_idx {
                    0 => {}
                    1 => sql = self.read_rpc_nvarchar_param()?,
                    2 => scroll_opt = self.read_int_param(type_id)?,
                    3 => cc_opt = self.read_int_param(type_id)?,
                    4 => param_def = self.read_rpc_nvarchar_param()?,
                    _ => self.skip_typed_value(type_id)?,
                },
                CursorOp::Execute => match param_idx {
                    0 => cursor_handle = self.read_int_param(type_id)?,
                    1 => row_count = self.read_int_param(type_id)?,
                    _ => self.skip_typed_value(type_id)?,
                },
                CursorOp::PrepExec => match param_idx {
                    0 => {}
                    1 => cursor_handle = self.read_int_param(type_id)?,
                    2 => sql = self.read_rpc_nvarchar_param()?,
                    3 => scroll_opt = self.read_int_param(type_id)?,
                    4 => cc_opt = self.read_int_param(type_id)?,
                    5 => {
                        param_def = self.read_rpc_nvarchar_param()?;
                        if let Some(ref pd) = param_def {
                            let decl_parts = parse_param_decl(pd);
                            params = self.read_rpc_params(&decl_parts)?;
                        }
                    }
                    _ => self.skip_typed_value(type_id)?,
                },
                CursorOp::Option => match param_idx {
                    0 => cursor_handle = self.read_int_param(type_id)?,
                    1 => row_num = self.read_int_param(type_id)?,
                    _ => self.skip_typed_value(type_id)?,
                },
            }
            param_idx += 1;
        }

        Ok(Some(RpcRequest::Cursor(CursorRpcRequest {
            cursor_op,
            cursor_handle,
            scroll_opt,
            cc_opt,
            row_count,
            sql,
            param_def,
            params,
            fetch_type,
            row_num,
            n_rows,
        })))
    }

    fn read_int_param(&mut self, type_id: u8) -> io::Result<Option<i32>> {
        match type_id {
            0x26 => {
                // INTN (4 bytes) - reads as i32 from little-endian u32
                if self.reader.remaining() < 4 {
                    return Ok(None);
                }
                let val = self.reader.read_u32_le()?;
                let len = self.reader.read_u8()?;
                if len == 0 {
                    Ok(None)
                } else {
                    Ok(Some(val as i32))
                }
            }
            0x38 => {
                // BIGINT - reads as i32 from little-endian u64
                if self.reader.remaining() < 8 {
                    return Ok(None);
                }
                let val = self.reader.read_u64_le()?;
                Ok(Some(val as i32))
            }
            _ => {
                self.skip_typed_value(type_id)?;
                Ok(None)
            }
        }
    }

    fn skip_typed_value(&mut self, type_id: u8) -> io::Result<()> {
        match type_id {
            0x1F => {
                // NULL
                Ok(())
            }
            0x26 => {
                // INTN
                if self.reader.remaining() >= 5 {
                    self.reader.skip(5)?;
                }
                Ok(())
            }
            0x38 => {
                // BIGINTN
                if self.reader.remaining() >= 9 {
                    self.reader.skip(9)?;
                }
                Ok(())
            }
            0x6A | 0x68 => {
                // INT | SMALLINT
                if self.reader.remaining() >= 1 {
                    self.reader.skip(1)?;
                }
                Ok(())
            }
            _ => {
                // Skip based on type family
                let skip_chars = match type_id {
                    0xE7 | 0xA7 => 9,
                    0x22 | 0x21 => 5,
                    0x7A | 0x7C => 1,
                    _ => 3,
                };
                let to_skip = self.reader.remaining().min(skip_chars);
                self.reader.skip(to_skip)?;
                Ok(())
            }
        }
    }

    fn skip_all_headers(&mut self) -> io::Result<()> {
        if self.reader.remaining() < 4 {
            return Ok(());
        }
        let total_header_len = self.reader.read_u32_le()? as usize;
        if total_header_len > 4 {
            let skip = total_header_len - 4;
            if self.reader.remaining() < skip {
                return Ok(());
            }
            self.reader.skip(skip)?;
        }
        Ok(())
    }

    fn read_proc_selector(&mut self) -> io::Result<RpcProcSelector> {
        if self.reader.remaining() < 2 {
            return Ok(RpcProcSelector::Id(0));
        }
        let name_len = self.reader.read_u16_le()?;
        if name_len == 0xFFFF {
            if self.reader.remaining() < 2 {
                return Ok(RpcProcSelector::Id(0));
            }
            return Ok(RpcProcSelector::Id(self.reader.read_u16_le()?));
        }
        let byte_len = name_len as usize * 2;
        if self.reader.remaining() < byte_len {
            return Ok(RpcProcSelector::Id(0));
        }
        let bytes = self.reader.read_bytes(byte_len)?;
        Ok(RpcProcSelector::Name(decode::decode_utf16le(bytes)))
    }

    fn skip_rpc_flags(&mut self) -> io::Result<()> {
        if self.reader.remaining() < 2 {
            return Ok(());
        }
        let _flags = self.reader.read_u16_le()?;
        Ok(())
    }

    fn read_rpc_params(&mut self, decl_parts: &[(String, String)]) -> io::Result<Vec<RpcParam>> {
        let mut params: Vec<RpcParam> = Vec::new();
        let mut idx = 0usize;

        while self.reader.remaining() > 0 {
            let Some(param_name) = self.read_param_name(idx, decl_parts)? else {
                break;
            };
            if self.reader.remaining() < 2 {
                break;
            }
            let _status = self.reader.read_u8()?;
            let type_id = self.reader.read_u8()?;

            let decoded = decode::read_typed_value(&mut self.reader, type_id)?;
            let final_type = if let Some((_, decl_type)) = decl_parts.get(idx) {
                decl_type.clone()
            } else {
                decoded.type_name
            };

            params.push(RpcParam {
                name: param_name,
                type_name: final_type,
                value_sql: decoded.value_sql,
                tvp_rows: decoded.tvp_rows,
            });
            idx += 1;
        }

        Ok(params)
    }

    fn read_param_name(
        &mut self,
        idx: usize,
        decl_parts: &[(String, String)],
    ) -> io::Result<Option<String>> {
        if self.reader.remaining() < 1 {
            return Ok(None);
        }
        let name_len = self.reader.read_u8()? as usize;
        if name_len == 0 {
            return Ok(Some(
                decl_parts
                    .get(idx)
                    .map(|(n, _)| n.clone())
                    .unwrap_or_default(),
            ));
        }
        if self.reader.remaining() < name_len * 2 {
            return Ok(None);
        }
        let bytes = self.reader.read_bytes(name_len * 2)?;
        Ok(Some(decode::decode_utf16le(bytes)))
    }

    fn read_rpc_nvarchar_param(&mut self) -> io::Result<Option<String>> {
        if self.reader.remaining() < 1 {
            return Ok(None);
        }
        let name_len = self.reader.read_u8()? as usize;
        if name_len > 0 {
            if self.reader.remaining() < name_len * 2 {
                return Ok(None);
            }
            self.reader.skip(name_len * 2)?;
        }
        if self.reader.remaining() < 2 {
            return Ok(None);
        }
        let _status = self.reader.read_u8()?;
        let type_id = self.reader.read_u8()?;

        match type_id {
            0xE7 | 0xA7 => {
                if self.reader.remaining() < 9 {
                    return Ok(None);
                }
                let _max_len = self.reader.read_u16_le()?;
                self.reader.skip(5)?;
                let actual_len = self.reader.read_u16_le()? as usize;
                if actual_len == 0xFFFF {
                    return Ok(Some(String::new()));
                }
                if self.reader.remaining() < actual_len {
                    return Ok(None);
                }
                let bytes = self.reader.read_bytes(actual_len)?;
                if type_id == 0xE7 {
                    Ok(Some(decode::decode_utf16le(bytes)))
                } else {
                    Ok(Some(String::from_utf8_lossy(bytes).to_string()))
                }
            }
            0x63 | 0x23 => {
                // NTEXT or TEXT
                if self.reader.remaining() < 13 {
                    return Ok(None);
                }
                let _max_len = self.reader.read_u32_le()?;
                self.reader.skip(5)?;
                let actual_len = self.reader.read_u32_le()? as usize;
                if self.reader.remaining() < actual_len {
                    return Ok(None);
                }
                let bytes = self.reader.read_bytes(actual_len)?;
                if type_id == 0x63 {
                    Ok(Some(decode::decode_utf16le(bytes)))
                } else {
                    Ok(Some(String::from_utf8_lossy(bytes).to_string()))
                }
            }
            0xF1 => {
                if self.reader.remaining() < 8 {
                    return Ok(None);
                }
                let _total = self.reader.read_u64_le()?;
                let mut result = Vec::new();
                loop {
                    if self.reader.remaining() < 4 {
                        break;
                    }
                    let chunk_len = self.reader.read_u32_le()? as usize;
                    if chunk_len == 0 {
                        break;
                    }
                    if self.reader.remaining() < chunk_len {
                        break;
                    }
                    result.extend_from_slice(self.reader.read_bytes(chunk_len)?);
                }
                Ok(Some(decode::decode_utf16le(&result)))
            }
            _ => Ok(Some(String::new())),
        }
    }
}

#[allow(dead_code)]
fn is_supported_rpc_proc(proc: &RpcProcSelector) -> bool {
    match proc {
        RpcProcSelector::Id(id) => *id == 10 || *id == 13,
        RpcProcSelector::Name(name) => {
            let base = normalize_proc_name(name);
            base == "sp_executesql"
                || base == "sp_prepexec"
                || base == "sp_prepare"
                || base == "sp_unprepare"
        }
    }
}

fn normalize_proc_name(name: &str) -> String {
    let mut part = name.trim();
    if let Some(last) = part.rsplit('.').next() {
        part = last;
    }
    part.trim_matches(|c| c == '[' || c == ']' || c == ' ')
        .to_ascii_lowercase()
}

pub fn parse_param_decl(decl: &str) -> Vec<(String, String)> {
    if decl.trim().is_empty() {
        return vec![];
    }
    decl.split(',')
        .filter_map(|part| {
            let part = part.trim();
            let mut iter = part.splitn(2, char::is_whitespace);
            let name = iter.next()?.trim().to_string();
            let type_name = iter.next()?.trim().to_string();
            if name.is_empty() || type_name.is_empty() {
                None
            } else {
                Some((name, type_name))
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rpc_proc_resolves_by_id() {
        // MS-TDS spec IDs (corrected)
        assert_eq!(RpcProc::from_id(1), Some(RpcProc::CursorOpen));
        assert_eq!(RpcProc::from_id(2), Some(RpcProc::CursorOpen));
        assert_eq!(RpcProc::from_id(3), Some(RpcProc::CursorPrepare)); // sp_cursorprepare
        assert_eq!(RpcProc::from_id(4), Some(RpcProc::CursorExecute)); // sp_cursorexecute
        assert_eq!(RpcProc::from_id(5), Some(RpcProc::CursorPrepExec)); // sp_cursorprepexec
        assert_eq!(RpcProc::from_id(6), Some(RpcProc::CursorUnprepare)); // sp_cursorunprepare
        assert_eq!(RpcProc::from_id(7), Some(RpcProc::CursorFetch));
        assert_eq!(RpcProc::from_id(8), Some(RpcProc::CursorOption)); // sp_cursoroption
        assert_eq!(RpcProc::from_id(9), Some(RpcProc::CursorClose)); // sp_cursorclose
        assert_eq!(RpcProc::from_id(10), Some(RpcProc::ExecuteSql));
        assert_eq!(RpcProc::from_id(11), Some(RpcProc::Prepare)); // sp_prepare
        assert_eq!(RpcProc::from_id(12), Some(RpcProc::Execute)); // sp_execute
        assert_eq!(RpcProc::from_id(13), Some(RpcProc::PrepExec)); // sp_prepexec
        assert_eq!(RpcProc::from_id(14), Some(RpcProc::PrepExecRpc)); // sp_prepexecrpc
        assert_eq!(RpcProc::from_id(15), Some(RpcProc::Unprepare)); // sp_unprepare
        assert_eq!(RpcProc::from_id(42), None);
    }

    #[test]
    fn rpc_proc_resolves_by_name() {
        assert_eq!(RpcProc::from_name("sp_cursor"), Some(RpcProc::CursorOpen));
        assert_eq!(
            RpcProc::from_name("sp_cursoropen"),
            Some(RpcProc::CursorOpen)
        );
        assert_eq!(
            RpcProc::from_name("sp_cursorclose"),
            Some(RpcProc::CursorClose)
        );
        assert_eq!(
            RpcProc::from_name("sp_cursorfetch"),
            Some(RpcProc::CursorFetch)
        );
        assert_eq!(
            RpcProc::from_name("[dbo].[sp_cursorprepare]"),
            Some(RpcProc::CursorPrepare)
        );
        assert_eq!(
            RpcProc::from_name("sp_cursorexecute"),
            Some(RpcProc::CursorExecute)
        );
        assert_eq!(
            RpcProc::from_name("sp_cursorunprepare"),
            Some(RpcProc::CursorUnprepare)
        );
        assert_eq!(
            RpcProc::from_name("sp_cursoroption"),
            Some(RpcProc::CursorOption)
        );
        assert_eq!(
            RpcProc::from_name("sp_executesql"),
            Some(RpcProc::ExecuteSql)
        );
        assert_eq!(RpcProc::from_name("sp_prepexec"), Some(RpcProc::PrepExec));
        assert_eq!(RpcProc::from_name("sp_prepare"), Some(RpcProc::Prepare));
        assert_eq!(RpcProc::from_name("sp_execute"), Some(RpcProc::Execute));
        assert_eq!(RpcProc::from_name("sp_unprepare"), Some(RpcProc::Unprepare));
        assert_eq!(
            RpcProc::from_name("sp_reset_connection"),
            Some(RpcProc::ResetConnection)
        );
        assert_eq!(RpcProc::from_name("sp_help"), None);
    }

    #[test]
    fn rpc_proc_is_cursor() {
        assert!(RpcProc::CursorOpen.is_cursor());
        assert!(RpcProc::CursorClose.is_cursor());
        assert!(RpcProc::CursorFetch.is_cursor());
        assert!(RpcProc::CursorPrepare.is_cursor());
        assert!(RpcProc::CursorExecute.is_cursor());
        assert!(RpcProc::CursorUnprepare.is_cursor());
        assert!(RpcProc::CursorOption.is_cursor());
        assert!(!RpcProc::ExecuteSql.is_cursor());
        assert!(!RpcProc::PrepExec.is_cursor());
        assert!(!RpcProc::Prepare.is_cursor());
        assert!(!RpcProc::Execute.is_cursor());
        assert!(!RpcProc::Unprepare.is_cursor());
        assert!(!RpcProc::ResetConnection.is_cursor());
    }

    #[test]
    fn param_decl_parsing() {
        let decls = parse_param_decl("@x INT, @name NVARCHAR(50)");
        assert_eq!(decls.len(), 2);
        assert_eq!(decls[0], ("@x".to_string(), "INT".to_string()));
        assert_eq!(decls[1], ("@name".to_string(), "NVARCHAR(50)".to_string()));

        let empty = parse_param_decl("");
        assert!(empty.is_empty());

        let single = parse_param_decl("@y INT");
        assert_eq!(single.len(), 1);
    }
}

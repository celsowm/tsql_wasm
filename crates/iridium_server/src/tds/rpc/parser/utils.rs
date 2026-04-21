use super::types::{RpcProc, CursorOp, RpcProcSelector};

impl RpcProc {
    pub(crate) fn from_id(id: u16) -> Option<Self> {
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

    pub(crate) fn from_name(name: &str) -> Option<Self> {
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
    pub(crate) fn from_id(_id: u16) -> Option<Self> {
        None
    }

    #[allow(dead_code)]
    pub(crate) fn from_name(name: &str) -> Option<Self> {
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

#[allow(dead_code)]
pub(crate) fn is_supported_rpc_proc(proc: &RpcProcSelector) -> bool {
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

pub(crate) fn normalize_proc_name(name: &str) -> String {
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

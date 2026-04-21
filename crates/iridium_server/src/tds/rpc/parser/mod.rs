pub mod types;
pub mod utils;
pub mod parser;

pub use types::*;
pub use utils::*;
pub use parser::*;

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

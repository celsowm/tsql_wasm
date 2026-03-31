mod decode;
mod parser;
mod render;
mod types;

pub use parser::parse_rpc;
pub use render::build_param_preamble;
pub use types::{RpcParam, RpcRequest};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_param_preamble_scalar_and_tvp() {
        let params = vec![
            RpcParam {
                name: "@x".to_string(),
                type_name: "INT".to_string(),
                value_sql: "42".to_string(),
                tvp_rows: None,
            },
            RpcParam {
                name: "@tvp".to_string(),
                type_name: "dbo.IntList READONLY".to_string(),
                value_sql: "NULL".to_string(),
                tvp_rows: Some(vec![vec!["1".to_string()], vec!["2".to_string()]]),
            },
        ];
        let sql = build_param_preamble(&params);
        assert!(sql.contains("DECLARE @x INT = 42;"));
        assert!(sql.contains("DECLARE @tvp dbo.IntList;"));
        assert!(sql.contains("INSERT INTO @tvp VALUES (1);"));
        assert!(sql.contains("INSERT INTO @tvp VALUES (2);"));
    }

    #[test]
    fn normalize_decl_type_strips_readonly() {
        assert_eq!(render::normalize_decl_type("dbo.T READONLY"), "dbo.T");
        assert_eq!(render::normalize_decl_type("dbo.T"), "dbo.T");
    }
}

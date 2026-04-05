pub mod common;
pub mod dml;
pub mod ddl;
pub mod procedural;

use crate::parser::ast;
use crate::ast as executor_ast;
use crate::error::DbError;

pub use common::*;
pub use dml::*;
pub use ddl::*;
pub use procedural::*;

pub fn lower_batch(parser_stmts: Vec<ast::Statement>) -> Result<Vec<executor_ast::Statement>, DbError> {
    parser_stmts.into_iter().map(lower_statement).collect()
}

pub fn lower_statement(parser_stmt: ast::Statement) -> Result<executor_ast::Statement, DbError> {
    match parser_stmt {
        ast::Statement::Dml(dml) => lower_dml(dml),
        ast::Statement::Ddl(ddl) => lower_ddl(ddl),
        ast::Statement::Procedural(proc) => lower_procedural(proc),
        ast::Statement::Transaction(txn) => lower_transaction(txn),
        ast::Statement::Cursor(cursor) => lower_cursor(cursor),
        ast::Statement::Session(session) => lower_session(session),
        ast::Statement::WithCte { ctes, body } => {
            let ctes: Result<Vec<_>, _> = ctes.into_iter().map(|cte| {
                let query = lower_statement(ast::Statement::Dml(ast::DmlStatement::Select(Box::new(cte.query))))?;
                Ok(executor_ast::statements::procedural::CteDef {
                    name: cte.name,
                    query,
                })
            }).collect();
            let ctes = ctes?;
            let body = Box::new(lower_statement(*body)?);
            Ok(executor_ast::Statement::WithCte(executor_ast::statements::procedural::WithCteStmt {
                recursive: false,
                ctes,
                body,
            }))
        }
    }
}

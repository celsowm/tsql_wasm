use crate::ast::statements::*;
use crate::error::StmtResult;
use crate::executor::result::QueryResult;

pub trait StatementVisitor<Context> {
    fn visit_statement(&mut self, stmt: Statement, ctx: &mut Context) -> StmtResult<Option<QueryResult>> {
        match stmt {
            Statement::Dml(s) => self.visit_dml(s, ctx),
            Statement::Ddl(s) => self.visit_ddl(s, ctx),
            Statement::Procedural(s) => self.visit_procedural(s, ctx),
            Statement::Transaction(s) => self.visit_transaction(s, ctx),
            Statement::Cursor(s) => self.visit_cursor(s, ctx),
            Statement::Session(s) => self.visit_session(s, ctx),
            Statement::WithCte(s) => self.visit_with_cte(s, ctx),
        }
    }

    fn visit_dml(&mut self, stmt: DmlStatement, ctx: &mut Context) -> StmtResult<Option<QueryResult>>;
    fn visit_ddl(&mut self, stmt: DdlStatement, ctx: &mut Context) -> StmtResult<Option<QueryResult>>;
    fn visit_procedural(&mut self, stmt: ProceduralStatement, ctx: &mut Context) -> StmtResult<Option<QueryResult>>;
    fn visit_transaction(&mut self, stmt: TransactionStatement, ctx: &mut Context) -> StmtResult<Option<QueryResult>>;
    fn visit_cursor(&mut self, stmt: CursorStatement, ctx: &mut Context) -> StmtResult<Option<QueryResult>>;
    fn visit_session(&mut self, stmt: SessionStatement, ctx: &mut Context) -> StmtResult<Option<QueryResult>>;
    fn visit_with_cte(&mut self, stmt: WithCteStmt, ctx: &mut Context) -> StmtResult<Option<QueryResult>>;
}

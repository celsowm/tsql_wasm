use crate::ast::statements::*;
use crate::error::StmtResult;
use crate::executor::result::QueryResult;

/// Visitor trait for traversing the T-SQL statement AST.
///
/// This follows an intentional **closed-AST** design: the set of statement variants is fixed by the
/// SQL grammar, and the exhaustive `match` in [`visit_statement`](Self::visit_statement) provides
/// **compile-time safety** — adding a new [`Statement`] variant will produce a compiler error in
/// the default implementation, forcing every implementor to handle the new case.
///
/// This is the correct extensibility mechanism for a SQL engine with a fixed grammar: the grammar
/// defines the closed set of node kinds, while new *behaviors* are added by implementing this trait.
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

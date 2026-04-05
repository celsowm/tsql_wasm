use crate::parser::ast::*;
use crate::parser::token::Keyword;
use crate::parser::state::Parser;
use crate::parser::error::{ParseResult, Expected};

pub fn parse_drop(parser: &mut Parser) -> ParseResult<Statement> {
    if parser.at_keyword(Keyword::Table) {
        let _ = parser.next();
        let table = super::parse_multipart_name(parser)?;
        Ok(Statement::Ddl(DdlStatement::DropTable(table)))
    } else if parser.at_keyword(Keyword::View) {
        let _ = parser.next();
        let name = super::parse_multipart_name(parser)?;
        Ok(Statement::Ddl(DdlStatement::DropView(name)))
    } else if matches!(parser.peek(), Some(Token::Keyword(kw)) if matches!(kw, Keyword::Procedure | Keyword::Proc)) {
        let _ = parser.next();
        let name = super::parse_multipart_name(parser)?;
        Ok(Statement::Ddl(DdlStatement::DropProcedure(name)))
    } else if parser.at_keyword(Keyword::Index) {
        let _ = parser.next();
        let name = super::parse_multipart_name(parser)?;
        parser.expect_keyword(Keyword::On)?;
        let table = super::parse_multipart_name(parser)?;
        Ok(Statement::Ddl(DdlStatement::DropIndex { name, table }))
    } else if parser.at_keyword(Keyword::Type) {
        let _ = parser.next();
        let name = super::parse_multipart_name(parser)?;
        Ok(Statement::Ddl(DdlStatement::DropType(name)))
    } else if parser.at_keyword(Keyword::Schema) {
        let _ = parser.next();
        let name = match parser.next() {
             Some(Token::Identifier(id)) => id.clone(),
             Some(Token::Keyword(k)) => k.as_ref().to_string(),
             _ => return parser.backtrack(Expected::Description("identifier or keyword")),
        };
        Ok(Statement::Ddl(DdlStatement::DropSchema(name)))
    } else if parser.at_keyword(Keyword::Function) {
        let _ = parser.next();
        let name = super::parse_multipart_name(parser)?;
        Ok(Statement::Ddl(DdlStatement::DropFunction(name)))
    } else if parser.at_keyword(Keyword::Trigger) {
        let _ = parser.next();
        let name = super::parse_multipart_name(parser)?;
        Ok(Statement::Ddl(DdlStatement::DropTrigger(name)))
    } else {
        parser.backtrack(Expected::Description("drop target"))
    }
}


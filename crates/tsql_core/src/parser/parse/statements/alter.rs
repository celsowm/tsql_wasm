use crate::parser::ast::*;
use crate::parser::token::Keyword;
use crate::parser::state::Parser;
use crate::parser::error::{ParseResult, Expected};

pub fn parse_alter(parser: &mut Parser) -> ParseResult<Statement> {
    if parser.at_keyword(Keyword::Table) {
        let _ = parser.next();
        let table = super::parse_multipart_name(parser)?;
        if parser.at_keyword(Keyword::Add) {
            let _ = parser.next();
            if parser.at_keyword(Keyword::Constraint) {
                let _ = parser.next();
                let constraint = super::ddl::parse_alter_table_add_constraint(parser)?;
                Ok(Statement::Ddl(DdlStatement::AlterTable { table, action: AlterTableAction::AddConstraint(constraint) }))
            } else {
                let col = super::ddl::parse_column_def(parser)?;
                Ok(Statement::Ddl(DdlStatement::AlterTable { table, action: AlterTableAction::AddColumn(col) }))
            }
        } else if parser.at_keyword(Keyword::Drop) {
            let _ = parser.next();
            if parser.at_keyword(Keyword::Column) {
                let _ = parser.next();
                let col_name = match parser.next() {
                    Some(Token::Identifier(id)) => id.clone(),
                    Some(Token::Keyword(k)) => k.as_ref().to_string(),
                    _ => return parser.backtrack(Expected::Description("column name")),
                };
                Ok(Statement::Ddl(DdlStatement::AlterTable { table, action: AlterTableAction::DropColumn(col_name) }))
            } else if parser.at_keyword(Keyword::Constraint) {
                let _ = parser.next();
                let constraint_name = match parser.next() {
                    Some(Token::Identifier(id)) => id.clone(),
                    Some(Token::Keyword(k)) => k.as_ref().to_string(),
                    _ => return parser.backtrack(Expected::Description("constraint name")),
                };
                Ok(Statement::Ddl(DdlStatement::AlterTable { table, action: AlterTableAction::DropConstraint(constraint_name) }))
            } else {
                parser.backtrack(Expected::Description("column or constraint"))
            }
        } else {
            parser.backtrack(Expected::Description("add or drop"))
        }
    } else {
        parser.backtrack(Expected::Description("table"))
    }
}


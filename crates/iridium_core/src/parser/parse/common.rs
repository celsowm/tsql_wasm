use crate::parser::ast::*;
use crate::parser::error::ParseResult;
use crate::parser::state::Parser;
use crate::parser::token::Keyword;

pub fn parse_comma_list<P, R>(parser: &mut Parser, mut parser_fn: P) -> ParseResult<Vec<R>>
where
    P: FnMut(&mut Parser) -> ParseResult<R>,
{
    let mut results = Vec::new();
    results.push(parser_fn(parser)?);

    loop {
        if matches!(parser.peek(), Some(Token::Comma)) {
            let _ = parser.next();
            results.push(parser_fn(parser)?);
            continue;
        }
        break;
    }
    Ok(results)
}

pub fn is_stop_keyword(k: &str) -> bool {
    Keyword::parse(k)
        .map(|kw| {
            matches!(
                kw,
                Keyword::Where
                    | Keyword::Group
                    | Keyword::Order
                    | Keyword::Having
                    | Keyword::Else
                    | Keyword::End
                    | Keyword::If
                    | Keyword::Declare
                    | Keyword::Set
                    | Keyword::Exec
                    | Keyword::Execute
                    | Keyword::Print
                    | Keyword::Select
                    | Keyword::Insert
                    | Keyword::Update
                    | Keyword::Delete
                    | Keyword::Go
                    | Keyword::From
                    | Keyword::Join
                    | Keyword::On
                    | Keyword::Union
                    | Keyword::Intersect
                    | Keyword::Except
                    | Keyword::Cross
                    | Keyword::Apply
                    | Keyword::Outer
                    | Keyword::Inner
                    | Keyword::Left
                    | Keyword::Right
                    | Keyword::Full
                    | Keyword::Pivot
                    | Keyword::Unpivot
                    | Keyword::Output
                    | Keyword::With
                    | Keyword::By
                    | Keyword::Asc
                    | Keyword::Desc
                    | Keyword::Create
                    | Keyword::Alter
                    | Keyword::Drop
                    | Keyword::Truncate
                    | Keyword::Begin
                    | Keyword::Commit
                    | Keyword::Rollback
                    | Keyword::While
                    | Keyword::Return
                    | Keyword::Fetch
                    | Keyword::Close
                    | Keyword::Deallocate
                    | Keyword::Open
                    | Keyword::Save
                    | Keyword::Try
                    | Keyword::Catch
                    | Keyword::RaiseError
            )
        })
        .unwrap_or(false)
}

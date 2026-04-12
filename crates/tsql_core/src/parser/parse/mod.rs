pub mod expressions;
pub mod statements;

use crate::parser::ast::*;
use crate::parser::error::{Expected, ParseResult};
use crate::parser::parse::expressions::parse_expr;
use crate::parser::state::Parser;
use crate::parser::token::Keyword;

pub use crate::parser::parse::expressions::{parse_comma_list, parse_data_type};
pub use crate::parser::parse::statements::alter::parse_alter;
pub use crate::parser::parse::statements::ddl::{
    parse_create, parse_create_index, parse_create_schema, parse_create_type, parse_table_body,
};
pub use crate::parser::parse::statements::dml::{
    parse_delete, parse_insert, parse_merge, parse_update,
};
pub use crate::parser::parse::statements::drop::parse_drop;
pub use crate::parser::parse::statements::other::{
    parse_begin_end, parse_declare, parse_exec_dispatch, parse_if, parse_set, parse_try_catch,
};
pub use crate::parser::parse::statements::query::{
    parse_multipart_name as multipart_name, parse_select,
};

pub fn parse_batch(parser: &mut Parser) -> ParseResult<Vec<Statement>> {
    let mut statements = Vec::new();
    while !parser.is_empty() {
        while matches!(parser.peek(), Some(Token::Semicolon)) {
            let _ = parser.next();
        }
        if parser.is_empty() {
            break;
        }
        if matches!(parser.peek(), Some(Token::Go)) {
            let _ = parser.next();
            continue;
        }
        statements.push(parse_statement(parser)?);
    }
    Ok(statements)
}

pub fn parse_statement(parser: &mut Parser) -> ParseResult<Statement> {
    parser.enter_recursion()?;
    let res = parse_statement_inner(parser);
    parser.leave_recursion();
    res
}

fn parse_statement_inner(parser: &mut Parser) -> ParseResult<Statement> {
    if parser.at_keyword(Keyword::With) {
        let _ = parser.next();
        let ctes = parse_comma_list(parser, parse_cte_def)?;
        let body = parse_statement(parser)?;
        return Ok(Statement::WithCte {
            ctes,
            body: Box::new(body),
        });
    }

    match parser.peek() {
        Some(Token::Keyword(k)) => match *k {
            Keyword::Select => {
                let s = parse_select(parser)?;
                if let Some(assigns) = try_select_assign(&s) {
                    Ok(Statement::Dml(DmlStatement::SelectAssign {
                        assignments: assigns,
                        from: s.from,
                        selection: s.selection,
                    }))
                } else {
                    Ok(Statement::Dml(DmlStatement::Select(Box::new(s))))
                }
            }
            Keyword::Insert => {
                let _ = parser.next();
                Ok(Statement::Dml(DmlStatement::Insert(Box::new(
                    parse_insert(parser)?,
                ))))
            }
            Keyword::Update => {
                let _ = parser.next();
                Ok(Statement::Dml(DmlStatement::Update(Box::new(
                    parse_update(parser)?,
                ))))
            }
            Keyword::Delete => {
                let _ = parser.next();
                Ok(Statement::Dml(DmlStatement::Delete(Box::new(
                    parse_delete(parser)?,
                ))))
            }
            Keyword::Create => {
                let _ = parser.next();
                if parser.at_keyword(Keyword::Index) {
                    let _ = parser.next();
                    return parse_create_index(parser);
                }
                if parser.at_keyword(Keyword::Type) {
                    let _ = parser.next();
                    return parse_create_type(parser);
                }
                if parser.at_keyword(Keyword::Schema) {
                    let _ = parser.next();
                    return parse_create_schema(parser);
                }
                Ok(Statement::Ddl(DdlStatement::Create(Box::new(
                    parse_create(parser)?,
                ))))
            }
            Keyword::Drop => {
                let _ = parser.next();
                parse_drop(parser)
            }
            Keyword::Truncate => {
                let _ = parser.next();
                parser.expect_keyword(Keyword::Table)?;
                let table = multipart_name(parser)?;
                Ok(Statement::Ddl(DdlStatement::TruncateTable(table)))
            }
            Keyword::Alter => {
                let _ = parser.next();
                parse_alter(parser)
            }
            Keyword::Declare => {
                let _ = parser.next();
                parse_declare_dispatch(parser)
            }
            Keyword::Merge => {
                let _ = parser.next();
                Ok(Statement::Dml(DmlStatement::Merge(Box::new(parse_merge(
                    parser,
                )?))))
            }
            Keyword::Set => {
                let _ = parser.next();
                parse_set_dispatch(parser)
            }
            Keyword::If => {
                let _ = parser.next();
                Ok(parse_if(parser)?)
            }
            Keyword::While => {
                let _ = parser.next();
                let condition = parse_expr(parser)?;
                let stmt = parse_statement(parser)?;
                Ok(Statement::Procedural(ProceduralStatement::While {
                    condition,
                    stmt: Box::new(stmt),
                }))
            }
            Keyword::Exec | Keyword::Execute => {
                let _ = parser.next();
                Ok(parse_exec_dispatch(parser)?)
            }
            Keyword::Print => {
                let _ = parser.next();
                Ok(Statement::Procedural(ProceduralStatement::Print(
                    parse_expr(parser)?,
                )))
            }
            Keyword::RaiseError => {
                let _ = parser.next();
                parser.expect_lparen()?;
                let message = parse_expr(parser)?;
                parser.expect_comma()?;
                let severity = parse_expr(parser)?;
                parser.expect_comma()?;
                let state = parse_expr(parser)?;
                parser.expect_rparen()?;
                Ok(Statement::Procedural(ProceduralStatement::Raiserror {
                    message,
                    severity,
                    state,
                }))
            }
            Keyword::Break => {
                let _ = parser.next();
                Ok(Statement::Procedural(ProceduralStatement::Break))
            }
            Keyword::Continue => {
                let _ = parser.next();
                Ok(Statement::Procedural(ProceduralStatement::Continue))
            }
            Keyword::Return => {
                let _ = parser.next();
                let expr = if !parser.is_empty()
                    && !matches!(parser.peek(), Some(Token::Semicolon) | Some(Token::Go))
                {
                    Some(parse_expr(parser)?)
                } else {
                    None
                };
                Ok(Statement::Procedural(ProceduralStatement::Return(expr)))
            }
            Keyword::Begin => {
                let _ = parser.next();
                parse_begin_dispatch(parser)
            }
            Keyword::Commit => {
                let _ = parser.next();
                statements::transaction::parse_commit_transaction(parser)
            }
            Keyword::Rollback => {
                let _ = parser.next();
                statements::transaction::parse_rollback_transaction(parser)
            }
            Keyword::Save => {
                let _ = parser.next();
                statements::transaction::parse_save_transaction(parser)
            }
            Keyword::Open => {
                let _ = parser.next();
                statements::cursor::parse_open_cursor(parser)
            }
            Keyword::Close => {
                let _ = parser.next();
                statements::cursor::parse_close_cursor(parser)
            }
            Keyword::Deallocate => {
                let _ = parser.next();
                statements::cursor::parse_deallocate_cursor(parser)
            }
            Keyword::Fetch => {
                let _ = parser.next();
                statements::cursor::parse_fetch_cursor(parser)
            }
            _ => parser.backtrack(Expected::Description("statement keyword")),
        },
        _ => parser.backtrack(Expected::Description("statement keyword")),
    }
}

fn parse_declare_dispatch(parser: &mut Parser) -> ParseResult<Statement> {
    if let Some(Token::Variable(var_name)) = parser.peek() {
        let var_name = var_name.clone();
        if parser
            .peek_at(1)
            .map(|t| matches!(t, Token::Keyword(Keyword::Table)))
            .unwrap_or(false)
        {
            let _ = parser.next();
            let _ = parser.next();
            parser.expect_lparen()?;
            let (columns, constraints) = parse_table_body(parser)?;
            parser.expect_rparen()?;
            return Ok(Statement::Procedural(
                ProceduralStatement::DeclareTableVar {
                    name: var_name,
                    columns,
                    constraints,
                },
            ));
        }
    }
    if let Some(Token::Identifier(cursor_name)) = parser.peek() {
        if parser
            .peek_at(1)
            .map(|t| matches!(t, Token::Keyword(Keyword::Cursor)))
            .unwrap_or(false)
        {
            let cursor_name = cursor_name.clone();
            let _ = parser.next();
            let _ = parser.next();
            parser.expect_keyword(Keyword::For)?;
            let query = parse_select(parser)?;
            return Ok(Statement::Procedural(ProceduralStatement::DeclareCursor {
                name: cursor_name,
                query,
            }));
        }
    }
    Ok(Statement::Procedural(ProceduralStatement::Declare(
        parse_declare(parser)?,
    )))
}

fn parse_set_dispatch(parser: &mut Parser) -> ParseResult<Statement> {
    if parser.at_keyword(Keyword::Transaction) {
        let _ = parser.next();
        parser.expect_keyword(Keyword::Isolation)?;
        parser.expect_keyword(Keyword::Level)?;
        let mut level_keywords = Vec::new();
        while let Some(Token::Keyword(k)) = parser.peek() {
            if matches!(
                k,
                Keyword::Read
                    | Keyword::Uncommitted
                    | Keyword::Committed
                    | Keyword::Repeatable
                    | Keyword::Serializable
                    | Keyword::Snapshot
            ) {
                level_keywords.push(*k);
                let _ = parser.next();
            } else {
                break;
            }
        }
        let iso = match level_keywords.as_slice() {
            [Keyword::Read, Keyword::Uncommitted] => {
                crate::parser::ast::IsolationLevel::ReadUncommitted
            }
            [Keyword::Read, Keyword::Committed] => {
                crate::parser::ast::IsolationLevel::ReadCommitted
            }
            [Keyword::Repeatable, Keyword::Read] => {
                crate::parser::ast::IsolationLevel::RepeatableRead
            }
            [Keyword::Serializable] => crate::parser::ast::IsolationLevel::Serializable,
            [Keyword::Snapshot] => crate::parser::ast::IsolationLevel::Snapshot,
            _ => crate::parser::ast::IsolationLevel::ReadCommitted,
        };
        return Ok(Statement::Session(
            SessionStatement::SetTransactionIsolationLevel(iso),
        ));
    }
    if matches_set_name(parser.peek(), "IDENTITY_INSERT") || parser.at_keyword(Keyword::Identity) {
        let _ = parser.next();
        if matches_set_name(parser.peek(), "INSERT") || parser.at_keyword(Keyword::Insert) {
            let _ = parser.next();
        }
        let table = multipart_name(parser)?;
        let on = match parser.next() {
            Some(Token::Keyword(k)) if *k == Keyword::On => true,
            Some(Token::Keyword(k)) if *k == Keyword::Off => false,
            _ => return parser.backtrack(Expected::Description("ON or OFF")),
        };
        return Ok(Statement::Session(SessionStatement::SetIdentityInsert {
            table,
            on,
        }));
    }

    fn matches_set_name(tok: Option<&Token>, expected: &str) -> bool {
        match tok {
            Some(Token::Identifier(id)) => id.eq_ignore_ascii_case(expected),
            Some(Token::Keyword(k)) => k.as_ref().eq_ignore_ascii_case(expected),
            _ => false,
        }
    }

    fn parse_bool_setting(
        parser: &mut Parser,
        option: crate::parser::ast::SessionOption,
    ) -> ParseResult<Statement> {
        let _ = parser.next();
        let val = match parser.next() {
            Some(Token::Keyword(k)) if *k == Keyword::On => true,
            Some(Token::Keyword(k)) if *k == Keyword::Off => false,
            Some(Token::Identifier(id)) if id.eq_ignore_ascii_case("ON") => true,
            Some(Token::Identifier(id)) if id.eq_ignore_ascii_case("OFF") => false,
            _ => return parser.backtrack(Expected::Description("ON or OFF")),
        };
        Ok(Statement::Session(SessionStatement::SetOption {
            option,
            value: crate::parser::ast::SessionOptionValue::Bool(val),
        }))
    }

    fn parse_text_setting(
        parser: &mut Parser,
        option: crate::parser::ast::SessionOption,
    ) -> ParseResult<Statement> {
        let _ = parser.next();
        let text = match parser.next() {
            Some(Token::String(s)) => s.clone(),
            Some(Token::Identifier(id)) => id.clone(),
            Some(Token::Keyword(k)) => k.as_ref().to_string(),
            _ => return parser.backtrack(Expected::Description("text")),
        };
        Ok(Statement::Session(SessionStatement::SetOption {
            option,
            value: crate::parser::ast::SessionOptionValue::Text(text),
        }))
    }

    fn parse_signed_int(parser: &mut Parser) -> ParseResult<i64> {
        let sign = if matches!(parser.peek(), Some(Token::Operator(op)) if *op == "-") {
            let _ = parser.next();
            -1i64
        } else if matches!(parser.peek(), Some(Token::Operator(op)) if *op == "+") {
            let _ = parser.next();
            1i64
        } else {
            1i64
        };

        if let Some(Token::Number { value: n, .. }) = parser.next() {
            Ok(sign * (*n as i64))
        } else {
            parser.backtrack(Expected::Description("number"))
        }
    }

    fn is_set_boundary(tok: Option<&Token>) -> bool {
        matches!(tok, None | Some(Token::Semicolon) | Some(Token::Go))
            || matches!(tok, Some(Token::Keyword(Keyword::Set)))
    }

    fn parse_generic_set_option(
        parser: &mut Parser,
        option_name: String,
    ) -> ParseResult<Statement> {
        let value = match parser.peek() {
            Some(Token::Keyword(k)) if *k == Keyword::On => {
                let _ = parser.next();
                crate::parser::ast::SessionOptionValue::Bool(true)
            }
            Some(Token::Keyword(k)) if *k == Keyword::Off => {
                let _ = parser.next();
                crate::parser::ast::SessionOptionValue::Bool(false)
            }
            Some(Token::Identifier(id)) if id.eq_ignore_ascii_case("ON") => {
                let _ = parser.next();
                crate::parser::ast::SessionOptionValue::Bool(true)
            }
            Some(Token::Identifier(id)) if id.eq_ignore_ascii_case("OFF") => {
                let _ = parser.next();
                crate::parser::ast::SessionOptionValue::Bool(false)
            }
            Some(Token::Number { .. }) => {
                crate::parser::ast::SessionOptionValue::Int(parse_signed_int(parser)?)
            }
            Some(Token::Operator(op)) if *op == "-" || *op == "+" => {
                crate::parser::ast::SessionOptionValue::Int(parse_signed_int(parser)?)
            }
            Some(_) => {
                let mut parts = Vec::new();
                while !is_set_boundary(parser.peek()) {
                    let Some(tok) = parser.next() else { break };
                    match tok {
                        Token::Identifier(v) | Token::String(v) | Token::Variable(v) => {
                            parts.push(v.clone())
                        }
                        Token::Keyword(k) => parts.push(k.as_ref().to_string()),
                        Token::Number { raw, .. } => parts.push(raw.clone()),
                        Token::Operator(op) => parts.push(op.clone()),
                        Token::Comma => parts.push(",".to_string()),
                        Token::Dot => parts.push(".".to_string()),
                        Token::LParen => parts.push("(".to_string()),
                        Token::RParen => parts.push(")".to_string()),
                        Token::Star => parts.push("*".to_string()),
                        Token::Tilde => parts.push("~".to_string()),
                        Token::BinaryLiteral(v) => parts.push(v.clone()),
                        Token::Semicolon | Token::Go => break,
                    }
                }
                if parts.is_empty() {
                    return parser.backtrack(Expected::Description("SET option value"));
                }
                crate::parser::ast::SessionOptionValue::Text(parts.join(" "))
            }
            None => return parser.backtrack(Expected::Description("SET option value")),
        };

        Ok(Statement::Session(SessionStatement::SetOption {
            option: crate::parser::ast::SessionOption::Unsupported(option_name),
            value,
        }))
    }

    if matches_set_name(parser.peek(), "ANSI_NULLS") {
        return parse_bool_setting(parser, crate::parser::ast::SessionOption::AnsiNulls);
    }
    if matches_set_name(parser.peek(), "QUOTED_IDENTIFIER") {
        return parse_bool_setting(parser, crate::parser::ast::SessionOption::QuotedIdentifier);
    }
    if matches_set_name(parser.peek(), "NOCOUNT") {
        return parse_bool_setting(parser, crate::parser::ast::SessionOption::NoCount);
    }
    if matches_set_name(parser.peek(), "XACT_ABORT") {
        return parse_bool_setting(parser, crate::parser::ast::SessionOption::XactAbort);
    }
    if matches_set_name(parser.peek(), "ANSI_NULL_DFLT_ON") {
        return parse_bool_setting(parser, crate::parser::ast::SessionOption::AnsiNullDfltOn);
    }
    if matches_set_name(parser.peek(), "ANSI_PADDING") {
        return parse_bool_setting(parser, crate::parser::ast::SessionOption::AnsiPadding);
    }
    if matches_set_name(parser.peek(), "ANSI_WARNINGS") {
        return parse_bool_setting(parser, crate::parser::ast::SessionOption::AnsiWarnings);
    }
    if matches_set_name(parser.peek(), "ARITHABORT") {
        return parse_bool_setting(parser, crate::parser::ast::SessionOption::ArithAbort);
    }
    if matches_set_name(parser.peek(), "CONCAT_NULL_YIELDS_NULL") {
        return parse_bool_setting(
            parser,
            crate::parser::ast::SessionOption::ConcatNullYieldsNull,
        );
    }
    if matches_set_name(parser.peek(), "CURSOR_CLOSE_ON_COMMIT") {
        return parse_bool_setting(
            parser,
            crate::parser::ast::SessionOption::CursorCloseOnCommit,
        );
    }
    if matches_set_name(parser.peek(), "IMPLICIT_TRANSACTIONS") {
        return parse_bool_setting(
            parser,
            crate::parser::ast::SessionOption::ImplicitTransactions,
        );
    }
    if matches_set_name(parser.peek(), "DATEFIRST") {
        let _ = parser.next();
        let val = parse_signed_int(parser)?;
        return Ok(Statement::Session(SessionStatement::SetOption {
            option: crate::parser::ast::SessionOption::DateFirst,
            value: crate::parser::ast::SessionOptionValue::Int(val),
        }));
    }
    if matches_set_name(parser.peek(), "LANGUAGE") {
        return parse_text_setting(parser, crate::parser::ast::SessionOption::Language);
    }
    if matches_set_name(parser.peek(), "DATEFORMAT") {
        return parse_text_setting(parser, crate::parser::ast::SessionOption::DateFormat);
    }
    if matches_set_name(parser.peek(), "LOCK_TIMEOUT") {
        let _ = parser.next();
        let val = parse_signed_int(parser)?;
        return Ok(Statement::Session(SessionStatement::SetOption {
            option: crate::parser::ast::SessionOption::LockTimeout,
            value: crate::parser::ast::SessionOptionValue::Int(val),
        }));
    }
    if matches_set_name(parser.peek(), "ROWCOUNT") {
        let _ = parser.next();
        let val = parse_signed_int(parser)?;
        return Ok(Statement::Session(SessionStatement::SetOption {
            option: crate::parser::ast::SessionOption::RowCount,
            value: crate::parser::ast::SessionOptionValue::Int(val),
        }));
    }
    if matches_set_name(parser.peek(), "TEXTSIZE") {
        let _ = parser.next();
        let val = parse_signed_int(parser)?;
        return Ok(Statement::Session(SessionStatement::SetOption {
            option: crate::parser::ast::SessionOption::TextSize,
            value: crate::parser::ast::SessionOptionValue::Int(val),
        }));
    }
    if matches_set_name(parser.peek(), "QUERY_GOVERNOR_COST_LIMIT") {
        let _ = parser.next();
        let val = parse_signed_int(parser)?;
        return Ok(Statement::Session(SessionStatement::SetOption {
            option: crate::parser::ast::SessionOption::QueryGovernorCostLimit,
            value: crate::parser::ast::SessionOptionValue::Int(val),
        }));
    }
    if matches_set_name(parser.peek(), "DEADLOCK_PRIORITY") {
        let _ = parser.next();
        let value = match parser.peek() {
            Some(Token::Keyword(Keyword::Low)) => {
                let _ = parser.next();
                crate::parser::ast::SessionOptionValue::Text("LOW".to_string())
            }
            Some(Token::Keyword(Keyword::Normal)) => {
                let _ = parser.next();
                crate::parser::ast::SessionOptionValue::Text("NORMAL".to_string())
            }
            Some(Token::Keyword(Keyword::High)) => {
                let _ = parser.next();
                crate::parser::ast::SessionOptionValue::Text("HIGH".to_string())
            }
            Some(Token::Number { .. }) | Some(Token::Operator(_)) => {
                crate::parser::ast::SessionOptionValue::Int(parse_signed_int(parser)?)
            }
            Some(Token::Identifier(id))
                if id.eq_ignore_ascii_case("LOW")
                    || id.eq_ignore_ascii_case("NORMAL")
                    || id.eq_ignore_ascii_case("HIGH") =>
            {
                let value = id.clone();
                let _ = parser.next();
                crate::parser::ast::SessionOptionValue::Text(value)
            }
            _ => return parser.backtrack(Expected::Description("deadlock priority")),
        };
        return Ok(Statement::Session(SessionStatement::SetOption {
            option: crate::parser::ast::SessionOption::DeadlockPriority,
            value,
        }));
    }

    if parser.at_keyword(Keyword::Statistics) {
        let _ = parser.next();
        if parser.at_keyword(Keyword::Io) {
            let _ = parser.next();
            return parse_bool_setting(parser, crate::parser::ast::SessionOption::StatisticsIo);
        }
        if parser.at_keyword(Keyword::Time) {
            let _ = parser.next();
            return parse_bool_setting(parser, crate::parser::ast::SessionOption::StatisticsTime);
        }
    }
    if parser.at_keyword(Keyword::Showplan) {
        let _ = parser.next();
        if parser.at_keyword(Keyword::All) {
            let _ = parser.next();
            return parse_bool_setting(parser, crate::parser::ast::SessionOption::ShowplanAll);
        }
    }

    if let Some(tok) = parser.peek() {
        let option_name = match tok {
            Token::Identifier(id) => Some(id.clone()),
            Token::Keyword(k) => Some(k.as_ref().to_string()),
            _ => None,
        };
        if let Some(option_name) = option_name {
            let _ = parser.next();
            return parse_generic_set_option(parser, option_name);
        }
    }
    parse_set(parser)
}

fn parse_begin_dispatch(parser: &mut Parser) -> ParseResult<Statement> {
    if parser.at_keyword(Keyword::Distributed) {
        let _ = parser.next();
    }
    if let Some(Token::Keyword(k2)) = parser.peek() {
        if *k2 == Keyword::Try {
            let _ = parser.next();
            return parse_try_catch(parser);
        }
        if matches!(k2, Keyword::Tran | Keyword::Transaction) {
            return statements::transaction::parse_begin_transaction(parser);
        }
    }
    parse_begin_end(parser)
}

fn parse_cte_def(parser: &mut Parser) -> ParseResult<CteDef> {
    let name = if let Some(tok) = parser.next() {
        match tok {
            Token::Identifier(id) => id.clone(),
            Token::Keyword(k) => k.as_ref().to_string(),
            _ => return parser.backtrack(Expected::Description("identifier or keyword")),
        }
    } else {
        return parser.backtrack(Expected::Description("identifier or keyword"));
    };

    let mut columns = Vec::new();
    if matches!(parser.peek(), Some(Token::LParen)) {
        let _ = parser.next();
        columns = parse_comma_list(parser, |p| {
            if let Some(tok) = p.next() {
                match tok {
                    Token::Identifier(id) => Ok(id.clone()),
                    Token::Keyword(k) => Ok(k.as_ref().to_string()),
                    _ => p.backtrack(Expected::Description("column name")),
                }
            } else {
                p.backtrack(Expected::Description("column name"))
            }
        })?;
        parser.expect_rparen()?;
    }

    parser.expect_keyword(Keyword::As)?;
    parser.expect_lparen()?;
    let query = parse_select(parser)?;
    parser.expect_rparen()?;

    Ok(CteDef {
        name,
        columns,
        query,
    })
}

pub fn parse_routine_param(parser: &mut Parser) -> ParseResult<RoutineParam> {
    let name = if let Some(Token::Variable(v)) = parser.next() {
        v.clone()
    } else {
        return parser.backtrack(Expected::Description("variable"));
    };
    let data_type = parse_data_type(parser)?;
    let mut is_output = false;
    if matches!(parser.peek(), Some(Token::Keyword(k)) if matches!(k, Keyword::Output | Keyword::Out))
    {
        let _ = parser.next();
        is_output = true;
    }
    let mut is_readonly = false;
    if matches!(data_type, crate::parser::ast::DataType::Custom(_)) {
        match parser.next() {
            Some(Token::Identifier(id)) if id.eq_ignore_ascii_case("READONLY") => {
                is_readonly = true;
            }
            Some(Token::Keyword(k)) if k.as_sql().eq_ignore_ascii_case("READONLY") => {
                is_readonly = true;
            }
            _ => return parser.backtrack(Expected::Description("READONLY")),
        }
    }
    let mut default = None;
    if let Some(Token::Operator(op)) = parser.peek() {
        if *op == "=" {
            let _ = parser.next();
            default = Some(parse_expr(parser)?);
        }
    }
    Ok(RoutineParam {
        name,
        data_type,
        is_output,
        is_readonly,
        default,
    })
}

fn try_select_assign(select: &SelectStmt) -> Option<Vec<SelectAssignTarget>> {
    let mut assigns = Vec::new();
    for item in &select.projection {
        if let Expr::Binary {
            left,
            op: BinaryOp::Eq,
            right,
        } = &item.expr
        {
            if let Expr::Variable(v) = &**left {
                assigns.push(SelectAssignTarget {
                    variable: v.clone(),
                    expr: (**right).clone(),
                });
                continue;
            }
        }
        return None;
    }
    if assigns.is_empty() {
        return None;
    }
    Some(assigns)
}

pub mod expressions;
pub mod statements;

use crate::parser::ast::*;
use winnow::prelude::*;
use winnow::error::{ErrMode, ContextError};

pub use crate::parser::parser::expressions::{parse_expr, parse_data_type, parse_comma_list, expect_keyword, expect_punctuation};
pub use crate::parser::parser::statements::query::{parse_select, parse_select_body, parse_table_ref, parse_multipart_name as multipart_name};
pub use crate::parser::parser::statements::other::{parse_declare, parse_set, parse_if, parse_begin_end, parse_exec_dispatch, parse_try_catch};
pub use crate::parser::parser::statements::dml::{parse_insert, parse_update, parse_delete, parse_merge};
pub use crate::parser::parser::statements::ddl::{parse_create, parse_column_def, parse_table_body, parse_create_index, parse_create_type, parse_create_schema};

pub fn parse_batch<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<Vec<Statement<'a>>> {
    let mut statements = Vec::new();
    while !input.is_empty() {
        while matches!(peek_token(input), Some(Token::Semicolon)) {
            let _ = next_token(input);
        }
        if input.is_empty() { break; }
        if matches!(peek_token(input), Some(Token::Go)) {
            let _ = next_token(input);
            continue;
        }
        statements.push(parse_statement(input)?);
    }
    Ok(statements)
}

pub fn parse_statement<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<Statement<'a>> {
    if let Some(Token::Keyword(k)) = peek_token(input) {
        let k_upper = k.to_uppercase();
        if k_upper == "WITH" {
             let _ = next_token(input);
             let ctes = parse_comma_list(input, parse_cte_def)?;
             let body = parse_statement(input)?;
             return Ok(Statement::WithCte { ctes, body: Box::new(body) });
        }
    }

    match peek_token(input) {
        Some(Token::Keyword(k)) => {
            let k_upper = k.to_uppercase();
            match k_upper.as_str() {
                "SELECT" => {
                    let s = parse_select(input)?;
                    if let Some(assigns) = try_select_assign(&s) {
                        Ok(Statement::SelectAssign {
                            assignments: assigns,
                            from: s.from,
                            selection: s.selection,
                        })
                    } else {
                        Ok(Statement::Select(Box::new(s)))
                    }
                }
                "INSERT" => {
                    let _ = next_token(input);
                    Ok(Statement::Insert(Box::new(parse_insert(input)?)))
                }
                "UPDATE" => {
                    let _ = next_token(input);
                    Ok(Statement::Update(Box::new(parse_update(input)?)))
                }
                "DELETE" => {
                    let _ = next_token(input);
                    Ok(Statement::Delete(Box::new(parse_delete(input)?)))
                }
                "CREATE" => {
                    let _ = next_token(input);
                    if matches!(peek_token(input), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("INDEX")) {
                        let _ = next_token(input);
                        return parse_create_index(input);
                    }
                    if matches!(peek_token(input), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("TYPE")) {
                        let _ = next_token(input);
                        return parse_create_type(input);
                    }
                    if matches!(peek_token(input), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("SCHEMA")) {
                        let _ = next_token(input);
                        return parse_create_schema(input);
                    }
                    Ok(Statement::Create(Box::new(parse_create(input)?)))
                }
                "DROP" => {
                    let _ = next_token(input);
                    if matches!(peek_token(input), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("TABLE")) {
                        let _ = next_token(input);
                        let table = multipart_name(input)?;
                        Ok(Statement::DropTable(table))
                    } else if matches!(peek_token(input), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("VIEW")) {
                        let _ = next_token(input);
                        let name = multipart_name(input)?;
                        Ok(Statement::DropView(name))
                    } else if matches!(peek_token(input), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("PROCEDURE") || kw.eq_ignore_ascii_case("PROC")) {
                        let _ = next_token(input);
                        let name = multipart_name(input)?;
                        Ok(Statement::DropProcedure(name))
                    } else if matches!(peek_token(input), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("INDEX")) {
                        let _ = next_token(input);
                        let name = multipart_name(input)?;
                        expect_keyword(input, "ON")?;
                        let table = multipart_name(input)?;
                        Ok(Statement::DropIndex { name, table })
                    } else if matches!(peek_token(input), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("TYPE")) {
                        let _ = next_token(input);
                        let name = multipart_name(input)?;
                        Ok(Statement::DropType(name))
                    } else if matches!(peek_token(input), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("SCHEMA")) {
                        let _ = next_token(input);
                        let name = match next_token(input) {
                             Some(Token::Identifier(id)) | Some(Token::Keyword(id)) => id.clone(),
                             _ => return Err(ErrMode::Backtrack(ContextError::new())),
                        };
                        Ok(Statement::DropSchema(name))
                    } else if matches!(peek_token(input), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("FUNCTION")) {
                        let _ = next_token(input);
                        let name = multipart_name(input)?;
                        Ok(Statement::DropFunction(name))
                    } else if matches!(peek_token(input), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("TRIGGER")) {
                        let _ = next_token(input);
                        let name = multipart_name(input)?;
                        Ok(Statement::DropTrigger(name))
                    } else {
                        Err(ErrMode::Backtrack(ContextError::new()))
                    }
                }
                "TRUNCATE" => {
                    let _ = next_token(input);
                    expect_keyword(input, "TABLE")?;
                    let table = multipart_name(input)?;
                    Ok(Statement::TruncateTable(table))
                }
                "ALTER" => {
                    let _ = next_token(input);
                    if matches!(peek_token(input), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("TABLE")) {
                        let _ = next_token(input);
                        let table = multipart_name(input)?;
                        if matches!(peek_token(input), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("ADD")) {
                            let _ = next_token(input);
                            // Check for ADD CONSTRAINT
                            if matches!(peek_token(input), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("CONSTRAINT")) {
                                let _ = next_token(input);
                                let constraint_name = match next_token(input) {
                                    Some(Token::Identifier(id)) | Some(Token::Keyword(id)) => id.clone(),
                                    _ => return Err(ErrMode::Backtrack(ContextError::new())),
                                };
                                // Parse constraint type
                                let constraint = if matches!(peek_token(input), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("PRIMARY")) {
                                    let _ = next_token(input);
                                    expect_keyword(input, "KEY")?;
                                    expect_punctuation(input, Token::LParen)?;
                                    let columns = parse_comma_list(input, |i| {
                                        match next_token(i) {
                                            Some(Token::Identifier(id)) | Some(Token::Keyword(id)) => Ok(id.clone()),
                                            _ => Err(ErrMode::Backtrack(ContextError::new())),
                                        }
                                    })?;
                                    expect_punctuation(input, Token::RParen)?;
                                    TableConstraint::PrimaryKey {
                                        name: Some(constraint_name),
                                        columns,
                                    }
                                } else if matches!(peek_token(input), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("FOREIGN")) {
                                    let _ = next_token(input);
                                    expect_keyword(input, "KEY")?;
                                    expect_punctuation(input, Token::LParen)?;
                                    let columns = parse_comma_list(input, |i| {
                                        match next_token(i) {
                                            Some(Token::Identifier(id)) | Some(Token::Keyword(id)) => Ok(id.clone()),
                                            _ => Err(ErrMode::Backtrack(ContextError::new())),
                                        }
                                    })?;
                                    expect_punctuation(input, Token::RParen)?;
                                    expect_keyword(input, "REFERENCES")?;
                                    let ref_table = multipart_name(input)?;
                                    let mut ref_columns = Vec::new();
                                    if matches!(peek_token(input), Some(Token::LParen)) {
                                        let _ = next_token(input);
                                        ref_columns = parse_comma_list(input, |i| {
                                            match next_token(i) {
                                                Some(Token::Identifier(id)) | Some(Token::Keyword(id)) => Ok(id.clone()),
                                                _ => Err(ErrMode::Backtrack(ContextError::new())),
                                            }
                                        })?;
                                        expect_punctuation(input, Token::RParen)?;
                                    }
                                    // Parse ON DELETE/UPDATE actions
                                    let mut on_delete = None;
                                    let mut on_update = None;
                                    while let Some(Token::Keyword(kw)) = peek_token(input) {
                                        if kw.eq_ignore_ascii_case("ON") {
                                            let _ = next_token(input);
                                            if matches!(peek_token(input), Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("DELETE")) {
                                                let _ = next_token(input);
                                                on_delete = Some(parse_referential_action_v2(input)?);
                                            } else if matches!(peek_token(input), Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("UPDATE")) {
                                                let _ = next_token(input);
                                                on_update = Some(parse_referential_action_v2(input)?);
                                            }
                                        } else {
                                            break;
                                        }
                                    }
                                    TableConstraint::ForeignKey {
                                        name: Some(constraint_name),
                                        columns,
                                        ref_table,
                                        ref_columns,
                                        on_delete,
                                        on_update,
                                    }
                                } else if matches!(peek_token(input), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("CHECK")) {
                                    let _ = next_token(input);
                                    expect_punctuation(input, Token::LParen)?;
                                    let expr = parse_expr(input)?;
                                    expect_punctuation(input, Token::RParen)?;
                                    TableConstraint::Check {
                                        name: Some(constraint_name),
                                        expr,
                                    }
                                } else if matches!(peek_token(input), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("UNIQUE")) {
                                    let _ = next_token(input);
                                    expect_punctuation(input, Token::LParen)?;
                                    let columns = parse_comma_list(input, |i| {
                                        match next_token(i) {
                                            Some(Token::Identifier(id)) | Some(Token::Keyword(id)) => Ok(id.clone()),
                                            _ => Err(ErrMode::Backtrack(ContextError::new())),
                                        }
                                    })?;
                                    expect_punctuation(input, Token::RParen)?;
                                    TableConstraint::Unique {
                                        name: Some(constraint_name),
                                        columns,
                                    }
                                } else {
                                    return Err(ErrMode::Backtrack(ContextError::new()));
                                };
                                Ok(Statement::AlterTable { table, action: AlterTableAction::AddConstraint(constraint) })
                            } else {
                                // ADD column
                                let col = parse_column_def(input)?;
                                Ok(Statement::AlterTable { table, action: AlterTableAction::AddColumn(col) })
                            }
                        } else if matches!(peek_token(input), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("DROP")) {
                            let _ = next_token(input);
                            if matches!(peek_token(input), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("COLUMN")) {
                                let _ = next_token(input);
                                let col_name = match next_token(input) {
                                    Some(Token::Identifier(id)) | Some(Token::Keyword(id)) => id.clone(),
                                    _ => return Err(ErrMode::Backtrack(ContextError::new())),
                                };
                                Ok(Statement::AlterTable { table, action: AlterTableAction::DropColumn(col_name) })
                            } else if matches!(peek_token(input), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("CONSTRAINT")) {
                                let _ = next_token(input);
                                let constraint_name = match next_token(input) {
                                    Some(Token::Identifier(id)) | Some(Token::Keyword(id)) => id.clone(),
                                    _ => return Err(ErrMode::Backtrack(ContextError::new())),
                                };
                                Ok(Statement::AlterTable { table, action: AlterTableAction::DropConstraint(constraint_name) })
                            } else {
                                Err(ErrMode::Backtrack(ContextError::new()))
                            }
                        } else {
                            Err(ErrMode::Backtrack(ContextError::new()))
                        }
                    } else {
                        Err(ErrMode::Backtrack(ContextError::new()))
                    }
                }
                "DECLARE" => {
                     let _ = next_token(input);
                     // Check for DECLARE @name TABLE (...)
                     if let Some(Token::Variable(var_name)) = peek_token(input) {
                         let var_name = var_name.clone();
                         let mut temp = *input;
                         let _ = next_token(&mut temp);
                         if matches!(peek_token(&temp), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("TABLE")) {
                             let _ = next_token(input); // consume variable
                             let _ = next_token(input); // consume TABLE
                             expect_punctuation(input, Token::LParen)?;
                             let (columns, constraints) = parse_table_body(input)?;
                             expect_punctuation(input, Token::RParen)?;
                             return Ok(Statement::DeclareTableVar { name: var_name, columns, constraints });
                         }
                     }
                     // Check for DECLARE cursor_name CURSOR FOR SELECT
                     if let Some(Token::Identifier(cursor_name)) = peek_token(input) {
                         let cursor_name = cursor_name.clone();
                         let mut temp = *input;
                         let _ = next_token(&mut temp);
                         if matches!(peek_token(&temp), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("CURSOR")) {
                             let _ = next_token(input); // consume name
                             let _ = next_token(input); // consume CURSOR
                             expect_keyword(input, "FOR")?;
                             let query = parse_select(input)?;
                             return Ok(Statement::DeclareCursor { name: cursor_name, query });
                         }
                     }
                     Ok(Statement::Declare(parse_declare(input)?))
                }
                "MERGE" => {
                    let _ = next_token(input);
                    let m = parse_merge(input)?;
                    Ok(Statement::Merge(Box::new(m)))
                }
                "SET" => {
                     let _ = next_token(input);
                     if let Some(Token::Keyword(kw)) = peek_token(input) {
                         let kw_upper = kw.to_uppercase();
                         if kw_upper == "LOCK_TIMEOUT" {
                             let _ = next_token(input);
                             let val = if let Some(Token::Number(n)) = next_token(input) {
                                 *n as i32
                             } else {
                                 return Err(ErrMode::Backtrack(ContextError::new()));
                             };
                             return Ok(Statement::SetOption { 
                                 option: crate::ast::SessionOption::LockTimeout, 
                                 value: crate::ast::SessionOptionValue::Int(val) 
                             });
                         }
                     }
                     if matches!(peek_token(input), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("TRANSACTION")) {
                          let _ = next_token(input);
                          expect_keyword(input, "ISOLATION")?;
                          expect_keyword(input, "LEVEL")?;
                          
                          let mut level = String::new();
                          loop {
                              if let Some(Token::Keyword(k)) = peek_token(input) {
                                  let k_upper = k.to_uppercase();
                                  if matches!(k_upper.as_str(), "READ" | "UNCOMMITTED" | "COMMITTED" | "REPEATABLE" | "SERIALIZABLE" | "SNAPSHOT") {
                                      level.push_str(k.as_ref());
                                      level.push(' ');
                                      let _ = next_token(input);
                                      continue;
                                  }
                              }
                              break;
                          }
                          level = level.trim().to_uppercase();
                          let iso = match level.as_str() {
                              "READ UNCOMMITTED" => crate::ast::IsolationLevel::ReadUncommitted,
                              "READ COMMITTED" => crate::ast::IsolationLevel::ReadCommitted,
                              "REPEATABLE READ" => crate::ast::IsolationLevel::RepeatableRead,
                              "SERIALIZABLE" => crate::ast::IsolationLevel::Serializable,
                              "SNAPSHOT" => crate::ast::IsolationLevel::Snapshot,
                              _ => crate::ast::IsolationLevel::ReadCommitted,
                          };
                          return Ok(Statement::SetTransactionIsolationLevel(iso));
                     }
                     if matches!(peek_token(input), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("NOCOUNT")) {
                         let _ = next_token(input);
                         let val = match next_token(input) {
                             Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("ON") => true,
                             Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("OFF") => false,
                             _ => return Err(ErrMode::Backtrack(ContextError::new())),
                         };
                         return Ok(Statement::SetOption { 
                             option: crate::ast::SessionOption::NoCount, 
                             value: crate::ast::SessionOptionValue::Bool(val) 
                         });
                     }
                     if matches!(peek_token(input), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("IDENTITY_INSERT")) {
                         let _ = next_token(input);
                         let table = multipart_name(input)?;
                         let on = match next_token(input) {
                             Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("ON") => true,
                             Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("OFF") => false,
                             _ => return Err(ErrMode::Backtrack(ContextError::new())),
                         };
                         return Ok(Statement::SetIdentityInsert { table, on });
                     }
                     Ok(parse_set(input)?)
                }
                "IF" => {
                     let _ = next_token(input);
                     Ok(parse_if(input, parse_statement)?)
                }
                "WHILE" => {
                    let _ = next_token(input);
                    let condition = parse_expr(input)?;
                    let stmt = parse_statement(input)?;
                    Ok(Statement::While { condition, stmt: Box::new(stmt) })
                }
                "EXEC" | "EXECUTE" => {
                     let _ = next_token(input);
                     Ok(parse_exec_dispatch(input)?)
                }
                "PRINT" => {
                     let _ = next_token(input);
                     Ok(Statement::Print(parse_expr(input)?))
                }
                "RAISERROR" => {
                    let _ = next_token(input);
                    expect_punctuation(input, Token::LParen)?;
                    let message = parse_expr(input)?;
                    expect_punctuation(input, Token::Comma)?;
                    let severity = parse_expr(input)?;
                    expect_punctuation(input, Token::Comma)?;
                    let state = parse_expr(input)?;
                    expect_punctuation(input, Token::RParen)?;
                    Ok(Statement::Raiserror { message, severity, state })
                }
                "BREAK" => {
                    let _ = next_token(input);
                    Ok(Statement::Break)
                }
                "CONTINUE" => {
                    let _ = next_token(input);
                    Ok(Statement::Continue)
                }
                "RETURN" => {
                    let _ = next_token(input);
                    let expr = if !input.is_empty() && !matches!(peek_token(input), Some(Token::Semicolon) | Some(Token::Go)) {
                         Some(parse_expr(input)?)
                    } else {
                         None
                    };
                    Ok(Statement::Return(expr))
                }
                "BEGIN" => {
                    let _ = next_token(input);
                    if matches!(peek_token(input), Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("DISTRIBUTED")) {
                        let _ = next_token(input);
                    }
                    if let Some(Token::Keyword(k2)) = peek_token(input) {
                        if k2.eq_ignore_ascii_case("TRY") {
                            let _ = next_token(input);
                            return parse_try_catch(input);
                        }
                        if k2.eq_ignore_ascii_case("TRAN") || k2.eq_ignore_ascii_case("TRANSACTION") {
                            let _ = next_token(input);
                            let name = if let Some(Token::Identifier(id)) = peek_token(input) {
                                let name = id.clone();
                                let _ = next_token(input);
                                Some(name)
                            } else {
                                None
                            };
                            if matches!(peek_token(input), Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("WITH")) {
                                let mut temp = *input;
                                let _ = next_token(&mut temp);
                                if matches!(peek_token(&temp), Some(tok) if tok.eq_ignore_ascii_case("MARK")) {
                                    let _ = next_token(input);
                                    let _ = next_token(input);
                                    if matches!(peek_token(input), Some(Token::String(_)) | Some(Token::Identifier(_)) | Some(Token::Keyword(_))) {
                                        let _ = next_token(input);
                                    }
                                }
                            }
                            return Ok(Statement::BeginTransaction(name));
                        }
                    }
                    parse_begin_end(input, parse_statement)
                }
                "COMMIT" => {
                    let _ = next_token(input);
                    if let Some(Token::Keyword(k2)) = peek_token(input) {
                        if k2.eq_ignore_ascii_case("TRAN") || k2.eq_ignore_ascii_case("TRANSACTION") {
                            let _ = next_token(input);
                        }
                    }
                    let name = if let Some(Token::Identifier(id)) = peek_token(input) {
                        let name = id.clone();
                        let _ = next_token(input);
                        Some(name)
                    } else {
                        None
                    };
                    Ok(Statement::CommitTransaction(name))
                }
                "ROLLBACK" => {
                    let _ = next_token(input);
                    if let Some(Token::Keyword(k2)) = peek_token(input) {
                        if k2.eq_ignore_ascii_case("TRAN") || k2.eq_ignore_ascii_case("TRANSACTION") {
                            let _ = next_token(input);
                        }
                    }
                    let name = if let Some(Token::Identifier(id)) = peek_token(input) {
                        let name = id.clone();
                        let _ = next_token(input);
                        Some(name)
                    } else {
                        None
                    };
                    Ok(Statement::RollbackTransaction(name))
                }
                "SAVE" => {
                    let _ = next_token(input);
                    if let Some(Token::Keyword(k2)) = peek_token(input) {
                        if k2.eq_ignore_ascii_case("TRAN") || k2.eq_ignore_ascii_case("TRANSACTION") {
                            let _ = next_token(input);
                        }
                    }
                    if let Some(Token::Identifier(id)) = next_token(input) {
                        Ok(Statement::SaveTransaction(id.clone()))
                    } else {
                        Err(ErrMode::Backtrack(ContextError::new()))
                    }
                }
                "OPEN" => {
                    let _ = next_token(input);
                    if let Some(Token::Identifier(id)) = next_token(input) {
                        Ok(Statement::OpenCursor(id.clone()))
                    } else {
                        Err(ErrMode::Backtrack(ContextError::new()))
                    }
                }
                "CLOSE" => {
                    let _ = next_token(input);
                    if let Some(Token::Identifier(id)) = next_token(input) {
                        Ok(Statement::CloseCursor(id.clone()))
                    } else {
                        Err(ErrMode::Backtrack(ContextError::new()))
                    }
                }
                "DEALLOCATE" => {
                    let _ = next_token(input);
                    if let Some(Token::Identifier(id)) = next_token(input) {
                        Ok(Statement::DeallocateCursor(id.clone()))
                    } else {
                        Err(ErrMode::Backtrack(ContextError::new()))
                    }
                }
                "FETCH" => {
                    let _ = next_token(input);
                    let mut direction = FetchDirection::Next;
                    if let Some(Token::Keyword(k)) = peek_token(input) {
                        match k.to_uppercase().as_str() {
                            "NEXT" => { let _ = next_token(input); direction = FetchDirection::Next; }
                            "PRIOR" => { let _ = next_token(input); direction = FetchDirection::Prior; }
                            "FIRST" => { let _ = next_token(input); direction = FetchDirection::First; }
                            "LAST" => { let _ = next_token(input); direction = FetchDirection::Last; }
                            "ABSOLUTE" => {
                                let _ = next_token(input);
                                direction = FetchDirection::Absolute(parse_expr(input)?);
                            }
                            "RELATIVE" => {
                                let _ = next_token(input);
                                direction = FetchDirection::Relative(parse_expr(input)?);
                            }
                            _ => {}
                        }
                    }
                    if matches!(peek_token(input), Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("FROM")) {
                        let _ = next_token(input);
                    }
                    let name = if let Some(Token::Identifier(id)) = next_token(input) {
                        id.clone()
                    } else {
                        return Err(ErrMode::Backtrack(ContextError::new()));
                    };
                    let mut into_vars = None;
                    if matches!(peek_token(input), Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("INTO")) {
                        let _ = next_token(input);
                        into_vars = Some(parse_comma_list(input, |i| {
                            if let Some(Token::Variable(v)) = next_token(i) {
                                Ok(v.clone())
                            } else {
                                Err(ErrMode::Backtrack(ContextError::new()))
                            }
                        })?);
                    }
                    Ok(Statement::FetchCursor { name, direction, into_vars })
                }
                _ => Err(ErrMode::Backtrack(ContextError::new())),
            }
        }
        _ => Err(ErrMode::Backtrack(ContextError::new())),
    }
}

fn parse_cte_def<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<CteDef<'a>> {
    let name = if let Some(tok) = next_token(input) {
        match tok {
            Token::Identifier(id) | Token::Keyword(id) => id.clone(),
            _ => return Err(ErrMode::Backtrack(ContextError::new())),
        }
    } else {
        return Err(ErrMode::Backtrack(ContextError::new()));
    };

    let mut columns = Vec::new();
    if matches!(peek_token(input), Some(Token::LParen)) {
        let _ = next_token(input);
        columns = parse_comma_list(input, |i| {
            if let Some(tok) = next_token(i) {
                match tok {
                    Token::Identifier(id) | Token::Keyword(id) => Ok(id.clone()),
                    _ => Err(ErrMode::Backtrack(ContextError::new())),
                }
            } else {
                Err(ErrMode::Backtrack(ContextError::new()))
            }
        })?;
        expect_punctuation(input, Token::RParen)?;
    }

    expect_keyword(input, "AS")?;
    expect_punctuation(input, Token::LParen)?;
    let query = parse_select(input)?;
    expect_punctuation(input, Token::RParen)?;

    Ok(CteDef { name, columns, query })
}

pub fn parse_routine_param<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<RoutineParam<'a>> {
    let name = if let Some(tok) = next_token(input) {
        match tok {
            Token::Variable(v) => v.clone(),
            _ => return Err(ErrMode::Backtrack(ContextError::new())),
        }
    } else {
        return Err(ErrMode::Backtrack(ContextError::new()));
    };
    let data_type = parse_data_type(input)?;
    let mut is_output = false;
    if matches!(peek_token(input), Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("OUTPUT") || k.eq_ignore_ascii_case("OUT")) {
        let _ = next_token(input);
        is_output = true;
    }
    let mut default = None;
    if let Some(Token::Operator(op)) = peek_token(input) {
        if op.as_ref() == "=" {
            let _ = next_token(input);
            default = Some(parse_expr(input)?);
        }
    }
    Ok(RoutineParam { name, data_type, is_output, default })
}

fn try_select_assign<'a>(select: &SelectStmt<'a>) -> Option<Vec<SelectAssignTarget<'a>>> {
    let mut assigns = Vec::new();
    for item in &select.projection {
        if let Expr::Binary { left, op: BinaryOp::Eq, right } = &item.expr {
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
    if assigns.is_empty() { return None; }
    Some(assigns)
}

pub fn peek_token<'a>(input: &&'a [Token<'a>]) -> Option<&'a Token<'a>> {
    input.first()
}

pub fn next_token<'a>(input: &mut &'a [Token<'a>]) -> Option<&'a Token<'a>> {
    let tok = input.first()?;
    *input = &input[1..];
    Some(tok)
}

impl<'a> Token<'a> {
    pub fn eq_ignore_ascii_case(&self, other: &str) -> bool {
        match self {
            Token::Keyword(k) | Token::Identifier(k) => k.eq_ignore_ascii_case(other),
            _ => false,
        }
    }
}

fn parse_referential_action_v2<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<ReferentialAction> {
    match next_token(input) {
        Some(Token::Keyword(k)) => match k.to_uppercase().as_str() {
            "NO" => {
                expect_keyword(input, "ACTION")?;
                Ok(ReferentialAction::NoAction)
            }
            "CASCADE" => Ok(ReferentialAction::Cascade),
            "SET" => {
                if matches!(peek_token(input), Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("NULL")) {
                    let _ = next_token(input);
                    Ok(ReferentialAction::SetNull)
                } else if matches!(peek_token(input), Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("DEFAULT")) {
                    let _ = next_token(input);
                    Ok(ReferentialAction::SetDefault)
                } else {
                    Err(ErrMode::Backtrack(ContextError::new()))
                }
            }
            _ => Err(ErrMode::Backtrack(ContextError::new())),
        },
        _ => Err(ErrMode::Backtrack(ContextError::new())),
    }
}

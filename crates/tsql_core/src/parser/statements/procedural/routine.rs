use crate::ast::*;
use crate::error::DbError;
use crate::parser::utils::{find_matching_paren_index, parse_object_name, split_csv_top_level};

fn parse_routine_param(raw: &str) -> Result<RoutineParam, DbError> {
    let tokens = raw.split_whitespace().collect::<Vec<_>>();
    if tokens.len() < 2 {
        return Err(DbError::Parse("invalid routine parameter".into()));
    }
    let name = tokens[0].to_string();
    let type_token = tokens[1];
    let mut idx = 2usize;
    let mut default = None;
    let mut is_output = false;
    let mut is_readonly = false;
    while idx < tokens.len() {
        if tokens[idx].eq_ignore_ascii_case("=") {
            if idx + 1 >= tokens.len() {
                return Err(DbError::Parse(
                    "missing default value in routine parameter".into(),
                ));
            }
            let expr_txt = tokens[idx + 1..].join(" ");
            default = Some(crate::parser::expression::parse_expr(&expr_txt)?);
            break;
        }
        if tokens[idx].eq_ignore_ascii_case("OUTPUT") {
            is_output = true;
        } else if tokens[idx].eq_ignore_ascii_case("READONLY") {
            is_readonly = true;
        }
        idx += 1;
    }

    let param_type = match super::parse_data_type_inline(type_token) {
        Ok(dt) => RoutineParamType::Scalar(dt),
        Err(_) => RoutineParamType::TableType(parse_object_name(type_token)),
    };

    if matches!(param_type, RoutineParamType::TableType(_)) {
        if !is_readonly {
            return Err(DbError::Parse(
                "table-valued parameters must be declared READONLY".into(),
            ));
        }
        if is_output {
            return Err(DbError::Parse(
                "table-valued parameters cannot be OUTPUT".into(),
            ));
        }
        if default.is_some() {
            return Err(DbError::Parse(
                "table-valued parameters cannot have default values".into(),
            ));
        }
    }

    Ok(RoutineParam {
        name,
        param_type,
        is_output,
        is_readonly,
        default,
    })
}

pub(crate) fn parse_routine_params(input: &str) -> Result<Vec<RoutineParam>, DbError> {
    let trimmed = input
        .trim()
        .trim_start_matches('(')
        .trim_end_matches(')')
        .trim();
    if trimmed.is_empty() {
        return Ok(vec![]);
    }
    let mut out = Vec::new();
    for raw in split_csv_top_level(trimmed) {
        out.push(parse_routine_param(&raw)?);
    }
    Ok(out)
}

pub(crate) fn parse_create_procedure(sql: &str) -> Result<Statement, DbError> {
    let after = sql["CREATE PROCEDURE".len()..].trim();
    let as_idx = crate::parser::utils::find_keyword_top_level(after, "AS")
        .ok_or_else(|| DbError::Parse("CREATE PROCEDURE missing AS".into()))?;
    let head = after[..as_idx].trim();
    let body = after[as_idx + "AS".len()..].trim();
    let mut tokens = head.split_whitespace();
    let name_tok = tokens
        .next()
        .ok_or_else(|| DbError::Parse("CREATE PROCEDURE missing name".into()))?;
    let name = parse_object_name(name_tok);
    let params_text = head[name_tok.len()..].trim();
    let params = parse_routine_params(params_text)?;
    let proc_body = if body.to_uppercase().starts_with("BEGIN") {
        super::parse_begin_end_body(body)?
    } else {
        crate::parser::parse_batch(body)?
    };
    Ok(Statement::CreateProcedure(CreateProcedureStmt {
        name,
        params,
        body: proc_body,
    }))
}

pub(crate) fn parse_drop_procedure(sql: &str) -> Result<Statement, DbError> {
    let name = parse_object_name(sql["DROP PROCEDURE".len()..].trim());
    Ok(Statement::DropProcedure(DropProcedureStmt { name }))
}

pub(crate) fn parse_create_function(sql: &str) -> Result<Statement, DbError> {
    let after = sql["CREATE FUNCTION".len()..].trim();
    let open = after
        .find('(')
        .ok_or_else(|| DbError::Parse("CREATE FUNCTION missing '('".into()))?;
    let name = parse_object_name(after[..open].trim());
    let close = find_matching_paren_index(after, open)
        .ok_or_else(|| DbError::Parse("CREATE FUNCTION missing ')'".into()))?;
    let params_raw = after[open + 1..close].trim();
    let params = parse_routine_params(params_raw)?;
    if params
        .iter()
        .any(|p| matches!(p.param_type, RoutineParamType::TableType(_)))
    {
        return Err(DbError::Parse(
            "functions do not support table-valued parameters".into(),
        ));
    }
    let rest = after[close + 1..].trim();
    let returns_idx = crate::parser::utils::find_keyword_top_level(rest, "RETURNS")
        .ok_or_else(|| DbError::Parse("CREATE FUNCTION missing RETURNS".into()))?;
    let after_returns = rest[returns_idx + "RETURNS".len()..].trim();

    if after_returns.to_uppercase().starts_with("TABLE") {
        let as_idx = crate::parser::utils::find_keyword_top_level(after_returns, "AS")
            .ok_or_else(|| DbError::Parse("inline TVF missing AS".into()))?;
        let after_as = after_returns[as_idx + "AS".len()..].trim();
        let ret_idx = crate::parser::utils::find_keyword_top_level(after_as, "RETURN")
            .ok_or_else(|| DbError::Parse("inline TVF missing RETURN".into()))?;
        let query_raw = after_as[ret_idx + "RETURN".len()..].trim();
        let query_text = query_raw
            .trim_start_matches('(')
            .trim_end_matches(')')
            .trim();
        let query = match crate::parser::statements::select::parse_select(query_text)? {
            Statement::Select(s) => s,
            _ => return Err(DbError::Parse("inline TVF RETURN must be SELECT".into())),
        };
        return Ok(Statement::CreateFunction(CreateFunctionStmt {
            name,
            params,
            returns: None,
            body: FunctionBody::InlineTable(query),
        }));
    }

    let as_idx = crate::parser::utils::find_keyword_top_level(after_returns, "AS")
        .ok_or_else(|| DbError::Parse("scalar UDF missing AS".into()))?;
    let type_raw = after_returns[..as_idx].trim();
    let returns = Some(super::parse_data_type_inline(type_raw)?);
    let body_raw = after_returns[as_idx + "AS".len()..].trim();
    let body_stmts = if body_raw.to_uppercase().starts_with("BEGIN") {
        super::parse_begin_end_body(body_raw)?
    } else {
        crate::parser::parse_batch(body_raw)?
    };

    if body_stmts.len() == 1 {
        if let Statement::Return(Some(expr)) = &body_stmts[0] {
            return Ok(Statement::CreateFunction(CreateFunctionStmt {
                name,
                params,
                returns,
                body: FunctionBody::ScalarReturn(expr.clone()),
            }));
        }
    }

    Ok(Statement::CreateFunction(CreateFunctionStmt {
        name,
        params,
        returns,
        body: FunctionBody::Scalar(body_stmts),
    }))
}

pub(crate) fn parse_drop_function(sql: &str) -> Result<Statement, DbError> {
    let name = parse_object_name(sql["DROP FUNCTION".len()..].trim());
    Ok(Statement::DropFunction(DropFunctionStmt { name }))
}

use crate::ast::*;
use crate::error::DbError;

use crate::parser::expression::parse_expr_with_subqueries;
use crate::parser::statements::subquery_utils::{apply_subquery_map, extract_subqueries};
use crate::parser::utils::{
    find_if_blocks, find_keyword_top_level, find_top_level_begin, parse_object_name,
    split_csv_top_level,
};

pub(crate) fn parse_declare(sql: &str) -> Result<Statement, DbError> {
    let after_declare = sql["DECLARE".len()..].trim();
    let at_pos = after_declare
        .find('@')
        .ok_or_else(|| DbError::Parse("DECLARE requires @variable name".into()))?;
    let rest = &after_declare[at_pos..];
    let name_end = rest
        .find(|c: char| c.is_whitespace() || c == '=')
        .unwrap_or(rest.len());
    let var_name = rest[..name_end].to_string();
    let after_name = rest[name_end..].trim();

    if after_name.to_uppercase().starts_with("TABLE") {
        let open = after_name
            .find('(')
            .ok_or_else(|| DbError::Parse("DECLARE @t TABLE missing '('".into()))?;
        let close = after_name
            .rfind(')')
            .ok_or_else(|| DbError::Parse("DECLARE @t TABLE missing ')'".into()))?;
        let body = after_name[open + 1..close].trim();
        let mut columns = Vec::new();
        let mut table_constraints = Vec::new();
        for raw_col in split_csv_top_level(body) {
            let item = raw_col.trim();
            if item.to_uppercase().starts_with("CONSTRAINT ") {
                table_constraints.push(super::ddl::parse_table_constraint(item)?);
            } else {
                columns.push(super::ddl::parse_column_spec(item)?);
            }
        }
        return Ok(Statement::DeclareTableVar(DeclareTableVarStmt {
            name: var_name,
            columns,
            table_constraints,
        }));
    }

    let (data_type_spec, after_type) = parse_type_from_declare(after_name)?;

    let default = if after_type.trim_start().starts_with('=') {
        let expr_str = after_type.trim_start()[1..].trim();
        Some(crate::parser::expression::parse_expr(expr_str)?)
    } else {
        None
    };

    Ok(Statement::Declare(DeclareStmt {
        name: var_name,
        data_type: data_type_spec,
        default,
    }))
}

fn parse_type_from_declare(input: &str) -> Result<(DataTypeSpec, &str), DbError> {
    let trimmed = input.trim();
    let upper = trimmed.to_uppercase();

    let types: &[(&str, fn(u16) -> DataTypeSpec)] = &[
        ("VARCHAR(", DataTypeSpec::VarChar),
        ("NVARCHAR(", DataTypeSpec::NVarChar),
        ("CHAR(", DataTypeSpec::Char),
        ("NCHAR(", DataTypeSpec::NChar),
    ];

    for (prefix, constructor) in types {
        if upper.starts_with(prefix) {
            let close = upper.find(')').ok_or_else(|| {
                DbError::Parse(format!("missing ')' for {}", prefix.trim_end_matches('(')))
            })?;
            let len_str = &upper[prefix.len()..close];
            let len: u16 = len_str
                .parse()
                .map_err(|_| DbError::Parse("invalid type length".into()))?;
            let rest = &trimmed[close + 1..];
            return Ok((constructor(len), rest));
        }
    }

    if upper.starts_with("DECIMAL(") || upper.starts_with("NUMERIC(") {
        let close = upper
            .find(')')
            .ok_or_else(|| DbError::Parse("missing ')' for DECIMAL".into()))?;
        let inner = &upper[8..close];
        let parts: Vec<&str> = inner.split(',').map(|s| s.trim()).collect();
        let p: u8 = parts[0]
            .parse()
            .map_err(|_| DbError::Parse("invalid precision".into()))?;
        let s: u8 = if parts.len() > 1 {
            parts[1]
                .parse()
                .map_err(|_| DbError::Parse("invalid scale".into()))?
        } else {
            0
        };
        let rest = &trimmed[close + 1..];
        return Ok((DataTypeSpec::Decimal(p, s), rest));
    }

    let simple_types: &[(&str, DataTypeSpec)] = &[
        ("BIT", DataTypeSpec::Bit),
        ("TINYINT", DataTypeSpec::TinyInt),
        ("SMALLINT", DataTypeSpec::SmallInt),
        ("INT", DataTypeSpec::Int),
        ("BIGINT", DataTypeSpec::BigInt),
        ("DATE", DataTypeSpec::Date),
        ("TIME", DataTypeSpec::Time),
        ("DATETIME", DataTypeSpec::DateTime),
        ("DATETIME2", DataTypeSpec::DateTime2),
        ("UNIQUEIDENTIFIER", DataTypeSpec::UniqueIdentifier),
        ("SQL_VARIANT", DataTypeSpec::SqlVariant),
        ("VARCHAR", DataTypeSpec::VarChar(8000)),
        ("NVARCHAR", DataTypeSpec::NVarChar(4000)),
        ("DECIMAL", DataTypeSpec::Decimal(18, 0)),
    ];

    for (name, spec) in simple_types {
        if upper.starts_with(name) {
            let after = &trimmed[name.len()..];
            let next_char = after.chars().next();
            if next_char.is_none()
                || next_char.unwrap().is_whitespace()
                || next_char.unwrap() == '='
                || next_char.unwrap() == ';'
            {
                return Ok((spec.clone(), after));
            }
        }
    }

    Err(DbError::Parse(format!(
        "unsupported data type in DECLARE: '{}'",
        trimmed
    )))
}

pub(crate) fn parse_set(sql: &str) -> Result<Statement, DbError> {
    let after_set = sql["SET".len()..].trim();
    let upper = after_set.to_uppercase();

    if !after_set.contains('=') {
        return parse_set_option(after_set, &upper);
    }

    let eq_pos = after_set
        .find('=')
        .ok_or_else(|| DbError::Parse("SET requires '=' assignment".into()))?;
    let var_name = after_set[..eq_pos].trim().to_string();
    let expr_str = after_set[eq_pos + 1..].trim();
    let (processed, subquery_map) = extract_subqueries(expr_str);
    let mut expr = parse_expr_with_subqueries(&processed, &subquery_map)?;
    apply_subquery_map(&mut expr, &subquery_map);
    Ok(Statement::Set(SetStmt {
        name: var_name,
        expr,
    }))
}

fn parse_set_option(raw: &str, upper: &str) -> Result<Statement, DbError> {
    let mut split = raw.split_whitespace();
    let opt = split
        .next()
        .ok_or_else(|| DbError::Parse("SET requires option name".into()))?;
    let rest = raw[opt.len()..].trim();
    let opt_upper = opt.to_uppercase();

    let stmt = match opt_upper.as_str() {
        "ANSI_NULLS" => parse_set_bool_option(crate::ast::SessionOption::AnsiNulls, rest)?,
        "QUOTED_IDENTIFIER" => {
            parse_set_bool_option(crate::ast::SessionOption::QuotedIdentifier, rest)?
        }
        "NOCOUNT" => parse_set_bool_option(crate::ast::SessionOption::NoCount, rest)?,
        "XACT_ABORT" => parse_set_bool_option(crate::ast::SessionOption::XactAbort, rest)?,
        "DATEFIRST" => {
            if rest.is_empty() {
                return Err(DbError::Parse("SET DATEFIRST requires a value".into()));
            }
            let n = rest
                .parse::<i32>()
                .map_err(|_| DbError::Parse("SET DATEFIRST requires numeric value".into()))?;
            Statement::SetOption(crate::ast::SetOptionStmt {
                option: crate::ast::SessionOption::DateFirst,
                value: crate::ast::SessionOptionValue::Int(n),
            })
        }
        "LANGUAGE" => {
            if rest.is_empty() {
                return Err(DbError::Parse("SET LANGUAGE requires a value".into()));
            }
            Statement::SetOption(crate::ast::SetOptionStmt {
                option: crate::ast::SessionOption::Language,
                value: crate::ast::SessionOptionValue::Text(rest.to_string()),
            })
        }
        _ => {
            return Err(DbError::Parse(format!(
                "unsupported SET option '{}'",
                upper.split_whitespace().next().unwrap_or_default()
            )))
        }
    };
    Ok(stmt)
}

fn parse_set_bool_option(option: crate::ast::SessionOption, rest: &str) -> Result<Statement, DbError> {
    let value = rest.to_uppercase();
    let on = match value.as_str() {
        "ON" => true,
        "OFF" => false,
        _ => {
            let name = match option {
                crate::ast::SessionOption::AnsiNulls => "ANSI_NULLS",
                crate::ast::SessionOption::QuotedIdentifier => "QUOTED_IDENTIFIER",
                crate::ast::SessionOption::NoCount => "NOCOUNT",
                crate::ast::SessionOption::XactAbort => "XACT_ABORT",
                crate::ast::SessionOption::DateFirst => "DATEFIRST",
                crate::ast::SessionOption::Language => "LANGUAGE",
            };
            return Err(DbError::Parse(format!(
                "SET {} expects ON|OFF",
                name
            )))
        }
    };
    Ok(Statement::SetOption(crate::ast::SetOptionStmt {
        option,
        value: crate::ast::SessionOptionValue::Bool(on),
    }))
}

pub(crate) fn parse_if(sql: &str) -> Result<Statement, DbError> {
    let after_if = sql["IF".len()..].trim();

    let (begin_idx, else_idx) = find_if_blocks(after_if);

    let (condition_str, body_str, else_str) = if let Some(bi) = begin_idx {
        let cond = after_if[..bi].trim();
        let else_pos = else_idx.filter(|&ei| ei > bi);
        let body = if let Some(ei) = else_pos {
            &after_if[bi..ei]
        } else {
            &after_if[bi..]
        };
        let else_body = else_pos.map(|ei| &after_if[ei + "ELSE".len()..]);
        (cond, body, else_body)
    } else if let Some(ei) = else_idx {
        let cond = after_if[..ei].trim();
        let body_start = cond.len();
        (
            cond,
            &after_if[body_start..ei],
            Some(&after_if[ei + "ELSE".len()..]),
        )
    } else {
        return Err(DbError::Parse(
            "IF requires BEGIN...END blocks (use: IF condition BEGIN ... END)".into(),
        ));
    };

    let condition = crate::parser::expression::parse_expr(condition_str)?;
    let then_body = if body_str.trim().to_uppercase().starts_with("BEGIN") {
        parse_begin_end_body_with_end(body_str, find_body_end)?
    } else {
        crate::parser::parse_batch(body_str)?
    };

    let else_body = else_str
        .map(|s| {
            let s = s.trim();
            if s.to_uppercase().starts_with("BEGIN") {
                parse_begin_end_body(s)
            } else {
                super::super::parse_batch(s)
            }
        })
        .transpose()?;

    Ok(Statement::If(IfStmt {
        condition,
        then_body,
        else_body,
    }))
}

pub(crate) fn parse_while(sql: &str) -> Result<Statement, DbError> {
    let after_while = sql["WHILE".len()..].trim();
    let begin_idx = find_top_level_begin(after_while)
        .ok_or_else(|| DbError::Parse("WHILE requires BEGIN...END body".into()))?;
    let condition_str = after_while[..begin_idx].trim();
    let body_str = &after_while[begin_idx..];

    let condition = crate::parser::expression::parse_expr(condition_str)?;
    let body = parse_begin_end_body(body_str)?;

    Ok(Statement::While(WhileStmt { condition, body }))
}

pub(crate) fn parse_begin_end(sql: &str) -> Result<Statement, DbError> {
    let body = parse_begin_end_body(sql)?;
    Ok(Statement::BeginEnd(body))
}

fn parse_begin_end_body(sql: &str) -> Result<Vec<Statement>, DbError> {
    let trimmed = sql.trim();
    let upper = trimmed.to_uppercase();
    if !upper.starts_with("BEGIN") {
        return Err(DbError::Parse("expected BEGIN".into()));
    }
    let rest = trimmed["BEGIN".len()..].trim();
    let end_idx = find_matching_end(rest)?;
    let body_str = rest[..end_idx].trim();
    super::super::parse_batch(body_str)
}

fn parse_begin_end_body_with_end<F>(sql: &str, end_fn: F) -> Result<Vec<Statement>, DbError>
where
    F: Fn(&str) -> Result<usize, DbError>,
{
    let trimmed = sql.trim();
    let upper = trimmed.to_uppercase();
    if !upper.starts_with("BEGIN") {
        return Err(DbError::Parse("expected BEGIN".into()));
    }
    let rest = trimmed["BEGIN".len()..].trim();
    let end_idx = end_fn(rest)?;
    let body_str = rest[..end_idx].trim();
    super::super::parse_batch(body_str)
}

fn find_matching_end(input: &str) -> Result<usize, DbError> {
    let upper = input.to_uppercase();
    let mut depth = 0usize;
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        let remaining = chars.len() - i;
        if remaining >= 5
            && &upper[i..i + 5] == "BEGIN"
            && (i == 0 || !chars[i - 1].is_alphanumeric())
            && (i + 5 == chars.len() || !chars[i + 5].is_alphanumeric())
        {
            depth += 1;
            i += 5;
            continue;
        }
        if remaining >= 3
            && &upper[i..i + 3] == "END"
            && (i == 0 || !chars[i - 1].is_alphanumeric())
            && (i + 3 == chars.len() || !chars[i + 3].is_alphanumeric())
        {
            if depth == 0 {
                return Ok(i);
            }
            depth -= 1;
            i += 3;
            continue;
        }
        i += 1;
    }

    Err(DbError::Parse("missing END".into()))
}

fn find_body_end(input: &str) -> Result<usize, DbError> {
    let upper = input.to_uppercase();
    let mut depth = 0usize;
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        let remaining = chars.len() - i;
        if remaining >= 5
            && &upper[i..i + 5] == "BEGIN"
            && (i == 0 || !chars[i - 1].is_alphanumeric())
            && (i + 5 == chars.len() || !chars[i + 5].is_alphanumeric())
        {
            depth += 1;
            i += 5;
            continue;
        }
        if remaining >= 3
            && &upper[i..i + 3] == "END"
            && (i == 0 || !chars[i - 1].is_alphanumeric())
            && (i + 3 == chars.len() || !chars[i + 3].is_alphanumeric())
        {
            if depth == 0 {
                return Ok(i);
            }
            depth -= 1;
            i += 3;
            continue;
        }
        i += 1;
    }

    Err(DbError::Parse("missing END".into()))
}

pub(crate) fn parse_exec(sql: &str) -> Result<Statement, DbError> {
    let upper = sql.to_uppercase();
    let after_exec = if upper.starts_with("EXECUTE ") {
        sql["EXECUTE".len()..].trim()
    } else {
        sql["EXEC".len()..].trim()
    };
    if after_exec.is_empty() {
        return Err(DbError::Parse("EXEC requires arguments".into()));
    }

    if after_exec.starts_with('\'') || after_exec.starts_with("N'") || after_exec.starts_with("n'")
    {
        let expr = crate::parser::expression::parse_expr(after_exec)?;
        return Ok(Statement::ExecDynamic(ExecStmt { sql_expr: expr }));
    }

    let mut split_at = after_exec
        .find(char::is_whitespace)
        .unwrap_or(after_exec.len());
    if split_at == 0 {
        split_at = after_exec.len();
    }
    let proc_name = after_exec[..split_at].trim();
    let remainder = after_exec[split_at..].trim();

    if proc_name.eq_ignore_ascii_case("sp_executesql") {
        let args_parts = split_csv_top_level(remainder);
        if args_parts.is_empty() {
            return Err(DbError::Parse(
                "sp_executesql requires at least @stmt argument".into(),
            ));
        }
        let sql_expr = crate::parser::expression::parse_expr(args_parts[0].trim())?;
        let params_def = if args_parts.len() >= 2 {
            Some(crate::parser::expression::parse_expr(args_parts[1].trim())?)
        } else {
            None
        };
        let mut args = Vec::new();
        for p in args_parts.into_iter().skip(2) {
            args.push(parse_exec_argument(p.trim())?);
        }
        return Ok(Statement::SpExecuteSql(SpExecuteSqlStmt {
            sql_expr,
            params_def,
            args,
        }));
    }
    let name = parse_object_name(proc_name);
    let mut args = Vec::new();
    if !remainder.is_empty() {
        for p in split_csv_top_level(remainder) {
            args.push(parse_exec_argument(p.trim())?);
        }
    }
    Ok(Statement::ExecProcedure(ExecProcedureStmt { name, args }))
}

fn parse_exec_argument(input: &str) -> Result<ExecArgument, DbError> {
    let upper = input.to_uppercase();
    let is_output = upper.ends_with(" OUTPUT");
    let raw = if is_output {
        input[..input.len() - " OUTPUT".len()].trim()
    } else {
        input
    };
    if let Some(eq) = raw.find('=') {
        let name = raw[..eq].trim().to_string();
        let expr = crate::parser::expression::parse_expr(raw[eq + 1..].trim())?;
        return Ok(ExecArgument {
            name: Some(name),
            expr,
            is_output,
        });
    }
    let expr = crate::parser::expression::parse_expr(raw.trim())?;
    Ok(ExecArgument {
        name: None,
        expr,
        is_output,
    })
}

pub(crate) fn parse_create_procedure(sql: &str) -> Result<Statement, DbError> {
    let after = sql["CREATE PROCEDURE".len()..].trim();
    let as_idx = find_keyword_top_level(after, "AS")
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
        parse_begin_end_body(body)?
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
    let rest = after[close + 1..].trim();
    let returns_idx = find_keyword_top_level(rest, "RETURNS")
        .ok_or_else(|| DbError::Parse("CREATE FUNCTION missing RETURNS".into()))?;
    let after_returns = rest[returns_idx + "RETURNS".len()..].trim();

    if after_returns.to_uppercase().starts_with("TABLE") {
        let as_idx = find_keyword_top_level(after_returns, "AS")
            .ok_or_else(|| DbError::Parse("inline TVF missing AS".into()))?;
        let after_as = after_returns[as_idx + "AS".len()..].trim();
        let ret_idx = find_keyword_top_level(after_as, "RETURN")
            .ok_or_else(|| DbError::Parse("inline TVF missing RETURN".into()))?;
        let query_raw = after_as[ret_idx + "RETURN".len()..].trim();
        let query_text = query_raw
            .trim_start_matches('(')
            .trim_end_matches(')')
            .trim();
        let query = match super::select::parse_select(query_text)? {
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

    let as_idx = find_keyword_top_level(after_returns, "AS")
        .ok_or_else(|| DbError::Parse("scalar UDF missing AS".into()))?;
    let type_raw = after_returns[..as_idx].trim();
    let returns = Some(parse_data_type_inline(type_raw)?);
    let body_raw = after_returns[as_idx + "AS".len()..].trim();
    let body_stmts = if body_raw.to_uppercase().starts_with("BEGIN") {
        parse_begin_end_body(body_raw)?
    } else {
        crate::parser::parse_batch(body_raw)?
    };
    let mut return_expr = None;
    for stmt in body_stmts {
        if let Statement::Return(Some(expr)) = stmt {
            return_expr = Some(expr);
            break;
        }
    }
    let expr = return_expr
        .ok_or_else(|| DbError::Parse("scalar function body must contain RETURN <expr>".into()))?;
    Ok(Statement::CreateFunction(CreateFunctionStmt {
        name,
        params,
        returns,
        body: FunctionBody::ScalarReturn(expr),
    }))
}

fn find_matching_paren_index(input: &str, open_idx: usize) -> Option<usize> {
    let chars: Vec<char> = input.chars().collect();
    let mut depth = 0usize;
    let mut in_string = false;
    for (i, ch) in chars.iter().enumerate().skip(open_idx) {
        match *ch {
            '\'' => in_string = !in_string,
            '(' if !in_string => depth += 1,
            ')' if !in_string => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

pub(crate) fn parse_drop_function(sql: &str) -> Result<Statement, DbError> {
    let name = parse_object_name(sql["DROP FUNCTION".len()..].trim());
    Ok(Statement::DropFunction(DropFunctionStmt { name }))
}

fn parse_routine_params(input: &str) -> Result<Vec<RoutineParam>, DbError> {
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
        let tokens = raw.split_whitespace().collect::<Vec<_>>();
        if tokens.len() < 2 {
            return Err(DbError::Parse("invalid routine parameter".into()));
        }
        let name = tokens[0].to_string();
        let mut idx = 1usize;
        let data_type = parse_data_type_inline(tokens[idx])?;
        idx += 1;
        let mut default = None;
        let mut is_output = false;
        while idx < tokens.len() {
            if tokens[idx].eq_ignore_ascii_case("=") {
                let expr_txt = tokens[idx + 1..].join(" ");
                default = Some(crate::parser::expression::parse_expr(&expr_txt)?);
                break;
            }
            if tokens[idx].eq_ignore_ascii_case("OUTPUT") {
                is_output = true;
            }
            idx += 1;
        }
        out.push(RoutineParam {
            name,
            data_type,
            is_output,
            default,
        });
    }
    Ok(out)
}

fn parse_data_type_inline(input: &str) -> Result<DataTypeSpec, DbError> {
    super::ddl::parse_data_type(input)
}

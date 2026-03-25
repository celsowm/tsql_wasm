use crate::ast::*;
use crate::error::DbError;
use crate::parser::utils::{parse_object_name, split_csv_top_level};

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

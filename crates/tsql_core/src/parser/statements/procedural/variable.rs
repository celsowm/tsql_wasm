use crate::ast::*;
use crate::error::DbError;
use crate::parser::utils::split_csv_top_level;

pub(crate) fn parse_declare(sql: &str) -> Result<Statement, DbError> {
    let after_declare = sql["DECLARE".len()..].trim();
    let upper = after_declare.to_uppercase();

    let parts: Vec<&str> = after_declare.split_whitespace().collect();
    if parts.len() >= 3 && parts[1].to_uppercase() == "CURSOR" {
        let name = parts[0].to_string();
        let for_pos = upper
            .find("FOR")
            .ok_or_else(|| DbError::Parse("DECLARE CURSOR missing FOR".into()))?;
        let query_sql = &after_declare[for_pos + 3..].trim();
        let query = match crate::parser::statements::select::parse_select(query_sql)? {
            Statement::Select(s) => s,
            _ => return Err(DbError::Parse("CURSOR FOR must be a SELECT".into())),
        };
        return Ok(Statement::DeclareCursor(DeclareCursorStmt { name, query }));
    }

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
                table_constraints.push(crate::parser::statements::ddl::parse_table_constraint(item)?);
            } else {
                columns.push(crate::parser::statements::ddl::parse_column_spec(item)?);
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
        let (processed, subquery_map) = crate::parser::statements::subquery_utils::extract_subqueries(expr_str);
        let mut expr = crate::parser::expression::parse_expr_with_subqueries(&processed, &subquery_map)?;
        crate::parser::statements::subquery_utils::apply_subquery_map(&mut expr, &subquery_map);
        Some(expr)
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
        ("BINARY(", DataTypeSpec::Binary),
        ("VARBINARY(", DataTypeSpec::VarBinary),
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

    if upper.starts_with("FLOAT(") {
        let close = upper
            .find(')')
            .ok_or_else(|| DbError::Parse("missing ')' for FLOAT".into()))?;
        let rest = &trimmed[close + 1..];
        return Ok((DataTypeSpec::Float, rest));
    }

    let simple_types: &[(&str, DataTypeSpec)] = &[  
        ("BIT", DataTypeSpec::Bit),
        ("TINYINT", DataTypeSpec::TinyInt),
        ("SMALLINT", DataTypeSpec::SmallInt),
        ("INT", DataTypeSpec::Int),
        ("BIGINT", DataTypeSpec::BigInt),
        ("FLOAT", DataTypeSpec::Float),
        ("REAL", DataTypeSpec::Float),
        ("MONEY", DataTypeSpec::Money),
        ("SMALLMONEY", DataTypeSpec::SmallMoney),
        ("DATE", DataTypeSpec::Date),
        ("TIME", DataTypeSpec::Time),
        ("DATETIME", DataTypeSpec::DateTime),
        ("DATETIME2", DataTypeSpec::DateTime2),
        ("UNIQUEIDENTIFIER", DataTypeSpec::UniqueIdentifier),
        ("SQL_VARIANT", DataTypeSpec::SqlVariant),
        ("VARCHAR", DataTypeSpec::VarChar(8000)),
        ("NVARCHAR", DataTypeSpec::NVarChar(4000)),
        ("SYSNAME", DataTypeSpec::NVarChar(128)),
        ("BINARY", DataTypeSpec::Binary(1)),
        ("VARBINARY", DataTypeSpec::VarBinary(8000)),
        ("DECIMAL", DataTypeSpec::Decimal(18, 0)),
    ];

    for (name, spec) in simple_types {
        if upper.starts_with(name) {
            let after = &trimmed[name.len()..];
            match after.chars().next() {
                None | Some(' ') | Some('\t') | Some('\n') | Some('\r') | Some('=') | Some(';') => {
                    return Ok((spec.clone(), after));
                }
                _ => {}
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

    if upper.starts_with("IDENTITY_INSERT ") {
        return parse_set_identity_insert(after_set);
    }

    if !after_set.contains('=') {
        return parse_set_option(after_set, &upper);
    }

    let eq_pos = after_set
        .find('=')
        .ok_or_else(|| DbError::Parse("SET requires '=' assignment".into()))?;
    let var_name = after_set[..eq_pos].trim().to_string();
    let expr_str = after_set[eq_pos + 1..].trim();
    let (processed, subquery_map) = crate::parser::statements::subquery_utils::extract_subqueries(expr_str);
    let mut expr = crate::parser::expression::parse_expr_with_subqueries(&processed, &subquery_map)?;
    crate::parser::statements::subquery_utils::apply_subquery_map(&mut expr, &subquery_map);
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
        "DATEFORMAT" => {
            if rest.is_empty() {
                return Err(DbError::Parse("SET DATEFORMAT requires a value".into()));
            }
            Statement::SetOption(crate::ast::SetOptionStmt {
                option: crate::ast::SessionOption::DateFormat,
                value: crate::ast::SessionOptionValue::Text(rest.to_string()),
            })
        }
        "LOCK_TIMEOUT" => {
            if rest.is_empty() {
                return Err(DbError::Parse("SET LOCK_TIMEOUT requires a value".into()));
            }
            let n = rest
                .parse::<i32>()
                .map_err(|_| DbError::Parse("SET LOCK_TIMEOUT requires numeric value".into()))?;
            Statement::SetOption(crate::ast::SetOptionStmt {
                option: crate::ast::SessionOption::LockTimeout,
                value: crate::ast::SessionOptionValue::Int(n),
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
                crate::ast::SessionOption::DateFormat => "DATEFORMAT",
                crate::ast::SessionOption::LockTimeout => "LOCK_TIMEOUT",
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

fn parse_set_identity_insert(raw: &str) -> Result<Statement, DbError> {
    let rest = raw["IDENTITY_INSERT".len()..].trim();
    let mut parts = rest.rsplitn(2, |c: char| c.is_whitespace());
    let on_off = parts
        .next()
        .ok_or_else(|| DbError::Parse("SET IDENTITY_INSERT requires table name and ON/OFF".into()))?
        .to_uppercase();
    let table_str = parts
        .next()
        .ok_or_else(|| DbError::Parse("SET IDENTITY_INSERT requires table name".into()))?
        .trim();

    let on = match on_off.as_str() {
        "ON" => true,
        "OFF" => false,
        _ => return Err(DbError::Parse("SET IDENTITY_INSERT expects ON or OFF".into())),
    };

    let table = crate::parser::utils::parse_object_name(table_str);
    Ok(Statement::SetIdentityInsert(crate::ast::SetIdentityInsertStmt { table, on }))
}

use serde::{Deserialize, Serialize};
use std::borrow::Cow;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Token<'a> {
    Keyword(Cow<'a, str>),
    Identifier(Cow<'a, str>),
    Variable(Cow<'a, str>),
    Number(f64),
    String(Cow<'a, str>), // Unescaped string
    Operator(Cow<'a, str>),
    LParen,
    RParen,
    Comma,
    Semicolon,
    Dot,
    Star,
    Tilde,
    BinaryLiteral(Cow<'a, str>),
    Go,
}

pub fn is_keyword(id: &str) -> bool {
    let keywords = [
        "SELECT", "FROM", "WHERE", "GROUP", "BY", "HAVING", "ORDER", "JOIN", "ON", 
        "AS", "IN", "IS", "NULL", "AND", "OR", "NOT", "CASE", "WHEN", "THEN", "ELSE", 
        "END", "CAST", "CONVERT", "IF", "BEGIN", "DECLARE", "SET", "EXEC", "EXECUTE", 
        "PRINT", "GO", "LIKE", "TOP", "DISTINCT", "INSERT", "UPDATE", "DELETE", "INTO", "VALUES", 
        "CREATE", "TABLE", "IDENTITY", "PRIMARY", "KEY", "DESC", "ASC",
        "WHILE", "BREAK", "CONTINUE", "RETURN", "TRAN", "TRANSACTION", 
        "COMMIT", "ROLLBACK", "SAVE", "EXISTS", "TRUNCATE", "DROP", "VIEW",
        "PROCEDURE", "FUNCTION", "TRIGGER", "INDEX", "SCHEMA", "TYPE", "MERGE",
        "OFFSET", "ROWS", "FETCH", "NEXT", "ONLY", "WITH", "USING", "MATCHED",
        "SOURCE", "THEN", "PROC", "OUTPUT", "OUT",
        "UNION", "INTERSECT", "EXCEPT", "ALL", "LOCK_TIMEOUT",
        "BETWEEN", "APPLY", "PIVOT", "UNPIVOT", "OVER", "PARTITION", "UNBOUNDED",
        "PRECEDING", "FOLLOWING", "CURRENT", "ROW", "TRY_CAST", "TRY_CONVERT",
        "RAISERROR", "TRY", "CATCH", "CURSOR", "OPEN", "CLOSE", "DEALLOCATE",
        "ALTER", "ADD", "CONSTRAINT", "REFERENCES", "DEFAULT", "CHECK", "UNIQUE",
        "FOREIGN", "NOLOCK", "ROWLOCK", "TABLOCK", "HOLDLOCK", "UPDLOCK", "XLOCK",
        "WITHIN", "GROUPS", "RANGE", "REAL", "MONEY", "SMALLMONEY",
        "CHAR", "NCHAR", "BINARY", "VARBINARY", "DATE", "TIME", "DATETIME",
        "DATETIME2", "DATETIMEOFFSET", "SMALLDATETIME", "UNIQUEIDENTIFIER",
        "XML", "IMAGE", "NTEXT", "SQL_VARIANT", "SYSNAME", "MAX", "COMPUTED",
        "FOR", "AFTER", "INSTEAD", "READUNCOMMITTED", "READCOMMITTED", "READPAST",
        "SERIALIZABLE", "SNAPSHOT", "NOEXPAND", "OUTER", "INNER", "LEFT", "RIGHT",
        "FULL", "CROSS", "DISTRIBUTED",
        "CASCADE", "ACTION", "NO", "COLUMN", "PRIOR", "LAST", "FIRST", "RETURNS",
        "ISOLATION", "LEVEL", "READ", "UNCOMMITTED", "COMMITTED", "REPEATABLE", "OF",
        "INT", "BIGINT", "SMALLINT", "TINYINT", "BIT", "FLOAT", "DECIMAL", "NUMERIC",
        "VARCHAR", "NVARCHAR", "VARBINARY", "TEXT", "NTEXT", "IMAGE", "XML",
        "ABSOLUTE", "RELATIVE", "ROUTINE", "COLLATION",
    ];
    let non_keywords = ["PARAMETER", "PARAMETERS", "SPECIFIC", "SPECIFIC_NAME", "PARAMETER_NAME", "DATA_TYPE", "PARAMETER_MODE", "COLUMN_NAME", "TABLE_NAME", "TABLE_SCHEMA", "TABLE_CATALOG", "TABLE_TYPE", "ORDINAL_POSITION", "ROUTINE_NAME", "ROUTINE_TYPE", "ROUTINE_SCHEMA", "SQL_DATA_ACCESS", "CHECK_OPTION", "IS_UPDATABLE", "ROUTINE_BODY", "IS_DETERMINISTIC", "SCHEMA_LEVEL_ROUTINE", "ROUTINE_DEFINITION", "INFORMATION", "INFORMATION_SCHEMA", "TABLES", "VIEWS", "COLUMNS", "ROUTINES", "PARAMETERS", "SCHEMATA", "TABLE_CONSTRAINTS", "CHECK_CONSTRAINTS", "REFERENTIAL_CONSTRAINTS", "KEY_COLUMN_USAGE", "CONSTRAINT_TABLE_USAGE", "CONSTRAINT_COLUMN_USAGE"];
    if non_keywords.iter().any(|&k| k.eq_ignore_ascii_case(id)) {
        return false;
    }
    keywords.iter().any(|&k| k.eq_ignore_ascii_case(id))
}

pub fn unescape_string(s: &str) -> String {
    let mut s_slice = s;
    if s_slice.starts_with('N') {
        s_slice = &s_slice[1..];
    }
    if s_slice.starts_with('\'') && s_slice.ends_with('\'') {
        s_slice = &s_slice[1..s_slice.len()-1];
    }
    s_slice.replace("''", "'")
}

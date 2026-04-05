use serde::{Deserialize, Serialize};
use std::fmt;
use std::hash::Hash;

macro_rules! define_keywords {
    (
        $(
            $(#[$cat:meta])*
            $variant:ident => $sql:expr
        ),* $(,)?
    ) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
        pub enum Keyword {
            $( $variant, )*
        }

        impl Keyword {
            pub fn parse(s: &str) -> Option<Keyword> {
                match s.to_uppercase().as_str() {
                    $( $sql => Some(Keyword::$variant), )*
                    _ => None,
                }
            }

            pub fn as_sql(&self) -> &'static str {
                match self {
                    $( Keyword::$variant => $sql, )*
                }
            }
        }

        impl fmt::Display for Keyword {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_sql())
            }
        }

        impl AsRef<str> for Keyword {
            fn as_ref(&self) -> &str {
                self.as_sql()
            }
        }
    };
}

define_keywords! {
    // DML
    Select => "SELECT",
    From => "FROM",
    Where => "WHERE",
    Group => "GROUP",
    By => "BY",
    Having => "HAVING",
    Order => "ORDER",
    Join => "JOIN",
    On => "ON",
    As => "AS",
    In => "IN",
    Is => "IS",
    Null => "NULL",
    And => "AND",
    Or => "OR",
    Not => "NOT",
    Case => "CASE",
    When => "WHEN",
    Then => "THEN",
    Else => "ELSE",
    End => "END",
    Cast => "CAST",
    Convert => "CONVERT",
    Like => "LIKE",
    Top => "TOP",
    Distinct => "DISTINCT",
    Insert => "INSERT",
    Update => "UPDATE",
    Delete => "DELETE",
    Into => "INTO",
    Values => "VALUES",
    Exists => "EXISTS",
    Truncate => "TRUNCATE",
    Merge => "MERGE",
    Using => "USING",
    Matched => "MATCHED",
    Source => "SOURCE",
    Output => "OUTPUT",
    Out => "OUT",
    Union => "UNION",
    Intersect => "INTERSECT",
    Except => "EXCEPT",
    All => "ALL",
    Between => "BETWEEN",
    Apply => "APPLY",
    Pivot => "PIVOT",
    Unpivot => "UNPIVOT",
    Over => "OVER",
    Partition => "PARTITION",
    Unbounded => "UNBOUNDED",
    Preceding => "PRECEDING",
    Following => "FOLLOWING",
    Current => "CURRENT",
    Row => "ROW",
    TryCast => "TRY_CAST",
    TryConvert => "TRY_CONVERT",
    Offset => "OFFSET",
    Rows => "ROWS",
    Fetch => "FETCH",
    Next => "NEXT",
    Only => "ONLY",
    With => "WITH",
    Within => "WITHIN",
    Groups => "GROUPS",
    Range => "RANGE",

    // DDL
    Create => "CREATE",
    Table => "TABLE",
    Identity => "IDENTITY",
    Primary => "PRIMARY",
    Key => "KEY",
    Desc => "DESC",
    Asc => "ASC",
    Alter => "ALTER",
    Add => "ADD",
    Constraint => "CONSTRAINT",
    References => "REFERENCES",
    Default => "DEFAULT",
    Check => "CHECK",
    Unique => "UNIQUE",
    Foreign => "FOREIGN",
    Drop => "DROP",
    View => "VIEW",
    Procedure => "PROCEDURE",
    Function => "FUNCTION",
    Trigger => "TRIGGER",
    Index => "INDEX",
    Schema => "SCHEMA",
    Type => "TYPE",
    Column => "COLUMN",
    Proc => "PROC",

    // Control flow
    If => "IF",
    Begin => "BEGIN",
    Declare => "DECLARE",
    Set => "SET",
    Exec => "EXEC",
    Execute => "EXECUTE",
    Print => "PRINT",
    Go => "GO",
    While => "WHILE",
    Break => "BREAK",
    Continue => "CONTINUE",
    Return => "RETURN",
    Try => "TRY",
    Catch => "CATCH",
    RaiseError => "RAISERROR",

    // Transactions
    Tran => "TRAN",
    Transaction => "TRANSACTION",
    Commit => "COMMIT",
    Rollback => "ROLLBACK",
    Save => "SAVE",
    Mark => "MARK",
    Distributed => "DISTRIBUTED",

    // Cursors
    Cursor => "CURSOR",
    Open => "OPEN",
    Close => "CLOSE",
    Deallocate => "DEALLOCATE",

    // Locking hints
    Nolock => "NOLOCK",
    Rowlock => "ROWLOCK",
    Tablock => "TABLOCK",
    Holdlock => "HOLDLOCK",
    Updlock => "UPDLOCK",
    Xlock => "XLOCK",
    Readuncommitted => "READUNCOMMITTED",
    Readcommitted => "READCOMMITTED",
    Readpast => "READPAST",
    Serializable => "SERIALIZABLE",
    Snapshot => "SNAPSHOT",
    Noexpand => "NOEXPAND",

    // Join types
    Outer => "OUTER",
    Inner => "INNER",
    Left => "LEFT",
    Right => "RIGHT",
    Full => "FULL",
    Cross => "CROSS",

    // Data types - integer
    Int => "INT",
    BigInt => "BIGINT",
    SmallInt => "SMALLINT",
    TinyInt => "TINYINT",
    Bit => "BIT",

    // Data types - floating point / decimal
    Float => "FLOAT",
    Decimal => "DECIMAL",
    Numeric => "NUMERIC",
    Real => "REAL",
    Money => "MONEY",
    SmallMoney => "SMALLMONEY",

    // Data types - character
    Char => "CHAR",
    NChar => "NCHAR",
    Varchar => "VARCHAR",
    Nvarchar => "NVARCHAR",
    Text => "TEXT",
    NText => "NTEXT",

    // Data types - binary
    Binary => "BINARY",
    Varbinary => "VARBINARY",
    Image => "IMAGE",

    // Data types - date/time
    Date => "DATE",
    Time => "TIME",
    DateTime => "DATETIME",
    DateTime2 => "DATETIME2",
    DateTimeOffset => "DATETIMEOFFSET",
    SmallDateTime => "SMALLDATETIME",

    // Data types - other
    UniqueIdentifier => "UNIQUEIDENTIFIER",
    Xml => "XML",
    SqlVariant => "SQL_VARIANT",
    SysName => "SYSNAME",
    Max => "MAX",
    Computed => "COMPUTED",

    // Triggers
    For => "FOR",
    After => "AFTER",
    Instead => "INSTEAD",

    // Cascade / referential
    Cascade => "CASCADE",
    Action => "ACTION",
    No => "NO",
    Prior => "PRIOR",

    // Function / routine metadata
    Returns => "RETURNS",
    Absolute => "ABSOLUTE",
    Relative => "RELATIVE",
    Routine => "ROUTINE",
    Collation => "COLLATION",

    // Isolation level
    Isolation => "ISOLATION",
    Level => "LEVEL",
    Read => "READ",
    Uncommitted => "UNCOMMITTED",
    Committed => "COMMITTED",
    Repeatable => "REPEATABLE",
    Of => "OF",
    Off => "OFF",

    // Lock timeout
    LockTimeout => "LOCK_TIMEOUT",

    // SET options
    NoCount => "NOCOUNT",

    // DML pseudo-tables
    Inserted => "INSERTED",
    Deleted => "DELETED",

    // Misc
    Last => "LAST",
    First => "FIRST",
}

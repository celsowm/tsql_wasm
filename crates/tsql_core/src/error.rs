use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorClass {
    Parse,
    Semantic,
    Execution,
    Storage,
}

#[derive(Debug, Clone, Error)]
pub enum DbError {
    #[error("parse error: {0}")]
    Parse(String),

    #[error("semantic error: {0}")]
    Semantic(String),

    #[error("execution error: {0}")]
    Execution(String),

    #[error("storage error: {0}")]
    Storage(String),

    #[error("deadlock: {0}")]
    Deadlock(String),

    /// A custom error with an explicit SQL Server–style class and number.
    /// Allows callers to raise domain-specific errors (e.g. timeout, permission)
    /// without modifying the DbError enum itself.
    #[error("error {class}/{number}: {message}")]
    Custom {
        class: u8,
        number: i32,
        message: String,
    },

    // -- Strongly-typed semantic errors --
    #[error("schema '{schema}' not found")]
    SchemaNotFound { schema: String },

    #[error("table '{schema}.{table}' not found")]
    TableNotFound { schema: String, table: String },

    #[error("column '{column}' not found")]
    ColumnNotFound { column: String },

    #[error("column '{column}' not found in table '{table}'")]
    ColumnNotFoundQualified { table: String, column: String },

    #[error("type mismatch: expected {expected}, found {found}")]
    TypeMismatch { expected: String, found: String },

    #[error("index '{index}' not found on table '{table}'")]
    IndexNotFound { table: String, index: String },

    #[error("primary key not found on table '{table}'")]
    PrimaryKeyNotFound { table: String },

    #[error("constraint '{constraint}' not found on table '{table}'")]
    ConstraintNotFound { table: String, constraint: String },

    #[error("database '{database}' not found")]
    DatabaseNotFound { database: String },

    #[error("object '{object}' not found")]
    ObjectNotFound { object: String },

    #[error("column '{column}' already exists in table")]
    DuplicateColumn { column: String },

    #[error("table '{table}' already exists in schema '{schema}'")]
    DuplicateTable { schema: String, table: String },

    #[error("invalid identifier: '{identifier}'")]
    InvalidIdentifier { identifier: String },

    #[error("trigger '{trigger}' not found in schema '{schema}'")]
    TriggerNotFound { schema: String, trigger: String },

    #[error("trigger '{trigger}' already exists in schema '{schema}'")]
    DuplicateTrigger { schema: String, trigger: String },

    #[error("view '{schema}.{view}' not found")]
    ViewNotFound { schema: String, view: String },

    #[error("view '{view}' already exists in schema '{schema}'")]
    DuplicateView { schema: String, view: String },

    #[error("type '{schema}.{type_name}' not found")]
    TypeNotFound { schema: String, type_name: String },

    #[error("type '{type_name}' already exists in schema '{schema}'")]
    DuplicateType { schema: String, type_name: String },

    #[error("schema '{schema}' already exists")]
    DuplicateSchema { schema: String },

    #[error("cursor '{cursor}' not declared")]
    CursorNotDeclared { cursor: String },

    #[error("cursor '{cursor}' has no query")]
    CursorHasNoQuery { cursor: String },
}

impl DbError {
    pub fn class(&self) -> ErrorClass {
        match self {
            DbError::Parse(_) => ErrorClass::Parse,
            DbError::Semantic(_)
            | DbError::SchemaNotFound { .. }
            | DbError::TableNotFound { .. }
            | DbError::ColumnNotFound { .. }
            | DbError::ColumnNotFoundQualified { .. }
            | DbError::TypeMismatch { .. }
            | DbError::IndexNotFound { .. }
            | DbError::PrimaryKeyNotFound { .. }
            | DbError::ConstraintNotFound { .. }
            | DbError::DatabaseNotFound { .. }
            | DbError::ObjectNotFound { .. }
            | DbError::DuplicateColumn { .. }
            | DbError::DuplicateTable { .. }
            | DbError::InvalidIdentifier { .. }
            | DbError::TriggerNotFound { .. }
            | DbError::DuplicateTrigger { .. }
            | DbError::ViewNotFound { .. }
            | DbError::DuplicateView { .. }
            | DbError::TypeNotFound { .. }
            | DbError::DuplicateType { .. }
            | DbError::DuplicateSchema { .. }
            | DbError::CursorNotDeclared { .. }
            | DbError::CursorHasNoQuery { .. } => ErrorClass::Semantic,
            DbError::Execution(_) | DbError::Deadlock(_) | DbError::Custom { .. } => {
                ErrorClass::Execution
            }
            DbError::Storage(_) => ErrorClass::Storage,
        }
    }

    pub fn number(&self) -> i32 {
        match self {
            DbError::Parse(_) => 102,
            DbError::Semantic(_)
            | DbError::SchemaNotFound { .. }
            | DbError::TableNotFound { .. }
            | DbError::ColumnNotFound { .. }
            | DbError::ColumnNotFoundQualified { .. }
            | DbError::TypeMismatch { .. }
            | DbError::IndexNotFound { .. }
            | DbError::PrimaryKeyNotFound { .. }
            | DbError::ConstraintNotFound { .. }
            | DbError::DatabaseNotFound { .. }
            | DbError::ObjectNotFound { .. }
            | DbError::DuplicateColumn { .. }
            | DbError::DuplicateTable { .. }
            | DbError::InvalidIdentifier { .. }
            | DbError::TriggerNotFound { .. }
            | DbError::DuplicateTrigger { .. }
            | DbError::ViewNotFound { .. }
            | DbError::DuplicateView { .. }
            | DbError::TypeNotFound { .. }
            | DbError::DuplicateType { .. }
            | DbError::DuplicateSchema { .. }
            | DbError::CursorNotDeclared { .. }
            | DbError::CursorHasNoQuery { .. } => 207,
            DbError::Execution(_) => 50000,
            DbError::Storage(_) => 50001,
            DbError::Deadlock(_) => 1205,
            DbError::Custom { number, .. } => *number,
        }
    }

    pub fn code(&self) -> &'static str {
        match self {
            DbError::Parse(_) => "TSQL_PARSE_ERROR",
            DbError::Semantic(_) => "TSQL_SEMANTIC_ERROR",
            DbError::SchemaNotFound { .. } => "TSQL_SCHEMA_NOT_FOUND",
            DbError::TableNotFound { .. } => "TSQL_TABLE_NOT_FOUND",
            DbError::ColumnNotFound { .. } => "TSQL_COLUMN_NOT_FOUND",
            DbError::ColumnNotFoundQualified { .. } => "TSQL_COLUMN_NOT_FOUND_QUALIFIED",
            DbError::TypeMismatch { .. } => "TSQL_TYPE_MISMATCH",
            DbError::IndexNotFound { .. } => "TSQL_INDEX_NOT_FOUND",
            DbError::PrimaryKeyNotFound { .. } => "TSQL_PRIMARY_KEY_NOT_FOUND",
            DbError::ConstraintNotFound { .. } => "TSQL_CONSTRAINT_NOT_FOUND",
            DbError::DatabaseNotFound { .. } => "TSQL_DATABASE_NOT_FOUND",
            DbError::ObjectNotFound { .. } => "TSQL_OBJECT_NOT_FOUND",
            DbError::DuplicateColumn { .. } => "TSQL_DUPLICATE_COLUMN",
            DbError::DuplicateTable { .. } => "TSQL_DUPLICATE_TABLE",
            DbError::InvalidIdentifier { .. } => "TSQL_INVALID_IDENTIFIER",
            DbError::TriggerNotFound { .. } => "TSQL_TRIGGER_NOT_FOUND",
            DbError::DuplicateTrigger { .. } => "TSQL_DUPLICATE_TRIGGER",
            DbError::ViewNotFound { .. } => "TSQL_VIEW_NOT_FOUND",
            DbError::DuplicateView { .. } => "TSQL_DUPLICATE_VIEW",
            DbError::TypeNotFound { .. } => "TSQL_TYPE_NOT_FOUND",
            DbError::DuplicateType { .. } => "TSQL_DUPLICATE_TYPE",
            DbError::DuplicateSchema { .. } => "TSQL_DUPLICATE_SCHEMA",
            DbError::CursorNotDeclared { .. } => "TSQL_CURSOR_NOT_DECLARED",
            DbError::CursorHasNoQuery { .. } => "TSQL_CURSOR_HAS_NO_QUERY",
            DbError::Execution(_) => "TSQL_EXECUTION_ERROR",
            DbError::Storage(_) => "TSQL_STORAGE_ERROR",
            DbError::Deadlock(_) => "TSQL_DEADLOCK_ERROR",
            DbError::Custom { .. } => "TSQL_CUSTOM_ERROR",
        }
    }

    // -- Strongly-typed constructors --
    pub fn schema_not_found(schema: impl Into<String>) -> Self {
        DbError::SchemaNotFound {
            schema: schema.into(),
        }
    }

    pub fn table_not_found(schema: impl Into<String>, table: impl Into<String>) -> Self {
        DbError::TableNotFound {
            schema: schema.into(),
            table: table.into(),
        }
    }

    pub fn column_not_found(column: impl Into<String>) -> Self {
        DbError::ColumnNotFound {
            column: column.into(),
        }
    }

    pub fn column_not_found_qualified(table: impl Into<String>, column: impl Into<String>) -> Self {
        DbError::ColumnNotFoundQualified {
            table: table.into(),
            column: column.into(),
        }
    }

    pub fn type_mismatch(expected: impl Into<String>, found: impl Into<String>) -> Self {
        DbError::TypeMismatch {
            expected: expected.into(),
            found: found.into(),
        }
    }

    pub fn index_not_found(table: impl Into<String>, index: impl Into<String>) -> Self {
        DbError::IndexNotFound {
            table: table.into(),
            index: index.into(),
        }
    }

    pub fn primary_key_not_found(table: impl Into<String>) -> Self {
        DbError::PrimaryKeyNotFound {
            table: table.into(),
        }
    }

    pub fn constraint_not_found(table: impl Into<String>, constraint: impl Into<String>) -> Self {
        DbError::ConstraintNotFound {
            table: table.into(),
            constraint: constraint.into(),
        }
    }

    pub fn database_not_found(database: impl Into<String>) -> Self {
        DbError::DatabaseNotFound {
            database: database.into(),
        }
    }

    pub fn object_not_found(object: impl Into<String>) -> Self {
        DbError::ObjectNotFound {
            object: object.into(),
        }
    }

    pub fn duplicate_column(column: impl Into<String>) -> Self {
        DbError::DuplicateColumn {
            column: column.into(),
        }
    }

    pub fn duplicate_table(schema: impl Into<String>, table: impl Into<String>) -> Self {
        DbError::DuplicateTable {
            schema: schema.into(),
            table: table.into(),
        }
    }

    pub fn invalid_identifier(identifier: impl Into<String>) -> Self {
        DbError::InvalidIdentifier {
            identifier: identifier.into(),
        }
    }

    pub fn trigger_not_found(schema: impl Into<String>, trigger: impl Into<String>) -> Self {
        DbError::TriggerNotFound {
            schema: schema.into(),
            trigger: trigger.into(),
        }
    }

    pub fn duplicate_trigger(schema: impl Into<String>, trigger: impl Into<String>) -> Self {
        DbError::DuplicateTrigger {
            schema: schema.into(),
            trigger: trigger.into(),
        }
    }

    pub fn view_not_found(schema: impl Into<String>, view: impl Into<String>) -> Self {
        DbError::ViewNotFound {
            schema: schema.into(),
            view: view.into(),
        }
    }

    pub fn duplicate_view(schema: impl Into<String>, view: impl Into<String>) -> Self {
        DbError::DuplicateView {
            schema: schema.into(),
            view: view.into(),
        }
    }

    pub fn type_not_found(schema: impl Into<String>, type_name: impl Into<String>) -> Self {
        DbError::TypeNotFound {
            schema: schema.into(),
            type_name: type_name.into(),
        }
    }

    pub fn duplicate_type(schema: impl Into<String>, type_name: impl Into<String>) -> Self {
        DbError::DuplicateType {
            schema: schema.into(),
            type_name: type_name.into(),
        }
    }

    pub fn duplicate_schema(schema: impl Into<String>) -> Self {
        DbError::DuplicateSchema {
            schema: schema.into(),
        }
    }

    pub fn cursor_not_declared(cursor: impl Into<String>) -> Self {
        DbError::CursorNotDeclared {
            cursor: cursor.into(),
        }
    }

    pub fn cursor_has_no_query(cursor: impl Into<String>) -> Self {
        DbError::CursorHasNoQuery {
            cursor: cursor.into(),
        }
    }
}

/// Represents the outcome of executing a SQL statement.
/// Control flow signals (BREAK, CONTINUE, RETURN) are modeled on the success path
/// rather than as error variants, preventing accidental swallowing by catch-all
/// error handlers and keeping TRY...CATCH semantics clean.
#[derive(Debug, Clone)]
pub enum StmtOutcome<T> {
    /// Statement completed normally.
    Ok(T),
    /// T-SQL BREAK statement inside a WHILE loop.
    Break,
    /// T-SQL CONTINUE statement inside a WHILE loop.
    Continue,
    /// T-SQL RETURN statement, optionally carrying a value (for functions/procedures).
    Return(Option<crate::types::Value>),
}

/// Convenience type alias for statement execution results.
pub type StmtResult<T> = Result<StmtOutcome<T>, DbError>;

impl<T> StmtOutcome<T> {
    /// Returns true if this is a normal completion (Ok).
    pub fn is_ok(&self) -> bool {
        matches!(self, StmtOutcome::Ok(_))
    }

    /// Returns true if this is any control flow signal.
    pub fn is_control_flow(&self) -> bool {
        !matches!(self, StmtOutcome::Ok(_))
    }

    /// Maps the Ok value using the given function, preserving control flow signals.
    pub fn map<U, F: FnOnce(T) -> U>(self, f: F) -> StmtOutcome<U> {
        match self {
            StmtOutcome::Ok(v) => StmtOutcome::Ok(f(v)),
            StmtOutcome::Break => StmtOutcome::Break,
            StmtOutcome::Continue => StmtOutcome::Continue,
            StmtOutcome::Return(v) => StmtOutcome::Return(v),
        }
    }

    /// Converts a StmtOutcome into a Result, treating control flow signals as errors.
    /// This is used at boundaries where control flow signals should not escape
    /// (e.g., top-level statement execution outside loops/functions).
    pub fn into_result(self) -> Result<T, DbError> {
        match self {
            StmtOutcome::Ok(v) => Ok(v),
            StmtOutcome::Break => Err(DbError::Execution("BREAK outside of WHILE".into())),
            StmtOutcome::Continue => Err(DbError::Execution("CONTINUE outside of WHILE".into())),
            StmtOutcome::Return(_) => Err(DbError::Execution(
                "RETURN outside of procedure/function".into(),
            )),
        }
    }

    /// Converts a StmtOutcome into a Result, swallowing RETURN signals as Ok(None).
    /// Used at procedure/function boundaries where RETURN is expected.
    pub fn into_result_swallow_return(self) -> Result<T, DbError>
    where
        T: Default,
    {
        match self {
            StmtOutcome::Ok(v) => Ok(v),
            StmtOutcome::Break => Err(DbError::Execution("BREAK outside of WHILE".into())),
            StmtOutcome::Continue => Err(DbError::Execution("CONTINUE outside of WHILE".into())),
            StmtOutcome::Return(_) => Ok(T::default()),
        }
    }

    /// Returns the inner value if Ok, or the provided default.
    pub fn unwrap_or(self, default: T) -> T {
        match self {
            StmtOutcome::Ok(v) => v,
            _ => default,
        }
    }
}

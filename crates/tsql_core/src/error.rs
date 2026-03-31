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
}

impl DbError {
    pub fn class(&self) -> ErrorClass {
        match self {
            DbError::Parse(_) => ErrorClass::Parse,
            DbError::Semantic(_) => ErrorClass::Semantic,
            DbError::Execution(_) => ErrorClass::Execution,
            DbError::Storage(_) => ErrorClass::Storage,
        }
    }

    pub fn code(&self) -> &'static str {
        match self {
            DbError::Parse(_) => "TSQL_PARSE_ERROR",
            DbError::Semantic(_) => "TSQL_SEMANTIC_ERROR",
            DbError::Execution(_) => "TSQL_EXECUTION_ERROR",
            DbError::Storage(_) => "TSQL_STORAGE_ERROR",
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

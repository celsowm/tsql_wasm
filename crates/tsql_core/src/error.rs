use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorClass {
    Parse,
    Semantic,
    Execution,
    Storage,
    ControlFlow,
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

    #[error("break")]
    Break,

    #[error("continue")]
    Continue,

    #[error("return")]
    Return(Option<crate::types::Value>),
}

impl DbError {
    pub fn class(&self) -> ErrorClass {
        match self {
            DbError::Parse(_) => ErrorClass::Parse,
            DbError::Semantic(_) => ErrorClass::Semantic,
            DbError::Execution(_) => ErrorClass::Execution,
            DbError::Storage(_) => ErrorClass::Storage,
            DbError::Break | DbError::Continue | DbError::Return(_) => ErrorClass::ControlFlow,
        }
    }

    pub fn code(&self) -> &'static str {
        match self {
            DbError::Parse(_) => "TSQL_PARSE_ERROR",
            DbError::Semantic(_) => "TSQL_SEMANTIC_ERROR",
            DbError::Execution(_) => "TSQL_EXECUTION_ERROR",
            DbError::Storage(_) => "TSQL_STORAGE_ERROR",
            DbError::Break => "TSQL_CONTROL_BREAK",
            DbError::Continue => "TSQL_CONTROL_CONTINUE",
            DbError::Return(_) => "TSQL_CONTROL_RETURN",
        }
    }
}

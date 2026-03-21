use thiserror::Error;

#[derive(Debug, Error)]
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

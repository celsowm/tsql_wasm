//! Playground mode for tsql-server
//!
//! Provides a seeded database with sample tables, views, and data
//! for testing SQL Server clients without requiring complex setup.

pub mod data;
pub mod schema;
pub mod views;

use tsql_core::{Database, StatementExecutor};

/// Seeds the database with playground schema and data
pub fn seed_playground(db: &Database) -> Result<(), tsql_core::DbError> {
    let session_id = db.create_session();

    // Execute schema creation
    for sql in schema::DDL_STATEMENTS {
        StatementExecutor::execute_session_batch_sql(db, session_id, sql)?;
    }

    // Execute views creation
    for sql in views::DDL_STATEMENTS {
        StatementExecutor::execute_session_batch_sql(db, session_id, sql)?;
    }

    // Insert sample data
    for sql in data::INSERT_STATEMENTS {
        StatementExecutor::execute_session_batch_sql(db, session_id, sql)?;
    }

    let _ = db.close_session(session_id);
    Ok(())
}

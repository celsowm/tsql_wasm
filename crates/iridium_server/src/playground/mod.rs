//! Playground mode for iridium-server
//!
//! Provides a seeded database with sample tables, views, and data
//! for testing SQL Server clients without requiring complex setup.

pub mod data;
pub mod schema;
pub mod views;

use iridium_core::{SessionManager, StatementExecutor};

/// Seeds the database with playground schema and data
pub fn seed_playground<D>(db: &D) -> Result<(), iridium_core::DbError>
where
    D: SessionManager + StatementExecutor,
{
    let session_id = db.create_session();

    // Execute schema creation
    for sql in schema::DDL_STATEMENTS {
        db.execute_session_batch_sql(session_id, sql)?;
    }

    // Execute views creation
    for sql in views::DDL_STATEMENTS {
        db.execute_session_batch_sql(session_id, sql)?;
    }

    // Insert sample data
    for sql in data::INSERT_STATEMENTS {
        db.execute_session_batch_sql(session_id, sql)?;
    }

    let _ = db.close_session(session_id);
    Ok(())
}


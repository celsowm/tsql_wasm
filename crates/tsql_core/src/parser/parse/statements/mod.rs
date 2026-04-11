pub mod alter;
pub mod control_flow;
pub mod create;
pub mod cursor;
pub mod ddl;
pub mod dml;
pub mod drop;
pub mod other;
pub mod query;
pub mod transaction;

pub use query::parse_multipart_name;

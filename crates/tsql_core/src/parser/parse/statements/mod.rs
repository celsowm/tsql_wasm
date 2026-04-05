pub mod query;
pub mod other;
pub mod dml;
pub mod ddl;
pub mod control_flow;
pub mod cursor;
pub mod transaction;
pub mod drop;
pub mod alter;
pub mod create;

pub use query::parse_multipart_name;

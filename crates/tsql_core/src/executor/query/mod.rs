pub(crate) mod binder;
pub(crate) mod binding;
pub(crate) mod from_tree;
pub(crate) mod executor;
pub(crate) mod finalize;
pub(crate) mod plan;
pub(crate) mod pipeline;
pub(crate) mod projection;
pub(crate) mod source;
pub(crate) mod transformer;
pub(crate) mod scan;

use crate::catalog::Catalog;
use crate::storage::Storage;

use super::clock::Clock;

pub struct QueryExecutor<'a> {
    pub catalog: &'a dyn Catalog,
    pub storage: &'a dyn Storage,
    pub clock: &'a dyn Clock,
}

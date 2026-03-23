mod delete;
mod insert;
pub(crate) mod output;
mod update;
pub(crate) mod validation;

pub(crate) use output::{build_output_result, build_output_result_merge, MergeOutputRow};

use crate::catalog::Catalog;
use crate::storage::Storage;

use super::clock::Clock;

pub(crate) struct MutationExecutor<'a> {
    pub(crate) catalog: &'a mut dyn Catalog,
    pub(crate) storage: &'a mut dyn Storage,
    pub(crate) clock: &'a dyn Clock,
}

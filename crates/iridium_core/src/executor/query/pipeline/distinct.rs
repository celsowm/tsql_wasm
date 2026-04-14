use crate::types::Value;

use crate::executor::projection::deduplicate_projected_rows;

pub(crate) fn deduplicate_rows(rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
    deduplicate_projected_rows(rows)
}

use crate::types::{DataType, Value};
use super::model::JoinedRow;

pub type Variables = std::collections::HashMap<String, (DataType, Value)>;

pub struct ExecutionContext<'a> {
    pub variables: &'a mut Variables,
    pub outer_row: Option<JoinedRow>,
    pub depth: usize,
}

impl<'a> ExecutionContext<'a> {
    pub fn new(variables: &'a mut Variables) -> Self {
        Self {
            variables,
            outer_row: None,
            depth: 0,
        }
    }

    pub fn subquery(&mut self) -> ExecutionContext<'_> {
        ExecutionContext {
            variables: self.variables,
            outer_row: self.outer_row.clone(),
            depth: self.depth + 1,
        }
    }

    pub fn with_outer_row(&mut self, row: JoinedRow) -> ExecutionContext<'_> {
        ExecutionContext {
            variables: self.variables,
            outer_row: Some(row),
            depth: self.depth + 1,
        }
    }
}

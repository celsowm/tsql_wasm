use crate::executor::context::RowContext;
use crate::types::Value;
use crate::executor::model::JoinedRow;

impl RowContext {
    pub fn fork(&self) -> Self {
        self.clone()
    }

    pub fn push_apply_row(&mut self, row: JoinedRow) {
        self.apply_stack.push(row);
    }

    pub fn pop_apply_row(&mut self) {
        self.apply_stack.pop();
    }

    pub fn get_window_value(&self, key: &str) -> Option<Value> {
        self.window_context.as_ref().and_then(|wc| wc.get(key))
    }
}

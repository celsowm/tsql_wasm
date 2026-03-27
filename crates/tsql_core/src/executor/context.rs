use std::cell::RefCell;
use std::collections::HashMap;

use super::cte::CteStorage;
use super::model::{JoinedRow, Cursor};
use crate::types::{DataType, Value};

pub type Variables = std::collections::HashMap<String, (DataType, Value)>;

#[derive(Debug, Clone)]
pub enum ControlFlow {
    Break,
    Continue,
    Return(Option<Value>),
}

pub struct ExecutionContext<'a> {
    pub variables: &'a mut Variables,
    pub outer_row: Option<JoinedRow>,
    pub depth: usize,
    pub ctes: CteStorage,
    pub loop_depth: usize,
    pub pending_control: RefCell<Option<ControlFlow>>,
    pub session_last_identity: &'a mut Option<i64>,
    pub scope_identity_stack: &'a mut Vec<Option<i64>>,
    pub table_vars: Vec<HashMap<String, String>>,
    pub temp_table_map: &'a mut HashMap<String, String>,
    pub session_table_var_map: &'a mut HashMap<String, String>,
    pub table_var_counter: &'a mut u64,
    pub scope_vars: Vec<Vec<String>>,
    pub ansi_nulls: bool,
    pub datefirst: i32,
    pub random_state: &'a mut u64,
    pub apply_row_stack: Vec<JoinedRow>,
    pub current_group: Option<super::model::Group>,
    pub window_context: Option<HashMap<crate::ast::Expr, Value>>,
    pub print_output: &'a mut Vec<String>,
    pub cursors: &'a mut HashMap<String, Cursor>,
    pub fetch_status: &'a mut i32,
    pub trigger_depth: usize,
    pub last_error: Option<DbError>,
}


impl<'a> ExecutionContext<'a> {
    pub fn new(
        variables: &'a mut Variables,
        session_last_identity: &'a mut Option<i64>,
        scope_identity_stack: &'a mut Vec<Option<i64>>,
        temp_table_map: &'a mut HashMap<String, String>,
        session_table_var_map: &'a mut HashMap<String, String>,
        table_var_counter: &'a mut u64,
        ansi_nulls: bool,
        datefirst: i32,
        random_state: &'a mut u64,
        cursors: &'a mut HashMap<String, Cursor>,
        fetch_status: &'a mut i32,
        print_output: &'a mut Vec<String>,
    ) -> Self {
        Self {
            variables,
            outer_row: None,
            depth: 0,
            ctes: CteStorage::new(),
            loop_depth: 0,
            pending_control: RefCell::new(None),
            session_last_identity,
            scope_identity_stack,
            table_vars: vec![HashMap::new()],
            temp_table_map,
            session_table_var_map,
            scope_vars: vec![vec![]],
            table_var_counter,
            ansi_nulls,
            datefirst,
            random_state,
            apply_row_stack: vec![],
            current_group: None,
            window_context: None,
            print_output,
            cursors,
            fetch_status,
            trigger_depth: 0,
            last_error: None,
        }
    }

    pub fn subquery(&mut self) -> ExecutionContext<'_> {
        ExecutionContext {
            variables: self.variables,
            outer_row: self.outer_row.clone(),
            depth: self.depth + 1,
            ctes: CteStorage::new(),
            loop_depth: self.loop_depth,
            pending_control: RefCell::new(None),
            session_last_identity: self.session_last_identity,
            scope_identity_stack: self.scope_identity_stack,
            table_vars: self.table_vars.clone(),
            temp_table_map: self.temp_table_map,
            session_table_var_map: self.session_table_var_map,
            scope_vars: self.scope_vars.clone(),
            table_var_counter: self.table_var_counter,
            ansi_nulls: self.ansi_nulls,
            datefirst: self.datefirst,
            random_state: self.random_state,
            apply_row_stack: self.apply_row_stack.clone(),
            current_group: self.current_group.clone(),
            window_context: self.window_context.clone(),
            print_output: self.print_output,
            cursors: self.cursors,
            fetch_status: self.fetch_status,
            trigger_depth: self.trigger_depth,
            last_error: self.last_error.clone(),
        }
    }

    pub fn with_outer_row(&mut self, row: JoinedRow) -> ExecutionContext<'_> {
        ExecutionContext {
            variables: self.variables,
            outer_row: Some(row),
            depth: self.depth + 1,
            ctes: CteStorage::new(),
            loop_depth: self.loop_depth,
            pending_control: RefCell::new(None),
            session_last_identity: self.session_last_identity,
            scope_identity_stack: self.scope_identity_stack,
            table_vars: self.table_vars.clone(),
            temp_table_map: self.temp_table_map,
            session_table_var_map: self.session_table_var_map,
            scope_vars: self.scope_vars.clone(),
            table_var_counter: self.table_var_counter,
            ansi_nulls: self.ansi_nulls,
            datefirst: self.datefirst,
            random_state: self.random_state,
            apply_row_stack: self.apply_row_stack.clone(),
            current_group: self.current_group.clone(),
            window_context: self.window_context.clone(),
            print_output: self.print_output,
            cursors: self.cursors,
            fetch_status: self.fetch_status,
        }
    }

    pub fn with_outer_row_extended(
        &mut self,
        _current_row: JoinedRow,
        outer_row: JoinedRow,
    ) -> ExecutionContext<'_> {
        ExecutionContext {
            variables: self.variables,
            outer_row: Some(outer_row),
            depth: self.depth + 1,
            ctes: CteStorage::new(),
            loop_depth: self.loop_depth,
            pending_control: RefCell::new(None),
            session_last_identity: self.session_last_identity,
            scope_identity_stack: self.scope_identity_stack,
            table_vars: self.table_vars.clone(),
            temp_table_map: self.temp_table_map,
            session_table_var_map: self.session_table_var_map,
            scope_vars: self.scope_vars.clone(),
            table_var_counter: self.table_var_counter,
            ansi_nulls: self.ansi_nulls,
            datefirst: self.datefirst,
            random_state: self.random_state,
            apply_row_stack: self.apply_row_stack.clone(),
            current_group: self.current_group.clone(),
            window_context: self.window_context.clone(),
            print_output: self.print_output,
            cursors: self.cursors,
            fetch_status: self.fetch_status,
        }
    }

    pub fn set_control(&self, cf: ControlFlow) {
        *self.pending_control.borrow_mut() = Some(cf);
    }

    pub fn take_control(&self) -> Option<ControlFlow> {
        self.pending_control.borrow_mut().take()
    }

    pub fn enter_scope(&mut self) {
        self.scope_vars.push(vec![]);
        self.table_vars.push(HashMap::new());
        self.scope_identity_stack.push(None);
    }

    pub fn leave_scope(&mut self) {
        let _ = self.leave_scope_collect_table_vars();
    }

    pub fn leave_scope_collect_table_vars(&mut self) -> Vec<String> {
        if let Some(vars) = self.scope_vars.pop() {
            for name in vars {
                self.variables.remove(&name);
            }
        }
        let mut dropped_physical = Vec::new();
        if self.table_vars.len() > 1 {
            if let Some(scope) = self.table_vars.pop() {
                for (logical, physical) in scope {
                    dropped_physical.push(physical.clone());
                    if self
                        .session_table_var_map
                        .get(&logical)
                        .map(|current| current.eq_ignore_ascii_case(&physical))
                        .unwrap_or(false)
                    {
                        let mut restored: Option<String> = None;
                        for outer in self.table_vars.iter().rev() {
                            if let Some(mapped) = outer.get(&logical) {
                                restored = Some(mapped.clone());
                                break;
                            }
                        }
                        if let Some(mapped) = restored {
                            self.session_table_var_map.insert(logical.clone(), mapped);
                        } else {
                            self.session_table_var_map.remove(&logical);
                        }
                    }
                }
            }
        }
        // Preserve identity value when leaving scope (propagate to parent)
        if let Some(val) = self.scope_identity_stack.pop() {
            if let Some(last) = self.scope_identity_stack.last_mut() {
                if val.is_some() {
                    *last = val;
                }
            }
        }
        dropped_physical
    }

    pub fn register_declared_var(&mut self, name: &str) {
        if let Some(scope) = self.scope_vars.last_mut() {
            scope.push(name.to_string());
        }
    }

    pub fn register_table_var(&mut self, logical_name: &str, physical_name: &str) {
        if let Some(scope) = self.table_vars.last_mut() {
            scope.insert(logical_name.to_uppercase(), physical_name.to_string());
        }
        self.session_table_var_map
            .insert(logical_name.to_uppercase(), physical_name.to_string());
    }

    pub fn resolve_table_name(&self, logical: &str) -> Option<String> {
        let mut upper = logical.to_uppercase();
        if upper.starts_with("DBO.") {
            upper = upper["DBO.".len()..].to_string();
        }

        // 1. Mapped names (temp tables #t and pseudo-tables like INSERTED)
        if let Some(mapped) = self.temp_table_map.get(&upper) {
            return Some(mapped.clone());
        }

        // 2. Table variables (@vars)
        if logical.starts_with('@') {
            for scope in self.table_vars.iter().rev() {
                if let Some(name) = scope.get(&upper) {
                    return Some(name.clone());
                }
            }
            if let Some(name) = self.session_table_var_map.get(&upper) {
                return Some(name.clone());
            }
            return None;
        }

        None
    }

    pub fn push_apply_row(&mut self, row: JoinedRow) {
        self.apply_row_stack.push(row);
    }

    pub fn pop_apply_row(&mut self) {
        self.apply_row_stack.pop();
    }

    pub fn set_last_identity(&mut self, val: i64) {
        *self.session_last_identity = Some(val);
        if let Some(last) = self.scope_identity_stack.last_mut() {
            *last = Some(val);
        } else {
            self.scope_identity_stack.push(Some(val));
        }
    }

    pub fn current_scope_identity(&self) -> Option<i64> {
        self.scope_identity_stack.last().and_then(|v| *v)
    }

    pub fn next_table_var_id(&mut self) -> u64 {
        *self.table_var_counter += 1;
        *self.table_var_counter
    }

    pub fn get_window_value(&self, expr: &crate::ast::Expr) -> Option<Value> {
        self.window_context.as_ref().and_then(|m| m.get(expr).cloned())
    }
}

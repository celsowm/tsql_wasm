use std::collections::{HashMap, HashSet};

use crate::executor::context::{FrameState, SessionStateRefs};
use crate::executor::string_norm::{normalize_identifier, strip_dbo_prefix};

impl FrameState {
    pub fn fork(&self) -> Self {
        self.clone()
    }

    pub fn enter_scope<'a>(&mut self, session: &mut SessionStateRefs<'a>) {
        self.scope_vars.push(vec![]);
        self.table_vars.push(HashMap::new());
        self.readonly_table_vars.push(HashSet::new());
        session.identity_stack.push(None);
    }

    pub fn leave_scope_collect_table_vars<'a>(
        &mut self,
        session: &mut SessionStateRefs<'a>,
    ) -> Vec<String> {
        if let Some(vars) = self.scope_vars.pop() {
            for name in vars {
                session.variables.remove(&name);
            }
        }

        let mut dropped_physical = Vec::new();
        if self.table_vars.len() > 1 {
            if let Some(scope) = self.table_vars.pop() {
                for (logical, physical) in scope {
                    dropped_physical.push(physical.clone());
                    if session
                        .var_map
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
                            session.var_map.insert(logical.clone(), mapped);
                        } else {
                            session.var_map.remove(&logical);
                        }
                    }
                }
            }
        }
        if self.readonly_table_vars.len() > 1 {
            let _ = self.readonly_table_vars.pop();
        }
        if let Some(val) = session.identity_stack.pop() {
            if let Some(last) = session.identity_stack.last_mut() {
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

    pub fn register_table_var<'a>(
        &mut self,
        session: &mut SessionStateRefs<'a>,
        logical_name: &str,
        physical_name: &str,
    ) {
        if let Some(scope) = self.table_vars.last_mut() {
            scope.insert(
                normalize_identifier(logical_name),
                physical_name.to_string(),
            );
        }
        session.var_map.insert(
            normalize_identifier(logical_name),
            physical_name.to_string(),
        );
    }

    pub fn mark_table_var_readonly(&mut self, logical_name: &str) {
        if let Some(scope) = self.readonly_table_vars.last_mut() {
            scope.insert(normalize_identifier(logical_name));
        }
    }

    pub fn is_readonly_table_var(&self, logical_name: &str) -> bool {
        let upper = normalize_identifier(strip_dbo_prefix(logical_name));
        self.readonly_table_vars
            .iter()
            .rev()
            .any(|scope| scope.contains(&upper))
    }

    pub fn resolve_table_name<'a>(
        &self,
        session: &SessionStateRefs<'a>,
        logical: &str,
    ) -> Option<String> {
        let upper = normalize_identifier(strip_dbo_prefix(logical));

        if let Some(mapped) = session.temp_map.get(&upper) {
            return Some(mapped.clone());
        }

        for scope in self.table_vars.iter().rev() {
            if let Some(name) = scope.get(&upper) {
                return Some(name.clone());
            }
        }
        if let Some(name) = session.var_map.get(&upper) {
            return Some(name.clone());
        }

        None
    }
}

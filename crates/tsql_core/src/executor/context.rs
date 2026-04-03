use std::collections::{HashMap, HashSet};

use super::cte::CteStorage;
use super::model::{Cursor, JoinedRow};
use crate::error::DbError;
use crate::types::{DataType, Value};

pub type Variables = std::collections::HashMap<String, (DataType, Value)>;

#[derive(Debug, Clone)]
pub enum ModuleKind {
    Procedure,
    Function,
    Trigger,
}

#[derive(Debug, Clone)]
pub struct ModuleFrame {
    pub object_id: i32,
    pub schema: String,
    pub name: String,
    pub kind: ModuleKind,
}


pub struct SessionStateRefs<'a> {
    pub variables: &'a mut Variables,
    pub last_identity: &'a mut Option<i64>,
    pub identity_stack: &'a mut Vec<Option<i64>>,
    pub temp_map: &'a mut HashMap<String, String>,
    pub var_map: &'a mut HashMap<String, String>,
    pub var_counter: &'a mut u64,
    pub random_state: &'a mut u64,
    pub cursors: &'a mut HashMap<String, Cursor>,
    pub fetch_status: &'a mut i32,
    pub print_output: &'a mut Vec<String>,
}

pub struct SessionMetadata {
    pub id: super::locks::SessionId,
    pub database: Option<String>,
    pub original_database: String,
    pub user: Option<String>,
    pub app_name: Option<String>,
    pub host_name: Option<String>,
}

pub struct FrameState {
    pub depth: usize,
    pub loop_depth: usize,
    pub trancount: u32,
    pub xact_state: i8,
    pub trigger_depth: usize,
    pub module_stack: Vec<ModuleFrame>,
    pub table_vars: Vec<HashMap<String, String>>,
    pub readonly_table_vars: Vec<HashSet<String>>,
    pub scope_vars: Vec<Vec<String>>,
}

pub struct RowContext {
    pub outer_row: Option<JoinedRow>,
    pub apply_stack: Vec<JoinedRow>,
    pub current_group: Option<super::model::Group>,
    pub window_context: Option<HashMap<crate::ast::Expr, Value>>,
}

pub struct ExecutionContext<'a> {
    pub session: SessionStateRefs<'a>,
    pub metadata: SessionMetadata,
    pub frame: FrameState,
    pub row: RowContext,
    pub ctes: CteStorage,
    pub ansi_nulls: bool,
    pub datefirst: i32,
    pub dirty_buffer: Option<std::sync::Arc<parking_lot::Mutex<super::dirty_buffer::DirtyBuffer>>>,
    pub identity_insert: HashSet<String>,
    pub skip_instead_of: bool,
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
        dirty_buffer: Option<std::sync::Arc<parking_lot::Mutex<super::dirty_buffer::DirtyBuffer>>>,
        session_id: super::locks::SessionId,
        session_original_database: String,
        user: Option<String>,
        app_name: Option<String>,
        host_name: Option<String>,
    ) -> Self {
        Self {
            session: SessionStateRefs {
                variables,
                last_identity: session_last_identity,
                identity_stack: scope_identity_stack,
                temp_map: temp_table_map,
                var_map: session_table_var_map,
                var_counter: table_var_counter,
                random_state,
                cursors,
                fetch_status,
                print_output,
            },
            metadata: SessionMetadata {
                id: session_id,
                database: None,
                original_database: session_original_database,
                user,
                app_name,
                host_name,
            },
            frame: FrameState {
                depth: 0,
                loop_depth: 0,
                trancount: 0,
                xact_state: 0,
                trigger_depth: 0,
                module_stack: vec![],
                scope_vars: vec![vec![]],
                table_vars: vec![HashMap::new()],
                readonly_table_vars: vec![HashSet::new()],
            },
            row: RowContext {
                outer_row: None,
                apply_stack: vec![],
                current_group: None,
                window_context: None,
            },
            ctes: CteStorage::new(),
            ansi_nulls,
            datefirst,
            dirty_buffer,
            identity_insert: HashSet::new(),
            skip_instead_of: false,
            last_error: None,
        }
    }

    // Delegation properties for backward compatibility (can be removed later)
    #[inline] pub fn variables(&self) -> &Variables { self.session.variables }
    #[inline] pub fn variables_mut(&mut self) -> &mut Variables { self.session.variables }
    #[inline] pub fn session_id(&self) -> super::locks::SessionId { self.metadata.id }
    #[inline] pub fn loop_depth(&self) -> usize { self.frame.loop_depth }
    #[inline] pub fn loop_depth_mut(&mut self) -> &mut usize { &mut self.frame.loop_depth }
    #[inline] pub fn trancount(&self) -> u32 { self.frame.trancount }
    #[inline] pub fn xact_state(&self) -> i8 { self.frame.xact_state }
    #[inline] pub fn trigger_depth(&self) -> usize { self.frame.trigger_depth }
    #[inline] pub fn trigger_depth_mut(&mut self) -> &mut usize { &mut self.frame.trigger_depth }
    #[inline] pub fn outer_row(&self) -> &Option<JoinedRow> { &self.row.outer_row }
    #[inline] pub fn outer_row_mut(&mut self) -> &mut Option<JoinedRow> { &mut self.row.outer_row }
    #[inline] pub fn current_group(&self) -> &Option<super::model::Group> { &self.row.current_group }
    #[inline] pub fn current_group_mut(&mut self) -> &mut Option<super::model::Group> { &mut self.row.current_group }

    pub fn subquery(&mut self) -> ExecutionContext<'_> {
        ExecutionContext {
            session: SessionStateRefs {
                variables: self.session.variables,
                last_identity: self.session.last_identity,
                identity_stack: self.session.identity_stack,
                temp_map: self.session.temp_map,
                var_map: self.session.var_map,
                var_counter: self.session.var_counter,
                random_state: self.session.random_state,
                cursors: self.session.cursors,
                fetch_status: self.session.fetch_status,
                print_output: self.session.print_output,
            },
            metadata: SessionMetadata {
                id: self.metadata.id,
                database: self.metadata.database.clone(),
                original_database: self.metadata.original_database.clone(),
                user: self.metadata.user.clone(),
                app_name: self.metadata.app_name.clone(),
                host_name: self.metadata.host_name.clone(),
            },
            frame: FrameState {
                depth: self.frame.depth + 1,
                loop_depth: self.frame.loop_depth,
                trancount: self.frame.trancount,
                xact_state: self.frame.xact_state,
                trigger_depth: self.frame.trigger_depth,
                module_stack: self.frame.module_stack.clone(),
                table_vars: self.frame.table_vars.clone(),
                readonly_table_vars: self.frame.readonly_table_vars.clone(),
                scope_vars: self.frame.scope_vars.clone(),
            },
            row: RowContext {
                outer_row: self.row.outer_row.clone(),
                apply_stack: self.row.apply_stack.clone(),
                current_group: self.row.current_group.clone(),
                window_context: self.row.window_context.clone(),
            },
            ctes: self.ctes.clone(),
            ansi_nulls: self.ansi_nulls,
            datefirst: self.datefirst,
            dirty_buffer: self.dirty_buffer.clone(),
            identity_insert: self.identity_insert.clone(),
            skip_instead_of: self.skip_instead_of,
            last_error: self.last_error.clone(),
        }
    }

    pub fn with_outer_row(&mut self, row: JoinedRow) -> ExecutionContext<'_> {
        let mut sub = self.subquery();
        sub.row.outer_row = Some(row);
        sub
    }

    pub fn with_outer_row_extended(
        &mut self,
        _current_row: JoinedRow,
        outer_row: JoinedRow,
    ) -> ExecutionContext<'_> {
        let mut sub = self.subquery();
        sub.row.outer_row = Some(outer_row);
        sub
    }


    pub fn enter_scope(&mut self) {
        self.frame.scope_vars.push(vec![]);
        self.frame.table_vars.push(HashMap::new());
        self.frame.readonly_table_vars.push(HashSet::new());
        self.session.identity_stack.push(None);
    }

    pub fn leave_scope(&mut self) {
        let _ = self.leave_scope_collect_table_vars();
    }

    pub fn leave_scope_collect_table_vars(&mut self) -> Vec<String> {
        if let Some(vars) = self.frame.scope_vars.pop() {
            for name in vars {
                self.session.variables.remove(&name);
            }
        }
        let mut dropped_physical = Vec::new();
        if self.frame.table_vars.len() > 1 {
            if let Some(scope) = self.frame.table_vars.pop() {
                for (logical, physical) in scope {
                    dropped_physical.push(physical.clone());
                    if self
                        .session
                        .var_map
                        .get(&logical)
                        .map(|current| current.eq_ignore_ascii_case(&physical))
                        .unwrap_or(false)
                    {
                        let mut restored: Option<String> = None;
                        for outer in self.frame.table_vars.iter().rev() {
                            if let Some(mapped) = outer.get(&logical) {
                                restored = Some(mapped.clone());
                                break;
                            }
                        }
                        if let Some(mapped) = restored {
                            self.session.var_map.insert(logical.clone(), mapped);
                        } else {
                            self.session.var_map.remove(&logical);
                        }
                    }
                }
            }
        }
        if self.frame.readonly_table_vars.len() > 1 {
            let _ = self.frame.readonly_table_vars.pop();
        }
        // Preserve identity value when leaving scope (propagate to parent)
        if let Some(val) = self.session.identity_stack.pop() {
            if let Some(last) = self.session.identity_stack.last_mut() {
                if val.is_some() {
                    *last = val;
                }
            }
        }
        dropped_physical
    }

    pub fn register_declared_var(&mut self, name: &str) {
        if let Some(scope) = self.frame.scope_vars.last_mut() {
            scope.push(name.to_string());
        }
    }

    pub fn register_table_var(&mut self, logical_name: &str, physical_name: &str) {
        if let Some(scope) = self.frame.table_vars.last_mut() {
            scope.insert(logical_name.to_uppercase(), physical_name.to_string());
        }
        self.session
            .var_map
            .insert(logical_name.to_uppercase(), physical_name.to_string());
    }

    pub fn mark_table_var_readonly(&mut self, logical_name: &str) {
        if let Some(scope) = self.frame.readonly_table_vars.last_mut() {
            scope.insert(logical_name.to_uppercase());
        }
    }

    pub fn is_readonly_table_var(&self, logical_name: &str) -> bool {
        let mut upper = logical_name.to_uppercase();
        if upper.starts_with("DBO.") {
            upper = upper["DBO.".len()..].to_string();
        }
        self.frame.readonly_table_vars
            .iter()
            .rev()
            .any(|scope| scope.contains(&upper))
    }

    pub fn resolve_table_name(&self, logical: &str) -> Option<String> {
        let mut upper = logical.to_uppercase();
        if upper.starts_with("DBO.") {
            upper = upper["DBO.".len()..].to_string();
        }

        // 1. Mapped names (temp tables #t and pseudo-tables like INSERTED)
        if let Some(mapped) = self.session.temp_map.get(&upper) {
            return Some(mapped.clone());
        }

        // 2. Scoped names (table variables @vars OR pseudo-tables like INSERTED)
        for scope in self.frame.table_vars.iter().rev() {
            if let Some(name) = scope.get(&upper) {
                return Some(name.clone());
            }
        }
        if let Some(name) = self.session.var_map.get(&upper) {
            return Some(name.clone());
        }

        None
    }

    pub fn push_apply_row(&mut self, row: JoinedRow) {
        self.row.apply_stack.push(row);
    }

    pub fn pop_apply_row(&mut self) {
        self.row.apply_stack.pop();
    }

    pub fn set_last_identity(&mut self, val: i64) {
        *self.session.last_identity = Some(val);
        if let Some(last) = self.session.identity_stack.last_mut() {
            *last = Some(val);
        } else {
            self.session.identity_stack.push(Some(val));
        }
    }

    pub fn current_scope_identity(&self) -> Option<i64> {
        self.session.identity_stack.last().and_then(|v| *v)
    }

    pub fn push_module(&mut self, module: ModuleFrame) {
        self.frame.module_stack.push(module);
    }

    pub fn pop_module(&mut self) {
        self.frame.module_stack.pop();
    }

    pub fn current_module(&self) -> Option<&ModuleFrame> {
        self.frame.module_stack.last()
    }

    pub fn current_procid(&self) -> Option<i32> {
        self.current_module().map(|module| module.object_id)
    }

    pub fn next_table_var_id(&mut self) -> u64 {
        *self.session.var_counter += 1;
        *self.session.var_counter
    }

    pub fn get_window_value(&self, expr: &crate::ast::Expr) -> Option<Value> {
        self.row.window_context
            .as_ref()
            .and_then(|m| m.get(expr).cloned())
    }

    pub fn create_snapshot(
        &self,
        options: &super::tooling::SessionOptions,
    ) -> super::session::SessionSnapshot {
        super::session::SessionSnapshot {
            variables: self.session.variables.clone(),
            identities: super::session::IdentityState {
                last_identity: *self.session.last_identity,
                scope_stack: self.session.identity_stack.clone(),
            },
            tables: super::session::TableState {
                temp_map: self.session.temp_map.clone(),
                var_map: self.session.var_map.clone(),
                var_counter: *self.session.var_counter,
            },
            cursors: super::session::CursorState {
                map: self.session.cursors.clone(),
                fetch_status: *self.session.fetch_status,
            },
            options: options.clone(),
            random_state: *self.session.random_state,
        }
    }

    pub fn restore_snapshot(
        &mut self,
        snapshot: super::session::SessionSnapshot,
        options: &mut super::tooling::SessionOptions,
    ) {
        *self.session.variables = snapshot.variables;
        *self.session.last_identity = snapshot.identities.last_identity;
        *self.session.identity_stack = snapshot.identities.scope_stack;
        *self.session.temp_map = snapshot.tables.temp_map;
        *self.session.var_map = snapshot.tables.var_map;
        *self.session.var_counter = snapshot.tables.var_counter;
        *self.session.cursors = snapshot.cursors.map;
        *self.session.fetch_status = snapshot.cursors.fetch_status;
        *options = snapshot.options;
        *self.session.random_state = snapshot.random_state;
        self.ansi_nulls = options.ansi_nulls;
        self.datefirst = options.datefirst;
    }
}

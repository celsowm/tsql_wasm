use std::collections::{HashMap, HashSet};

use super::cte::CteStorage;
use super::model::{Cursor, JoinedRow};
use crate::error::DbError;
use crate::types::{DataType, Value};

use super::string_norm::{normalize_identifier, strip_dbo_prefix};

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
    pub(crate) variables: &'a mut Variables,
    pub(crate) last_identity: &'a mut Option<i64>,
    pub(crate) identity_stack: &'a mut Vec<Option<i64>>,
    pub(crate) temp_map: &'a mut HashMap<String, String>,
    pub(crate) var_map: &'a mut HashMap<String, String>,
    pub(crate) var_counter: &'a mut u64,
    pub(crate) random_state: &'a mut u64,
    pub(crate) cursors: &'a mut HashMap<String, Cursor>,
    pub(crate) fetch_status: &'a mut i32,
    pub(crate) print_output: &'a mut Vec<String>,
    pub(crate) dirty_buffer:
        Option<std::sync::Arc<parking_lot::Mutex<super::dirty_buffer::DirtyBuffer>>>,
    pub(crate) identity_insert: HashSet<String>,
}

pub struct SessionMetadata {
    pub(crate) id: super::locks::SessionId,
    pub(crate) database: Option<String>,
    pub(crate) original_database: String,
    pub(crate) user: Option<String>,
    pub(crate) app_name: Option<String>,
    pub(crate) host_name: Option<String>,
    pub(crate) ansi_nulls: bool,
    pub(crate) datefirst: i32,
}

pub struct FrameState {
    pub(crate) depth: usize,
    pub(crate) loop_depth: usize,
    pub(crate) trancount: u32,
    pub(crate) xact_state: i8,
    pub(crate) trigger_depth: usize,
    pub(crate) module_stack: Vec<ModuleFrame>,
    pub(crate) table_vars: Vec<HashMap<String, String>>,
    pub(crate) readonly_table_vars: Vec<HashSet<String>>,
    pub(crate) scope_vars: Vec<Vec<String>>,
    pub(crate) skip_instead_of: bool,
    pub(crate) last_error: Option<DbError>,
}

pub struct RowContext {
    pub(crate) outer_row: Option<JoinedRow>,
    pub(crate) apply_stack: Vec<JoinedRow>,
    pub(crate) current_group: Option<super::model::Group>,
    pub(crate) window_context: Option<HashMap<String, Value>>,
    pub(crate) ctes: CteStorage,
}

pub struct ExecutionContext<'a> {
    pub(crate) session: SessionStateRefs<'a>,
    pub(crate) metadata: SessionMetadata,
    pub(crate) frame: FrameState,
    pub(crate) row: RowContext,
}

/// RAII guard that runs a cleanup closure on drop.
/// Use `ScopeGuard::new(|| cleanup_action())` to guarantee cleanup even on early returns.
pub struct ScopeGuard<F: FnOnce()> {
    cleanup: Option<F>,
}

impl<F: FnOnce()> ScopeGuard<F> {
    pub fn new(cleanup: F) -> Self {
        Self {
            cleanup: Some(cleanup),
        }
    }

    /// Consume the guard without running cleanup.
    pub fn dismiss(mut self) {
        self.cleanup = None;
    }
}

impl<F: FnOnce()> Drop for ScopeGuard<F> {
    fn drop(&mut self) {
        if let Some(cleanup) = self.cleanup.take() {
            cleanup();
        }
    }
}

impl<'a> ExecutionContext<'a> {
    /// P1 #12: Builder-style constructor that takes a `SessionRuntime` directly,
    /// eliminating the need to pass 18 individual parameters.
    pub fn from_session<C, S>(
        session: &'a mut super::session::SessionRuntime<C, S>,
        session_id: super::locks::SessionId,
        dirty_buffer: Option<std::sync::Arc<parking_lot::Mutex<super::dirty_buffer::DirtyBuffer>>>,
    ) -> Self {
        Self {
            session: SessionStateRefs {
                variables: &mut session.variables,
                last_identity: &mut session.identities.last_identity,
                identity_stack: &mut session.identities.scope_stack,
                temp_map: &mut session.tables.temp_map,
                var_map: &mut session.tables.var_map,
                var_counter: &mut session.tables.var_counter,
                random_state: &mut session.random_state,
                cursors: &mut session.cursors.map,
                fetch_status: &mut session.cursors.fetch_status,
                print_output: &mut session.diagnostics.print_output,
                dirty_buffer,
                identity_insert: HashSet::new(),
            },
            metadata: SessionMetadata {
                id: session_id,
                database: Some(session.original_database.clone()),
                original_database: session.original_database.clone(),
                user: session.user.clone(),
                app_name: session.app_name.clone(),
                host_name: session.host_name.clone(),
                ansi_nulls: session.options.ansi_nulls,
                datefirst: session.options.datefirst,
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
                skip_instead_of: false,
                last_error: None,
            },
            row: RowContext {
                outer_row: None,
                apply_stack: vec![],
                current_group: None,
                window_context: None,
                ctes: CteStorage::new(),
            },
        }
    }

    /// Legacy constructor — prefer `from_session()` for new code.
    #[deprecated(since = "0.2.0", note = "Use ExecutionContext::from_session() instead")]
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
                dirty_buffer,
                identity_insert: HashSet::new(),
            },
            metadata: SessionMetadata {
                id: session_id,
                database: Some(session_original_database.clone()),
                original_database: session_original_database,
                user,
                app_name,
                host_name,
                ansi_nulls,
                datefirst,
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
                skip_instead_of: false,
                last_error: None,
            },
            row: RowContext {
                outer_row: None,
                apply_stack: vec![],
                current_group: None,
                window_context: None,
                ctes: CteStorage::new(),
            },
        }
    }

    // Delegation methods for backward compatibility
    #[inline]
    pub fn variables(&self) -> &Variables {
        self.session.variables
    }
    #[inline]
    pub fn variables_mut(&mut self) -> &mut Variables {
        self.session.variables
    }
    #[inline]
    pub fn session_id(&self) -> super::locks::SessionId {
        self.metadata.id
    }
    #[inline]
    pub fn loop_depth(&self) -> usize {
        self.frame.loop_depth
    }
    #[inline]
    pub fn loop_depth_mut(&mut self) -> &mut usize {
        &mut self.frame.loop_depth
    }
    #[inline]
    pub fn trancount(&self) -> u32 {
        self.frame.trancount
    }
    #[inline]
    pub fn xact_state(&self) -> i8 {
        self.frame.xact_state
    }
    #[inline]
    pub fn trigger_depth(&self) -> usize {
        self.frame.trigger_depth
    }
    #[inline]
    pub fn trigger_depth_mut(&mut self) -> &mut usize {
        &mut self.frame.trigger_depth
    }
    #[inline]
    pub fn outer_row(&self) -> &Option<JoinedRow> {
        &self.row.outer_row
    }
    #[inline]
    pub fn outer_row_mut(&mut self) -> &mut Option<JoinedRow> {
        &mut self.row.outer_row
    }
    #[inline]
    pub fn current_group(&self) -> &Option<super::model::Group> {
        &self.row.current_group
    }
    #[inline]
    pub fn current_group_mut(&mut self) -> &mut Option<super::model::Group> {
        &mut self.row.current_group
    }

    // Session option accessors
    #[inline]
    pub fn ansi_nulls(&self) -> bool {
        self.metadata.ansi_nulls
    }
    #[inline]
    pub fn ansi_nulls_mut(&mut self) -> &mut bool {
        &mut self.metadata.ansi_nulls
    }
    #[inline]
    pub fn datefirst(&self) -> i32 {
        self.metadata.datefirst
    }
    #[inline]
    pub fn datefirst_mut(&mut self) -> &mut i32 {
        &mut self.metadata.datefirst
    }

    // Session state accessors
    #[inline]
    pub fn dirty_buffer(
        &self,
    ) -> &Option<std::sync::Arc<parking_lot::Mutex<super::dirty_buffer::DirtyBuffer>>> {
        &self.session.dirty_buffer
    }
    #[inline]
    pub fn identity_insert(&self) -> &HashSet<String> {
        &self.session.identity_insert
    }
    #[inline]
    pub fn identity_insert_mut(&mut self) -> &mut HashSet<String> {
        &mut self.session.identity_insert
    }

    // Frame state accessors
    #[inline]
    pub fn last_error(&self) -> &Option<DbError> {
        &self.frame.last_error
    }
    #[inline]
    pub fn last_error_mut(&mut self) -> &mut Option<DbError> {
        &mut self.frame.last_error
    }

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
                dirty_buffer: self.session.dirty_buffer.clone(),
                identity_insert: self.session.identity_insert.clone(),
            },
            metadata: SessionMetadata {
                id: self.metadata.id,
                database: self.metadata.database.clone(),
                original_database: self.metadata.original_database.clone(),
                user: self.metadata.user.clone(),
                app_name: self.metadata.app_name.clone(),
                host_name: self.metadata.host_name.clone(),
                ansi_nulls: self.metadata.ansi_nulls,
                datefirst: self.metadata.datefirst,
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
                skip_instead_of: self.frame.skip_instead_of,
                last_error: self.frame.last_error.clone(),
            },
            row: RowContext {
                outer_row: self.row.outer_row.clone(),
                apply_stack: self.row.apply_stack.clone(),
                current_group: self.row.current_group.clone(),
                window_context: self.row.window_context.clone(),
                ctes: self.row.ctes.clone(),
            },
        }
    }

    pub fn with_outer_row(&mut self, row: JoinedRow) -> ExecutionContext<'_> {
        let mut sub = self.subquery();
        sub.row.outer_row = Some(row);
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
            scope.insert(
                normalize_identifier(logical_name),
                physical_name.to_string(),
            );
        }
        self.session.var_map.insert(
            normalize_identifier(logical_name),
            physical_name.to_string(),
        );
    }

    pub fn mark_table_var_readonly(&mut self, logical_name: &str) {
        if let Some(scope) = self.frame.readonly_table_vars.last_mut() {
            scope.insert(normalize_identifier(logical_name));
        }
    }

    pub fn is_readonly_table_var(&self, logical_name: &str) -> bool {
        let upper = normalize_identifier(strip_dbo_prefix(logical_name));
        self.frame
            .readonly_table_vars
            .iter()
            .rev()
            .any(|scope| scope.contains(&upper))
    }

    pub fn resolve_table_name(&self, logical: &str) -> Option<String> {
        let upper = normalize_identifier(strip_dbo_prefix(logical));

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
        match self.session.identity_stack.last().and_then(|v| *v) {
            Some(v) => Some(v),
            None if self.session.identity_stack.len() == 1 => *self.session.last_identity,
            None => None,
        }
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

    pub fn get_window_value(&self, key: &str) -> Option<Value> {
        self.row
            .window_context
            .as_ref()
            .and_then(|m| m.get(key).cloned())
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
        self.metadata.ansi_nulls = options.ansi_nulls;
        self.metadata.datefirst = options.datefirst;
    }
}

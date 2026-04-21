use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::cte::CteStorage;
use super::model::{Cursor, JoinedRow};
use crate::error::DbError;
use crate::types::{DataType, Value};

use super::string_norm::{normalize_identifier, strip_dbo_prefix};
use super::context_factory;

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
    pub(crate) next_cursor_handle: &'a mut i32,
    pub(crate) handle_map: &'a mut HashMap<i32, String>,
    pub(crate) print_output: &'a mut Vec<String>,
    pub(crate) bulk_load_active: &'a mut bool,
    pub(crate) bulk_load_table: &'a mut Option<crate::ast::ObjectName>,
    pub(crate) bulk_load_columns: &'a mut Option<Vec<crate::ast::statements::ddl::ColumnSpec>>,
    pub(crate) bulk_load_received_metadata: &'a mut bool,
    pub(crate) context_info: &'a mut Vec<u8>,
    pub(crate) session_context: &'a mut HashMap<String, (Value, bool)>,
    pub(crate) dirty_buffer:
        Option<std::sync::Arc<parking_lot::Mutex<super::dirty_buffer::DirtyBuffer>>>,
    pub(crate) identity_insert: HashSet<String>,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
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

/// Stores pre-computed window function results and a row index so that
/// per-row lookups avoid cloning every key/value into a new HashMap.
#[derive(Debug, Clone)]
pub struct WindowContext {
    pub(crate) results: HashMap<String, Vec<Value>>,
    pub(crate) row_idx: usize,
}

impl WindowContext {
    pub fn get(&self, key: &str) -> Option<Value> {
        self.results
            .get(key)
            .and_then(|vals| vals.get(self.row_idx))
            .cloned()
    }
}

#[derive(Debug, Clone)]
pub struct RowContext {
    pub(crate) outer_stack: Vec<JoinedRow>,
    pub(crate) apply_stack: Vec<JoinedRow>,
    pub(crate) current_group: Option<super::model::Group>,
    pub(crate) window_context: Option<WindowContext>,
    pub(crate) ctes: CteStorage,
}

pub struct ExecutionContext<'a> {
    pub(crate) session: SessionStateRefs<'a>,
    pub(crate) metadata: SessionMetadata,
    pub(crate) options: super::tooling::SessionOptions,
    pub(crate) frame: FrameState,
    pub(crate) row: RowContext,
    pub(crate) subquery_cache: Arc<Mutex<HashMap<String, super::result::QueryResult>>>,
}

impl<'a> SessionStateRefs<'a> {
    #[inline]
    pub fn variables(&self) -> &Variables {
        self.variables
    }

    #[inline]
    pub fn variables_mut(&mut self) -> &mut Variables {
        self.variables
    }

    #[inline]
    pub fn dirty_buffer(
        &self,
    ) -> &Option<std::sync::Arc<parking_lot::Mutex<super::dirty_buffer::DirtyBuffer>>> {
        &self.dirty_buffer
    }

    #[inline]
    pub fn identity_insert(&self) -> &HashSet<String> {
        &self.identity_insert
    }

    #[inline]
    pub fn identity_insert_mut(&mut self) -> &mut HashSet<String> {
        &mut self.identity_insert
    }

    pub fn set_last_identity(&mut self, val: i64) {
        *self.last_identity = Some(val);
        if let Some(last) = self.identity_stack.last_mut() {
            *last = Some(val);
        } else {
            self.identity_stack.push(Some(val));
        }
    }

    pub fn current_scope_identity(&self) -> Option<i64> {
        match self.identity_stack.last().and_then(|v| *v) {
            Some(v) => Some(v),
            None if self.identity_stack.len() == 1 => *self.last_identity,
            None => None,
        }
    }

    pub fn next_table_var_id(&mut self) -> u64 {
        *self.var_counter += 1;
        *self.var_counter
    }

    pub fn create_snapshot(
        &self,
        options: &super::tooling::SessionOptions,
    ) -> super::session::SessionSnapshot {
        super::session::SessionSnapshot {
            variables: self.variables.clone(),
            identities: super::session::IdentityState {
                last_identity: *self.last_identity,
                scope_stack: self.identity_stack.clone(),
            },
            tables: super::session::TableState {
                temp_map: self.temp_map.clone(),
                var_map: self.var_map.clone(),
                var_counter: *self.var_counter,
            },
            cursors: super::session::CursorState {
                map: self.cursors.clone(),
                fetch_status: *self.fetch_status,
                next_cursor_handle: *self.next_cursor_handle,
                handle_map: self.handle_map.clone(),
            },
            options: options.clone(),
            random_state: *self.random_state,
            context_info: self.context_info.clone(),
            session_context: self.session_context.clone(),
        }
    }

    pub fn set_bulk_load_active(
        &mut self,
        active: bool,
        table: crate::ast::ObjectName,
        columns: Vec<crate::ast::statements::ddl::ColumnSpec>,
    ) {
        *self.bulk_load_active = active;
        *self.bulk_load_table = Some(table);
        *self.bulk_load_columns = Some(columns);
        *self.bulk_load_received_metadata = false;
    }

    pub fn restore_snapshot(
        &mut self,
        snapshot: super::session::SessionSnapshot,
        options: &mut super::tooling::SessionOptions,
    ) {
        *self.variables = snapshot.variables;
        *self.last_identity = snapshot.identities.last_identity;
        *self.identity_stack = snapshot.identities.scope_stack;
        *self.temp_map = snapshot.tables.temp_map;
        *self.var_map = snapshot.tables.var_map;
        *self.var_counter = snapshot.tables.var_counter;
        *self.cursors = snapshot.cursors.map;
        *self.fetch_status = snapshot.cursors.fetch_status;
        *self.next_cursor_handle = snapshot.cursors.next_cursor_handle;
        *self.handle_map = snapshot.cursors.handle_map;
        *options = snapshot.options;
        *self.random_state = snapshot.random_state;
        *self.context_info = snapshot.context_info;
        *self.session_context = snapshot.session_context;
    }
}

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
        context_factory::from_session(session, session_id, dirty_buffer)
    }

    /// Legacy constructor — prefer `from_session()` for new code.
    #[deprecated(since = "0.2.0", note = "Use ExecutionContext::from_session() instead")]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        variables: &'a mut Variables,
        bulk_load_active: &'a mut bool,
        bulk_load_table: &'a mut Option<crate::ast::ObjectName>,
        bulk_load_columns: &'a mut Option<Vec<crate::ast::statements::ddl::ColumnSpec>>,
        bulk_load_received_metadata: &'a mut bool,
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
        next_cursor_handle: &'a mut i32,
        handle_map: &'a mut HashMap<i32, String>,
        print_output: &'a mut Vec<String>,
        context_info: &'a mut Vec<u8>,
        session_context: &'a mut HashMap<String, (Value, bool)>,
        dirty_buffer: Option<std::sync::Arc<parking_lot::Mutex<super::dirty_buffer::DirtyBuffer>>>,
        session_id: super::locks::SessionId,
        session_current_database: String,
        session_original_database: String,
        user: Option<String>,
        app_name: Option<String>,
        host_name: Option<String>,
    ) -> Self {
        context_factory::legacy_new(
            variables,
            bulk_load_active,
            bulk_load_table,
            bulk_load_columns,
            bulk_load_received_metadata,
            session_last_identity,
            scope_identity_stack,
            temp_table_map,
            session_table_var_map,
            table_var_counter,
            ansi_nulls,
            datefirst,
            random_state,
            cursors,
            fetch_status,
            next_cursor_handle,
            handle_map,
            print_output,
            context_info,
            session_context,
            dirty_buffer,
            session_id,
            session_current_database,
            session_original_database,
            user,
            app_name,
            host_name,
        )
    }

    // Delegation methods for backward compatibility
    #[inline]
    pub fn variables(&self) -> &Variables {
        self.session.variables()
    }
    #[inline]
    pub fn variables_mut(&mut self) -> &mut Variables {
        self.session.variables_mut()
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
    pub fn outer_row(&self) -> Option<&JoinedRow> {
        self.row.outer_stack.last()
    }
    #[inline]
    pub fn outer_row_mut(&mut self) -> Option<&mut JoinedRow> {
        self.row.outer_stack.last_mut()
    }
    #[inline]
    pub fn outer_stack(&self) -> &[JoinedRow] {
        &self.row.outer_stack
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
        self.session.dirty_buffer()
    }
    #[inline]
    pub fn identity_insert(&self) -> &HashSet<String> {
        self.session.identity_insert()
    }
    #[inline]
    pub fn identity_insert_mut(&mut self) -> &mut HashSet<String> {
        self.session.identity_insert_mut()
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
                next_cursor_handle: self.session.next_cursor_handle,
                handle_map: self.session.handle_map,
                print_output: self.session.print_output,
                bulk_load_active: self.session.bulk_load_active,
                bulk_load_table: self.session.bulk_load_table,
                bulk_load_columns: self.session.bulk_load_columns,
                bulk_load_received_metadata: self.session.bulk_load_received_metadata,
                context_info: self.session.context_info,
                session_context: self.session.session_context,
                dirty_buffer: self.session.dirty_buffer.clone(),
                identity_insert: self.session.identity_insert.clone(),
            },
            metadata: self.metadata.clone(),
            options: self.options.clone(),
            frame: self.frame.fork(),
            row: self.row.fork(),
            subquery_cache: self.subquery_cache.clone(),
        }
    }

    pub fn with_outer_row(&mut self, row: JoinedRow) -> ExecutionContext<'_> {
        let mut sub = self.subquery();
        sub.row.outer_stack.push(row);
        sub
    }

    pub fn enter_scope(&mut self) {
        self.frame.enter_scope(&mut self.session);
    }

    pub fn leave_scope(&mut self) {
        let _ = self.leave_scope_collect_table_vars();
    }

    pub fn leave_scope_collect_table_vars(&mut self) -> Vec<String> {
        self.frame.leave_scope_collect_table_vars(&mut self.session)
    }

    pub fn register_declared_var(&mut self, name: &str) {
        self.frame.register_declared_var(name);
    }

    pub fn register_table_var(&mut self, logical_name: &str, physical_name: &str) {
        self.frame
            .register_table_var(&mut self.session, logical_name, physical_name);
    }

    pub fn mark_table_var_readonly(&mut self, logical_name: &str) {
        self.frame.mark_table_var_readonly(logical_name);
    }

    pub fn is_readonly_table_var(&self, logical_name: &str) -> bool {
        self.frame.is_readonly_table_var(logical_name)
    }

    pub fn resolve_table_name(&self, logical: &str) -> Option<String> {
        self.frame.resolve_table_name(&self.session, logical)
    }

    pub fn push_apply_row(&mut self, row: JoinedRow) {
        self.row.push_apply_row(row);
    }

    pub fn pop_apply_row(&mut self) {
        self.row.pop_apply_row();
    }

    pub fn set_last_identity(&mut self, val: i64) {
        self.session.set_last_identity(val);
    }

    pub fn set_bulk_load_active(
        &mut self,
        active: bool,
        table: crate::ast::ObjectName,
        columns: Vec<crate::ast::statements::ddl::ColumnSpec>,
    ) {
        self.session.set_bulk_load_active(active, table, columns);
    }

    pub fn current_scope_identity(&self) -> Option<i64> {
        self.session.current_scope_identity()
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
        self.session.next_table_var_id()
    }

    pub fn get_window_value(&self, key: &str) -> Option<Value> {
        self.row.get_window_value(key)
    }

    pub fn create_snapshot(
        &self,
        options: &super::tooling::SessionOptions,
    ) -> super::session::SessionSnapshot {
        self.session.create_snapshot(options)
    }

    pub fn restore_snapshot(
        &mut self,
        snapshot: super::session::SessionSnapshot,
        options: &mut super::tooling::SessionOptions,
    ) {
        self.session.restore_snapshot(snapshot, options);
        self.metadata.ansi_nulls = options.ansi_nulls;
        self.metadata.datefirst = options.datefirst;
        self.options = options.clone();
    }
}

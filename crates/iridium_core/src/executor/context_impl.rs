use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::error::DbError;
use crate::executor::context::{ExecutionContext, Variables};
use crate::executor::context_factory;
use crate::executor::dirty_buffer;
use crate::executor::locks;
use crate::executor::model::Group;
use crate::executor::session;
use crate::executor::tooling;
use crate::types::Value;

#[allow(dead_code)]
pub struct ScopeGuard<F: FnOnce()> {
    cleanup: Option<F>,
}

#[allow(dead_code)]
impl<F: FnOnce()> ScopeGuard<F> {
    pub fn new(cleanup: F) -> Self {
        Self {
            cleanup: Some(cleanup),
        }
    }

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
    pub fn from_session<C, S>(
        session: &'a mut crate::executor::session::SessionRuntime<C, S>,
        session_id: locks::SessionId,
        dirty_buffer: Option<Arc<Mutex<dirty_buffer::DirtyBuffer>>>,
    ) -> Self {
        context_factory::from_session(session, session_id, dirty_buffer)
    }

    #[deprecated(since = "0.2.0", note = "Use ExecutionContext::from_session() instead")]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        variables: &'a mut Variables,
        bulk_load: &'a mut crate::executor::session::BulkLoadState,
        session_last_identity: &'a mut Option<i64>,
        scope_identity_stack: &'a mut Vec<Option<i64>>,
        temp_table_map: &'a mut HashMap<String, String>,
        session_table_var_map: &'a mut HashMap<String, String>,
        table_var_counter: &'a mut u64,
        ansi_nulls: bool,
        datefirst: i32,
        random_state: &'a mut u64,
        cursors: &'a mut HashMap<String, crate::executor::model::Cursor>,
        fetch_status: &'a mut i32,
        next_cursor_handle: &'a mut i32,
        handle_map: &'a mut HashMap<i32, String>,
        print_output: &'a mut Vec<String>,
        context_info: &'a mut Vec<u8>,
        session_context: &'a mut HashMap<String, (Value, bool)>,
        dirty_buffer: Option<Arc<Mutex<dirty_buffer::DirtyBuffer>>>,
        session_id: locks::SessionId,
        session_current_database: String,
        session_original_database: String,
        user: Option<String>,
        app_name: Option<String>,
        host_name: Option<String>,
    ) -> Self {
        context_factory::legacy_new(
            variables,
            bulk_load,
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

    pub fn session_id(&self) -> locks::SessionId {
        self.metadata.id
    }

    pub fn depth(&self) -> usize {
        self.frame.depth
    }

    pub fn loop_depth(&self) -> usize {
        self.frame.loop_depth
    }

    pub fn trancount(&self) -> u32 {
        self.frame.trancount
    }

    pub fn xact_state(&self) -> i8 {
        self.frame.xact_state
    }

    pub fn trigger_depth(&self) -> usize {
        self.frame.trigger_depth
    }

    pub fn current_scope_identity(&self) -> Option<i64> {
        self.session.current_scope_identity()
    }

    pub fn current_procid(&self) -> Option<i32> {
        self.frame.module_stack.last().map(|frame| frame.object_id)
    }

    pub fn push_module(&mut self, frame: crate::executor::context::ModuleFrame) {
        self.frame.module_stack.push(frame);
    }

    pub fn pop_module(&mut self) {
        self.frame.module_stack.pop();
    }

    pub fn current_group(&self) -> &Option<Group> {
        &self.row.current_group
    }

    #[inline]
    pub fn current_group_mut(&mut self) -> &mut Option<Group> {
        &mut self.row.current_group
    }

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

    #[inline]
    pub fn dirty_buffer(&self) -> &Option<Arc<Mutex<dirty_buffer::DirtyBuffer>>> {
        self.session.dirty_buffer()
    }

    #[inline]
    pub fn identity_insert(&self) -> &std::collections::HashSet<String> {
        self.session.identity_insert()
    }

    #[inline]
    pub fn identity_insert_mut(&mut self) -> &mut std::collections::HashSet<String> {
        self.session.identity_insert_mut()
    }

    #[inline]
    pub fn last_error(&self) -> &Option<DbError> {
        &self.frame.last_error
    }

    #[inline]
    pub fn last_error_mut(&mut self) -> &mut Option<DbError> {
        &mut self.frame.last_error
    }

    pub fn set_last_identity(&mut self, val: i64) {
        self.session.set_last_identity(val);
    }

    pub fn next_table_var_id(&mut self) -> u64 {
        self.session.next_table_var_id()
    }

    pub fn set_bulk_load_active(
        &mut self,
        active: bool,
        table: crate::ast::ObjectName,
        columns: Vec<crate::ast::statements::ddl::ColumnSpec>,
    ) {
        self.session.set_bulk_load_active(active, table, columns);
    }

    pub fn create_snapshot(&self, options: &tooling::SessionOptions) -> session::SessionSnapshot {
        self.session.create_snapshot(options)
    }

    pub fn restore_snapshot(
        &mut self,
        snapshot: session::SessionSnapshot,
        options: &mut tooling::SessionOptions,
    ) {
        self.session.restore_snapshot(snapshot, options);
    }

    pub fn subquery(&mut self) -> ExecutionContext<'_> {
        ExecutionContext {
            session: self.session.fork(),
            metadata: self.metadata.clone(),
            options: self.options.clone(),
            frame: self.frame.fork(),
            row: self.row.fork(),
            subquery_cache: self.subquery_cache.clone(),
        }
    }

    pub fn with_outer_row(&mut self, row: crate::executor::model::JoinedRow) -> ExecutionContext<'_> {
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

    pub fn push_apply_row(&mut self, row: crate::executor::model::JoinedRow) {
        self.row.push_apply_row(row);
    }

    pub fn pop_apply_row(&mut self) {
        self.row.pop_apply_row();
    }

    pub fn get_window_value(&self, key: &str) -> Option<Value> {
        self.row.get_window_value(key)
    }
}

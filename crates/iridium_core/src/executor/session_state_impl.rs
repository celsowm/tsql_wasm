use std::collections::HashSet;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::executor::context::{SessionStateRefs, Variables};
use crate::executor::dirty_buffer;
use crate::executor::session;
use crate::executor::tooling;

impl<'a> SessionStateRefs<'a> {
    /// Create a derived SessionStateRefs for subquery execution.
    /// Eliminates the manual 23-field copy that previously lived in
    /// ExecutionContext::subquery().
    pub fn fork(&mut self) -> SessionStateRefs<'_> {
        SessionStateRefs {
            variables: self.variables,
            last_identity: self.last_identity,
            identity_stack: self.identity_stack,
            temp_map: self.temp_map,
            var_map: self.var_map,
            var_counter: self.var_counter,
            random_state: self.random_state,
            cursors: self.cursors,
            fetch_status: self.fetch_status,
            next_cursor_handle: self.next_cursor_handle,
            handle_map: self.handle_map,
            print_output: self.print_output,
            bulk_load: self.bulk_load,
            context_info: self.context_info,
            session_context: self.session_context,
            dirty_buffer: self.dirty_buffer.clone(),
            identity_insert: self.identity_insert.clone(),
        }
    }

    #[inline]
    pub fn variables(&self) -> &Variables {
        self.variables
    }

    #[inline]
    pub fn variables_mut(&mut self) -> &mut Variables {
        self.variables
    }

    #[inline]
    pub fn dirty_buffer(&self) -> &Option<Arc<Mutex<dirty_buffer::DirtyBuffer>>> {
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

    pub fn create_snapshot(&self, options: &tooling::SessionOptions) -> session::SessionSnapshot {
        session::SessionSnapshot {
            variables: self.variables.clone(),
            identities: session::IdentityState {
                last_identity: *self.last_identity,
                scope_stack: self.identity_stack.clone(),
            },
            tables: session::TableState {
                temp_map: self.temp_map.clone(),
                var_map: self.var_map.clone(),
                var_counter: *self.var_counter,
            },
            cursors: session::CursorState {
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
        self.bulk_load.active = active;
        self.bulk_load.table = Some(table);
        self.bulk_load.columns = Some(columns);
        self.bulk_load.received_metadata = false;
    }

    pub fn restore_snapshot(
        &mut self,
        snapshot: session::SessionSnapshot,
        options: &mut tooling::SessionOptions,
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

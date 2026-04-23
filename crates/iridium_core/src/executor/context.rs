use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

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
    pub(crate) bulk_load: &'a mut super::session::BulkLoadState,
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

#[derive(Debug, Clone)]
pub struct WindowContext {
    pub(crate) results: HashMap<String, Vec<Value>>,
    pub(crate) row_idx: usize,
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

impl WindowContext {
    pub fn get(&self, key: &str) -> Option<Value> {
        self.results
            .get(key)
            .and_then(|vals| vals.get(self.row_idx))
            .cloned()
    }
}

#[path = "context_impl.rs"]
mod context_impl;
#[path = "context_frame.rs"]
mod context_frame;
#[path = "context_row.rs"]
mod context_row;
#[path = "session_state_impl.rs"]
mod session_state_impl;

mod id_allocator;
mod index_registry;
mod object_resolver;
mod routine_registry;
mod schema_registry;
mod table_registry;
mod trigger_registry;
mod type_registry;
mod view_registry;

use crate::ast::Expr;
use crate::ast::{DataTypeSpec, FunctionBody, RoutineParam, Statement, TriggerEvent};
use crate::error::DbError;
use crate::types::DataType;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaDef {
    pub id: u32,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKeyDef {
    pub name: String,
    pub columns: Vec<String>,
    pub referenced_table: crate::ast::ObjectName,
    pub referenced_columns: Vec<String>,
    pub on_delete: crate::ast::ReferentialAction,
    pub on_update: crate::ast::ReferentialAction,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentityDef {
    pub seed: i64,
    pub increment: i64,
    pub current: i64,
}

impl IdentityDef {
    pub fn new(seed: i64, increment: i64) -> Self {
        Self {
            seed,
            increment,
            current: seed,
        }
    }

    pub fn next_value(&mut self) -> i64 {
        let value = self.current;
        self.current += self.increment;
        value
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub id: u32,
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub primary_key: bool,
    pub unique: bool,
    pub identity: Option<IdentityDef>,
    pub default: Option<Expr>,
    pub default_constraint_name: Option<String>,
    pub check: Option<Expr>,
    pub check_constraint_name: Option<String>,
    pub computed_expr: Option<Expr>,
    #[serde(default = "default_ansi_padding_on")]
    pub ansi_padding_on: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckConstraintDef {
    pub name: String,
    pub expr: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDef {
    pub id: u32,
    pub schema_id: u32,
    pub table_id: u32,
    pub name: String,
    pub column_ids: Vec<u32>,
    pub is_unique: bool,
    pub is_clustered: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDef {
    pub id: u32,
    pub schema_id: u32,
    pub schema_name: String,
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub check_constraints: Vec<CheckConstraintDef>,
    pub foreign_keys: Vec<ForeignKeyDef>,
}

impl TableDef {
    pub fn schema_or_dbo(&self) -> &str {
        &self.schema_name
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RoutineKind {
    Procedure {
        body: Vec<Statement>,
    },
    Function {
        returns: Option<DataTypeSpec>,
        body: FunctionBody,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutineDef {
    #[serde(default)]
    pub object_id: i32,
    pub schema: String,
    pub name: String,
    pub params: Vec<RoutineParam>,
    pub kind: RoutineKind,
    #[serde(default)]
    pub definition_sql: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableTypeDef {
    #[serde(default)]
    pub object_id: i32,
    pub schema: String,
    pub name: String,
    pub columns: Vec<crate::ast::ColumnSpec>,
    pub table_constraints: Vec<crate::ast::TableConstraintSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewDef {
    #[serde(default)]
    pub object_id: i32,
    pub schema: String,
    pub name: String,
    pub query: Statement, // Should be Statement::Select
    #[serde(default)]
    pub definition_sql: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerDef {
    #[serde(default)]
    pub object_id: i32,
    pub schema: String,
    pub name: String,
    pub table_schema: String,
    pub table_name: String,
    pub events: Vec<TriggerEvent>,
    pub is_instead_of: bool,
    pub body: Vec<Statement>,
    #[serde(default)]
    pub definition_sql: String,
}

pub trait IdAllocator {
    fn alloc_table_id(&mut self) -> u32;
    fn alloc_object_id(&mut self) -> i32;
    fn alloc_column_id(&mut self) -> u32;
    fn alloc_index_id(&mut self) -> u32;
    fn alloc_schema_id(&mut self) -> u32;
}

pub trait SchemaRegistry {
    fn get_schemas(&self) -> &[SchemaDef];
    fn get_schema_id(&self, name: &str) -> Option<u32>;
    fn create_schema(&mut self, name: &str) -> Result<(), DbError>;
    fn drop_schema(&mut self, name: &str) -> Result<(), DbError>;
}

pub trait TableRegistry {
    fn get_tables(&self) -> &[TableDef];
    fn find_table(&self, schema: &str, name: &str) -> Option<&TableDef>;
    fn find_table_mut(&mut self, schema: &str, name: &str) -> Option<&mut TableDef>;
    fn register_table(&mut self, table: TableDef);
    fn unregister_table_by_id(&mut self, id: u32);
    fn drop_table(&mut self, schema: &str, name: &str) -> Result<u32, DbError>;
    fn next_identity_value(&mut self, table_id: u32, column_name: &str) -> Result<i64, DbError>;
}

pub trait IndexRegistry {
    fn get_indexes(&self) -> &[IndexDef];
    fn register_index(&mut self, index: IndexDef);
    fn drop_index_by_table_id(&mut self, table_id: u32);
    fn create_index(
        &mut self,
        schema: &str,
        name: &str,
        table_schema: &str,
        table_name: &str,
        columns: &[String],
        // Using TableRegistry to find tables instead of passing them
    ) -> Result<(), DbError>;
    fn drop_index(
        &mut self,
        schema: &str,
        name: &str,
        table_schema: &str,
        table_name: &str,
    ) -> Result<(), DbError>;
}

pub trait RoutineRegistry {
    fn get_routines(&self) -> &[RoutineDef];
    fn find_routine(&self, schema: &str, name: &str) -> Option<&RoutineDef>;
    fn create_routine(&mut self, routine: RoutineDef) -> Result<(), DbError>;
    fn drop_routine(
        &mut self,
        schema: &str,
        name: &str,
        expect_function: bool,
    ) -> Result<(), DbError>;
}

pub trait TypeRegistry {
    fn get_table_types(&self) -> &[TableTypeDef];
    fn find_table_type(&self, schema: &str, name: &str) -> Option<&TableTypeDef>;
    fn create_table_type(&mut self, def: TableTypeDef) -> Result<(), DbError>;
    fn drop_table_type(&mut self, schema: &str, name: &str) -> Result<(), DbError>;
}

pub trait ViewRegistry {
    fn get_views(&self) -> &[ViewDef];
    fn find_view(&self, schema: &str, name: &str) -> Option<&ViewDef>;
    fn create_view(&mut self, view: ViewDef) -> Result<(), DbError>;
    fn drop_view(&mut self, schema: &str, name: &str) -> Result<(), DbError>;
}

pub trait TriggerRegistry {
    fn get_triggers(&self) -> &[TriggerDef];
    fn find_triggers_for_table(&self, schema: &str, name: &str) -> Vec<&TriggerDef>;
    fn create_trigger(&mut self, trigger: TriggerDef) -> Result<(), DbError>;
    fn drop_trigger(&mut self, schema: &str, name: &str) -> Result<(), DbError>;
}

pub trait ObjectResolver {
    fn object_id(&self, schema: &str, name: &str) -> Option<i32>;
}

/// Aggregate convenience trait that composes 9 focused sub-traits: [`IdAllocator`],
/// [`SchemaRegistry`], [`TableRegistry`], [`IndexRegistry`], [`RoutineRegistry`],
/// [`TypeRegistry`], [`ViewRegistry`], [`TriggerRegistry`], and [`ObjectResolver`].
///
/// New code that only needs a subset of catalog capabilities should prefer narrower sub-trait
/// bounds (e.g. `T: TableRegistry + SchemaRegistry`) to keep coupling minimal. This aggregate
/// exists as a facade for contexts that genuinely require full catalog access.
pub trait Catalog:
    IdAllocator
    + SchemaRegistry
    + TableRegistry
    + IndexRegistry
    + RoutineRegistry
    + TypeRegistry
    + ViewRegistry
    + TriggerRegistry
    + ObjectResolver
    + std::fmt::Debug
    + Send
    + Sync
{
    fn clone_boxed(&self) -> Box<dyn Catalog>;
    fn rebuild_maps(&mut self) {}
}

use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogImpl {
    schemas: Vec<SchemaDef>,
    tables: Vec<TableDef>,
    indexes: Vec<IndexDef>,
    routines: Vec<RoutineDef>,
    table_types: Vec<TableTypeDef>,
    views: Vec<ViewDef>,
    triggers: Vec<TriggerDef>,
    next_schema_id: u32,
    next_table_id: u32,
    next_column_id: u32,
    next_index_id: u32,
    #[serde(default = "default_next_object_id")]
    next_object_id: i32,

    #[serde(skip)]
    schema_map: HashMap<String, usize>,
    #[serde(skip)]
    table_map: HashMap<(u32, String), usize>,
    #[serde(skip)]
    routine_map: HashMap<(String, String), usize>,
    #[serde(skip)]
    type_map: HashMap<(String, String), usize>,
    #[serde(skip)]
    view_map: HashMap<(String, String), usize>,
    #[serde(skip)]
    trigger_map: HashMap<(String, String), usize>,
    #[serde(skip)]
    index_map: HashMap<(u32, String), usize>,
}

impl CatalogImpl {
    pub fn new() -> Self {
        let mut c = Self::default();
        let dbo_id = c.alloc_schema_id();
        c.schemas.push(SchemaDef {
            id: dbo_id,
            name: "dbo".to_string(),
        });
        c.rebuild_maps();
        c
    }

    pub fn rebuild_maps(&mut self) {
        self.schema_map = self
            .schemas
            .iter()
            .enumerate()
            .map(|(i, s)| (s.name.to_lowercase(), i))
            .collect();
        self.table_map = self
            .tables
            .iter()
            .enumerate()
            .map(|(i, t)| ((t.schema_id, t.name.to_lowercase()), i))
            .collect();
        self.routine_map = self
            .routines
            .iter()
            .enumerate()
            .map(|(i, r)| ((r.schema.to_lowercase(), r.name.to_lowercase()), i))
            .collect();
        self.type_map = self
            .table_types
            .iter()
            .enumerate()
            .map(|(i, t)| ((t.schema.to_lowercase(), t.name.to_lowercase()), i))
            .collect();
        self.view_map = self
            .views
            .iter()
            .enumerate()
            .map(|(i, v)| ((v.schema.to_lowercase(), v.name.to_lowercase()), i))
            .collect();
        self.trigger_map = self
            .triggers
            .iter()
            .enumerate()
            .map(|(i, t)| ((t.schema.to_lowercase(), t.name.to_lowercase()), i))
            .collect();
        self.index_map = self
            .indexes
            .iter()
            .enumerate()
            .map(|(i, idx)| ((idx.schema_id, idx.name.to_lowercase()), i))
            .collect();
    }

    /// Remove the table at `idx` using swap_remove (O(1)) and fix up only the
    /// table_map and index_map entries that were affected.
    pub(crate) fn remove_table_at(&mut self, idx: usize) {
        let removed = self.tables.swap_remove(idx);
        self.table_map.remove(&(removed.schema_id, removed.name.to_lowercase()));

        // If swap_remove moved the last element into `idx`, update its map entry.
        if idx < self.tables.len() {
            let swapped = &self.tables[idx];
            self.table_map.insert((swapped.schema_id, swapped.name.to_lowercase()), idx);
        }

        // Remove associated indexes and rebuild only the index_map.
        self.indexes.retain(|i| i.table_id != removed.id);
        self.index_map = self
            .indexes
            .iter()
            .enumerate()
            .map(|(i, idx)| ((idx.schema_id, idx.name.to_lowercase()), i))
            .collect();
    }
}

impl Default for CatalogImpl {
    fn default() -> Self {
        Self {
            schemas: Vec::new(),
            tables: Vec::new(),
            indexes: Vec::new(),
            routines: Vec::new(),
            table_types: Vec::new(),
            views: Vec::new(),
            triggers: Vec::new(),
            next_schema_id: 1,
            next_table_id: 1234567890,
            next_column_id: 1,
            next_index_id: 234567890,
            next_object_id: -1,
            schema_map: HashMap::new(),
            table_map: HashMap::new(),
            routine_map: HashMap::new(),
            type_map: HashMap::new(),
            view_map: HashMap::new(),
            trigger_map: HashMap::new(),
            index_map: HashMap::new(),
        }
    }
}

fn default_next_object_id() -> i32 {
    -1
}

fn default_ansi_padding_on() -> bool {
    true
}

impl Catalog for CatalogImpl {
    fn clone_boxed(&self) -> Box<dyn Catalog> {
        Box::new(self.clone())
    }

    fn rebuild_maps(&mut self) {
        self.rebuild_maps();
    }
}

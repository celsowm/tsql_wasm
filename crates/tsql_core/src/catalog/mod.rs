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

impl IdAllocator for CatalogImpl {
    fn alloc_table_id(&mut self) -> u32 {
        let id = self.next_table_id;
        self.next_table_id += 1;
        id
    }

    fn alloc_column_id(&mut self) -> u32 {
        let id = self.next_column_id;
        self.next_column_id += 1;
        id
    }

    fn alloc_index_id(&mut self) -> u32 {
        let id = self.next_index_id;
        self.next_index_id += 1;
        id
    }

    fn alloc_object_id(&mut self) -> i32 {
        let id = self.next_object_id;
        self.next_object_id -= 1;
        id
    }

    fn alloc_schema_id(&mut self) -> u32 {
        let id = self.next_schema_id;
        self.next_schema_id += 1;
        id
    }
}

impl SchemaRegistry for CatalogImpl {
    fn get_schemas(&self) -> &[SchemaDef] {
        &self.schemas
    }

    fn get_schema_id(&self, name: &str) -> Option<u32> {
        let idx = self.schema_map.get(&name.to_lowercase())?;
        Some(self.schemas[*idx].id)
    }

    fn create_schema(&mut self, name: &str) -> Result<(), DbError> {
        if self.get_schema_id(name).is_some() {
            return Err(DbError::Semantic(format!(
                "schema '{}' already exists",
                name
            )));
        }
        let id = self.alloc_schema_id();
        let idx = self.schemas.len();
        self.schemas.push(SchemaDef {
            id,
            name: name.to_string(),
        });
        self.schema_map.insert(name.to_lowercase(), idx);
        Ok(())
    }

    fn drop_schema(&mut self, name: &str) -> Result<(), DbError> {
        let schema_id = self
            .get_schema_id(name)
            .ok_or_else(|| DbError::Semantic(format!("schema '{}' not found", name)))?;

        let has_tables = self.tables.iter().any(|t| t.schema_id == schema_id);
        if has_tables {
            return Err(DbError::Semantic(format!(
                "schema '{}' cannot be dropped because it contains tables",
                name
            )));
        }

        self.schemas.retain(|s| s.id != schema_id);
        self.rebuild_maps();
        Ok(())
    }
}

impl TableRegistry for CatalogImpl {
    fn get_tables(&self) -> &[TableDef] {
        &self.tables
    }

    fn find_table(&self, schema: &str, name: &str) -> Option<&TableDef> {
        let schema_id = self.get_schema_id(schema)?;
        let idx = self.table_map.get(&(schema_id, name.to_lowercase()))?;
        Some(&self.tables[*idx])
    }

    fn find_table_mut(&mut self, schema: &str, name: &str) -> Option<&mut TableDef> {
        let schema_id = self.get_schema_id(schema)?;
        let idx = self.table_map.get(&(schema_id, name.to_lowercase()))?;
        Some(&mut self.tables[*idx])
    }

    fn register_table(&mut self, table: TableDef) {
        let idx = self.tables.len();
        self.table_map
            .insert((table.schema_id, table.name.to_lowercase()), idx);
        self.tables.push(table);
    }

    fn unregister_table_by_id(&mut self, id: u32) {
        self.tables.retain(|t| t.id != id);
        self.rebuild_maps();
    }

    fn drop_table(&mut self, schema: &str, name: &str) -> Result<u32, DbError> {
        let schema_id = self
            .get_schema_id(schema)
            .ok_or_else(|| DbError::Semantic(format!("schema '{}' not found", schema)))?;

        let idx = *self
            .table_map
            .get(&(schema_id, name.to_lowercase()))
            .ok_or_else(|| DbError::Semantic(format!("table '{}.{}' not found", schema, name)))?;

        let table_id = self.tables[idx].id;
        self.tables.remove(idx);
        self.indexes.retain(|idx| idx.table_id != table_id);
        self.rebuild_maps();
        Ok(table_id)
    }

    fn next_identity_value(&mut self, table_id: u32, column_name: &str) -> Result<i64, DbError> {
        let table = self
            .tables
            .iter_mut()
            .find(|t| t.id == table_id)
            .ok_or_else(|| DbError::Semantic(format!("table ID {} not found", table_id)))?;

        let col = table
            .columns
            .iter_mut()
            .find(|c| c.name.eq_ignore_ascii_case(column_name))
            .ok_or_else(|| DbError::Semantic(format!("column '{}' not found", column_name)))?;

        if let Some(identity) = &mut col.identity {
            Ok(identity.next_value())
        } else {
            Err(DbError::Execution(format!(
                "column '{}' is not an IDENTITY column",
                column_name
            )))
        }
    }
}

impl IndexRegistry for CatalogImpl {
    fn get_indexes(&self) -> &[IndexDef] {
        &self.indexes
    }

    fn register_index(&mut self, index: IndexDef) {
        self.indexes.push(index);
    }

    fn drop_index_by_table_id(&mut self, table_id: u32) {
        self.indexes.retain(|idx| idx.table_id != table_id);
    }

    fn create_index(
        &mut self,
        schema: &str,
        name: &str,
        table_schema: &str,
        table_name: &str,
        columns: &[String],
    ) -> Result<(), DbError> {
        let index_schema_id = self
            .get_schema_id(schema)
            .ok_or_else(|| DbError::Semantic(format!("schema '{}' not found", schema)))?;
        let table = self
            .find_table(table_schema, table_name)
            .ok_or_else(|| {
                DbError::Semantic(format!("table '{}.{}' not found", table_schema, table_name))
            })?
            .clone();

        if self.indexes.iter().any(|idx| {
            idx.schema_id == index_schema_id
                && idx.table_id == table.id
                && idx.name.eq_ignore_ascii_case(name)
        }) {
            return Err(DbError::Semantic(format!(
                "index '{}.{}' already exists",
                schema, name
            )));
        }

        let mut column_ids = Vec::new();
        for column in columns {
            let col_id = table
                .columns
                .iter()
                .find(|c| c.name.eq_ignore_ascii_case(column))
                .map(|c| c.id)
                .ok_or_else(|| DbError::Semantic(format!("column '{}' not found", column)))?;
            column_ids.push(col_id);
        }

        let new_index_id = self.alloc_index_id();
        self.indexes.push(IndexDef {
            id: new_index_id,
            schema_id: index_schema_id,
            table_id: table.id,
            name: name.to_string(),
            column_ids,
            is_unique: false,
            is_clustered: false,
        });
        self.index_map
            .insert((index_schema_id, name.to_lowercase()), self.indexes.len() - 1);
        Ok(())
    }

    fn drop_index(
        &mut self,
        schema: &str,
        name: &str,
        table_schema: &str,
        table_name: &str,
    ) -> Result<(), DbError> {
        let schema_id = self
            .get_schema_id(schema)
            .ok_or_else(|| DbError::Semantic(format!("schema '{}' not found", schema)))?;
        let table_id = self
            .find_table(table_schema, table_name)
            .map(|t| t.id)
            .ok_or_else(|| {
                DbError::Semantic(format!("table '{}.{}' not found", table_schema, table_name))
            })?;

        let Some(pos) = self.indexes.iter().position(|idx| {
            idx.schema_id == schema_id
                && idx.table_id == table_id
                && idx.name.eq_ignore_ascii_case(name)
        }) else {
            return Err(DbError::Semantic(format!(
                "index '{}.{}' not found",
                schema, name
            )));
        };

        self.indexes.remove(pos);
        self.rebuild_maps();
        Ok(())
    }
}

impl RoutineRegistry for CatalogImpl {
    fn get_routines(&self) -> &[RoutineDef] {
        &self.routines
    }

    fn find_routine(&self, schema: &str, name: &str) -> Option<&RoutineDef> {
        let idx = self
            .routine_map
            .get(&(schema.to_lowercase(), name.to_lowercase()))?;
        Some(&self.routines[*idx])
    }

    fn create_routine(&mut self, routine: RoutineDef) -> Result<(), DbError> {
        if self.find_routine(&routine.schema, &routine.name).is_some() {
            return Err(DbError::Semantic(format!(
                "routine '{}.{}' already exists",
                routine.schema, routine.name
            )));
        }
        let idx = self.routines.len();
        self.routine_map.insert(
            (routine.schema.to_lowercase(), routine.name.to_lowercase()),
            idx,
        );
        self.routines.push(routine);
        Ok(())
    }

    fn drop_routine(
        &mut self,
        schema: &str,
        name: &str,
        expect_function: bool,
    ) -> Result<(), DbError> {
        let Some(idx) = self
            .routine_map
            .get(&(schema.to_lowercase(), name.to_lowercase()))
            .copied()
        else {
            let kind = if expect_function {
                "function"
            } else {
                "procedure"
            };
            return Err(DbError::Semantic(format!(
                "{} '{}.{}' not found",
                kind, schema, name
            )));
        };

        let is_function = matches!(self.routines[idx].kind, RoutineKind::Function { .. });
        if is_function != expect_function {
            return Err(DbError::Semantic(format!(
                "'{}.{}' has different routine kind",
                schema, name
            )));
        }
        self.routines.remove(idx);
        self.rebuild_maps();
        Ok(())
    }
}

impl TypeRegistry for CatalogImpl {
    fn get_table_types(&self) -> &[TableTypeDef] {
        &self.table_types
    }

    fn find_table_type(&self, schema: &str, name: &str) -> Option<&TableTypeDef> {
        let idx = self
            .type_map
            .get(&(schema.to_lowercase(), name.to_lowercase()))?;
        Some(&self.table_types[*idx])
    }

    fn create_table_type(&mut self, def: TableTypeDef) -> Result<(), DbError> {
        if self.find_table_type(&def.schema, &def.name).is_some() {
            return Err(DbError::Semantic(format!(
                "type '{}.{}' already exists",
                def.schema, def.name
            )));
        }
        let idx = self.table_types.len();
        self.type_map
            .insert((def.schema.to_lowercase(), def.name.to_lowercase()), idx);
        self.table_types.push(def);
        Ok(())
    }

    fn drop_table_type(&mut self, schema: &str, name: &str) -> Result<(), DbError> {
        let Some(idx) = self
            .type_map
            .get(&(schema.to_lowercase(), name.to_lowercase()))
            .copied()
        else {
            return Err(DbError::Semantic(format!(
                "type '{}.{}' not found",
                schema, name
            )));
        };
        self.table_types.remove(idx);
        self.rebuild_maps();
        Ok(())
    }
}

impl ViewRegistry for CatalogImpl {
    fn get_views(&self) -> &[ViewDef] {
        &self.views
    }

    fn find_view(&self, schema: &str, name: &str) -> Option<&ViewDef> {
        let idx = self
            .view_map
            .get(&(schema.to_lowercase(), name.to_lowercase()))?;
        Some(&self.views[*idx])
    }

    fn create_view(&mut self, view: ViewDef) -> Result<(), DbError> {
        if self.find_view(&view.schema, &view.name).is_some() {
            return Err(DbError::Semantic(format!(
                "view '{}.{}' already exists",
                view.schema, view.name
            )));
        }
        let idx = self.views.len();
        self.view_map
            .insert((view.schema.to_lowercase(), view.name.to_lowercase()), idx);
        self.views.push(view);
        Ok(())
    }

    fn drop_view(&mut self, schema: &str, name: &str) -> Result<(), DbError> {
        let idx = *self
            .view_map
            .get(&(schema.to_lowercase(), name.to_lowercase()))
            .ok_or_else(|| DbError::Semantic(format!("view '{}.{}' not found", schema, name)))?;
        self.views.remove(idx);
        self.rebuild_maps();
        Ok(())
    }
}

impl TriggerRegistry for CatalogImpl {
    fn get_triggers(&self) -> &[TriggerDef] {
        &self.triggers
    }

    fn find_triggers_for_table(&self, schema: &str, name: &str) -> Vec<&TriggerDef> {
        self.triggers
            .iter()
            .filter(|t| {
                t.table_schema.eq_ignore_ascii_case(schema)
                    && t.table_name.eq_ignore_ascii_case(name)
            })
            .collect()
    }

    fn create_trigger(&mut self, trigger: TriggerDef) -> Result<(), DbError> {
        if self
            .trigger_map
            .get(&(trigger.schema.to_lowercase(), trigger.name.to_lowercase()))
            .is_some()
        {
            return Err(DbError::Semantic(format!(
                "trigger '{}.{}' already exists",
                trigger.schema, trigger.name
            )));
        }
        let idx = self.triggers.len();
        self.trigger_map.insert(
            (trigger.schema.to_lowercase(), trigger.name.to_lowercase()),
            idx,
        );
        self.triggers.push(trigger);
        Ok(())
    }

    fn drop_trigger(&mut self, schema: &str, name: &str) -> Result<(), DbError> {
        let idx = *self
            .trigger_map
            .get(&(schema.to_lowercase(), name.to_lowercase()))
            .ok_or_else(|| DbError::Semantic(format!("trigger '{}.{}' not found", schema, name)))?;
        self.triggers.remove(idx);
        self.rebuild_maps();
        Ok(())
    }
}

impl ObjectResolver for CatalogImpl {
    fn object_id(&self, schema: &str, name: &str) -> Option<i32> {
        if let Some(table) = self.find_table(schema, name) {
            return Some(table.id as i32);
        }
        if let Some(schema_id) = self.get_schema_id(schema) {
            if let Some(idx) = self
                .index_map
                .get(&(schema_id, name.to_lowercase()))
            {
                return Some(self.indexes[*idx].id as i32);
            }
        }
        if let Some(routine) = self.find_routine(schema, name) {
            return Some(routine.object_id);
        }
        if let Some(view) = self.find_view(schema, name) {
            return Some(view.object_id);
        }
        let trigger_idx = self
            .trigger_map
            .get(&(schema.to_lowercase(), name.to_lowercase()))?;
        Some(self.triggers[*trigger_idx].object_id)
    }
}

impl Catalog for CatalogImpl {
    fn clone_boxed(&self) -> Box<dyn Catalog> {
        Box::new(self.clone())
    }

    fn rebuild_maps(&mut self) {
        self.rebuild_maps();
    }
}

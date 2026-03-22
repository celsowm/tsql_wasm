use crate::ast::Expr;
use crate::ast::{DataTypeSpec, FunctionBody, RoutineParam, Statement};
use crate::error::DbError;
use crate::types::DataType;

#[derive(Debug, Clone)]
pub struct SchemaDef {
    pub id: u32,
    pub name: String,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct CheckConstraintDef {
    pub name: String,
    pub expr: Expr,
}

#[derive(Debug, Clone)]
pub struct IndexDef {
    pub id: u32,
    pub schema_id: u32,
    pub table_id: u32,
    pub name: String,
    pub column_ids: Vec<u32>,
    pub is_unique: bool,
    pub is_clustered: bool,
}

#[derive(Debug, Clone)]
pub struct TableDef {
    pub id: u32,
    pub schema_id: u32,
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub check_constraints: Vec<CheckConstraintDef>,
}

#[derive(Debug, Clone)]
pub enum RoutineKind {
    Procedure {
        body: Vec<Statement>,
    },
    Function {
        returns: Option<DataTypeSpec>,
        body: FunctionBody,
    },
}

#[derive(Debug, Clone)]
pub struct RoutineDef {
    pub schema: String,
    pub name: String,
    pub params: Vec<RoutineParam>,
    pub kind: RoutineKind,
}

pub trait Catalog: std::fmt::Debug + Send + Sync {
    fn get_schemas(&self) -> &[SchemaDef];
    fn get_tables(&self) -> &[TableDef];
    fn get_indexes(&self) -> &[IndexDef];
    fn get_tables_mut(&mut self) -> &mut Vec<TableDef>;
    fn get_indexes_mut(&mut self) -> &mut Vec<IndexDef>;
    fn alloc_table_id(&mut self) -> u32;
    fn alloc_column_id(&mut self) -> u32;
    fn alloc_index_id(&mut self) -> u32;
    fn get_schema_id(&self, name: &str) -> Option<u32>;
    fn find_table(&self, schema: &str, name: &str) -> Option<&TableDef>;
    fn find_table_mut(&mut self, schema: &str, name: &str) -> Option<&mut TableDef>;
    fn create_schema(&mut self, name: &str) -> Result<(), DbError>;
    fn drop_schema(&mut self, name: &str) -> Result<(), DbError>;
    fn drop_table(&mut self, schema: &str, name: &str) -> Result<u32, DbError>;
    fn create_index(
        &mut self,
        schema: &str,
        name: &str,
        table_schema: &str,
        table_name: &str,
        columns: &[String],
    ) -> Result<(), DbError>;
    fn drop_index(
        &mut self,
        schema: &str,
        name: &str,
        table_schema: &str,
        table_name: &str,
    ) -> Result<(), DbError>;
    fn object_id(&self, schema: &str, name: &str) -> Option<i32>;
    fn next_identity_value(&mut self, table_id: u32, column_name: &str) -> Result<i64, DbError>;
    fn create_routine(&mut self, routine: RoutineDef) -> Result<(), DbError>;
    fn drop_routine(
        &mut self,
        schema: &str,
        name: &str,
        expect_function: bool,
    ) -> Result<(), DbError>;
    fn find_routine(&self, schema: &str, name: &str) -> Option<&RoutineDef>;
}

#[derive(Debug, Default, Clone)]
pub struct CatalogImpl {
    pub schemas: Vec<SchemaDef>,
    pub tables: Vec<TableDef>,
    pub indexes: Vec<IndexDef>,
    pub routines: Vec<RoutineDef>,
    next_schema_id: u32,
    next_table_id: u32,
    next_column_id: u32,
    next_index_id: u32,
}

impl CatalogImpl {
    pub fn new() -> Self {
        let mut c = Self {
            next_schema_id: 1,
            next_table_id: 1,
            next_column_id: 1,
            next_index_id: 1,
            ..Default::default()
        };
        let dbo_id = c.alloc_schema_id();
        c.schemas.push(SchemaDef {
            id: dbo_id,
            name: "dbo".to_string(),
        });
        c
    }

    fn alloc_schema_id(&mut self) -> u32 {
        let id = self.next_schema_id;
        self.next_schema_id += 1;
        id
    }
}

impl Catalog for CatalogImpl {
    fn get_schemas(&self) -> &[SchemaDef] {
        &self.schemas
    }

    fn get_tables(&self) -> &[TableDef] {
        &self.tables
    }

    fn get_indexes(&self) -> &[IndexDef] {
        &self.indexes
    }

    fn get_tables_mut(&mut self) -> &mut Vec<TableDef> {
        &mut self.tables
    }

    fn get_indexes_mut(&mut self) -> &mut Vec<IndexDef> {
        &mut self.indexes
    }

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

    fn get_schema_id(&self, name: &str) -> Option<u32> {
        self.schemas
            .iter()
            .find(|s| s.name.eq_ignore_ascii_case(name))
            .map(|s| s.id)
    }

    fn find_table(&self, schema: &str, name: &str) -> Option<&TableDef> {
        let schema_id = self.get_schema_id(schema)?;
        self.tables
            .iter()
            .find(|t| t.schema_id == schema_id && t.name.eq_ignore_ascii_case(name))
    }

    fn find_table_mut(&mut self, schema: &str, name: &str) -> Option<&mut TableDef> {
        let schema_id = self.get_schema_id(schema)?;
        self.tables
            .iter_mut()
            .find(|t| t.schema_id == schema_id && t.name.eq_ignore_ascii_case(name))
    }

    fn create_schema(&mut self, name: &str) -> Result<(), DbError> {
        if self.get_schema_id(name).is_some() {
            return Err(DbError::Semantic(format!(
                "schema '{}' already exists",
                name
            )));
        }
        let id = self.alloc_schema_id();
        self.schemas.push(SchemaDef {
            id,
            name: name.to_string(),
        });
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
        Ok(())
    }

    fn drop_table(&mut self, schema: &str, name: &str) -> Result<u32, DbError> {
        let schema_id = self
            .get_schema_id(schema)
            .ok_or_else(|| DbError::Semantic(format!("schema '{}' not found", schema)))?;

        let pos = self
            .tables
            .iter()
            .position(|t| t.schema_id == schema_id && t.name.eq_ignore_ascii_case(name))
            .ok_or_else(|| DbError::Semantic(format!("table '{}.{}' not found", schema, name)))?;

        let table_id = self.tables[pos].id;
        self.tables.remove(pos);
        self.indexes.retain(|idx| idx.table_id != table_id);
        Ok(table_id)
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
        Ok(())
    }

    fn object_id(&self, schema: &str, name: &str) -> Option<i32> {
        if let Some(table) = self.find_table(schema, name) {
            return Some(table.id as i32);
        }
        if let Some(schema_id) = self.get_schema_id(schema) {
            if let Some(idx) = self
                .indexes
                .iter()
                .find(|idx| idx.schema_id == schema_id && idx.name.eq_ignore_ascii_case(name))
            {
                return Some(idx.id as i32);
            }
        }
        None
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

    fn create_routine(&mut self, routine: RoutineDef) -> Result<(), DbError> {
        if self.routines.iter().any(|r| {
            r.schema.eq_ignore_ascii_case(&routine.schema)
                && r.name.eq_ignore_ascii_case(&routine.name)
        }) {
            return Err(DbError::Semantic(format!(
                "routine '{}.{}' already exists",
                routine.schema, routine.name
            )));
        }
        self.routines.push(routine);
        Ok(())
    }

    fn drop_routine(
        &mut self,
        schema: &str,
        name: &str,
        expect_function: bool,
    ) -> Result<(), DbError> {
        let Some(pos) = self.routines.iter().position(|r| {
            r.schema.eq_ignore_ascii_case(schema) && r.name.eq_ignore_ascii_case(name)
        }) else {
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

        let is_function = matches!(self.routines[pos].kind, RoutineKind::Function { .. });
        if is_function != expect_function {
            return Err(DbError::Semantic(format!(
                "'{}.{}' has different routine kind",
                schema, name
            )));
        }
        self.routines.remove(pos);
        Ok(())
    }

    fn find_routine(&self, schema: &str, name: &str) -> Option<&RoutineDef> {
        self.routines
            .iter()
            .find(|r| r.schema.eq_ignore_ascii_case(schema) && r.name.eq_ignore_ascii_case(name))
    }
}

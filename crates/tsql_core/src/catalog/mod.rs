use crate::ast::Expr;
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
}

#[derive(Debug, Clone)]
pub struct TableDef {
    pub id: u32,
    pub schema_id: u32,
    pub name: String,
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, Default)]
pub struct Catalog {
    pub schemas: Vec<SchemaDef>,
    pub tables: Vec<TableDef>,
    next_schema_id: u32,
    next_table_id: u32,
    next_column_id: u32,
}

impl Catalog {
    pub fn new() -> Self {
        let mut c = Self {
            next_schema_id: 1,
            next_table_id: 1,
            next_column_id: 1,
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

    pub fn alloc_table_id(&mut self) -> u32 {
        let id = self.next_table_id;
        self.next_table_id += 1;
        id
    }

    pub fn alloc_column_id(&mut self) -> u32 {
        let id = self.next_column_id;
        self.next_column_id += 1;
        id
    }

    pub fn get_schema_id(&self, name: &str) -> Option<u32> {
        self.schemas
            .iter()
            .find(|s| s.name.eq_ignore_ascii_case(name))
            .map(|s| s.id)
    }

    pub fn find_table(&self, schema: &str, name: &str) -> Option<&TableDef> {
        let schema_id = self.get_schema_id(schema)?;
        self.tables
            .iter()
            .find(|t| t.schema_id == schema_id && t.name.eq_ignore_ascii_case(name))
    }

    pub fn find_table_mut(&mut self, schema: &str, name: &str) -> Option<&mut TableDef> {
        let schema_id = self.get_schema_id(schema)?;
        self.tables
            .iter_mut()
            .find(|t| t.schema_id == schema_id && t.name.eq_ignore_ascii_case(name))
    }

    pub fn create_schema(&mut self, name: &str) -> Result<(), crate::error::DbError> {
        if self.get_schema_id(name).is_some() {
            return Err(crate::error::DbError::Semantic(format!(
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

    pub fn drop_schema(&mut self, name: &str) -> Result<(), crate::error::DbError> {
        let schema_id = self.get_schema_id(name).ok_or_else(|| {
            crate::error::DbError::Semantic(format!("schema '{}' not found", name))
        })?;

        let has_tables = self.tables.iter().any(|t| t.schema_id == schema_id);
        if has_tables {
            return Err(crate::error::DbError::Semantic(format!(
                "schema '{}' cannot be dropped because it contains tables",
                name
            )));
        }

        self.schemas.retain(|s| s.id != schema_id);
        Ok(())
    }

    pub fn drop_table(&mut self, schema: &str, name: &str) -> Result<u32, crate::error::DbError> {
        let schema_id = self.get_schema_id(schema).ok_or_else(|| {
            crate::error::DbError::Semantic(format!("schema '{}' not found", schema))
        })?;

        let pos = self
            .tables
            .iter()
            .position(|t| t.schema_id == schema_id && t.name.eq_ignore_ascii_case(name))
            .ok_or_else(|| {
                crate::error::DbError::Semantic(format!("table '{}.{}' not found", schema, name))
            })?;

        let table_id = self.tables[pos].id;
        self.tables.remove(pos);
        Ok(table_id)
    }
}

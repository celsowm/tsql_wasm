use super::*;
use crate::error::DbError;

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

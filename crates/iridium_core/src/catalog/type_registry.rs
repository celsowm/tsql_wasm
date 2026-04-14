use super::*;
use crate::error::DbError;
use crate::executor::string_norm::normalize_identifier;

impl TypeRegistry for CatalogImpl {
    fn get_table_types(&self) -> &[TableTypeDef] {
        &self.table_types
    }

    fn find_table_type(&self, schema: &str, name: &str) -> Option<&TableTypeDef> {
        let idx = self
            .type_map
            .get(&(normalize_identifier(schema), normalize_identifier(name)))?;
        Some(&self.table_types[*idx])
    }

    fn create_table_type(&mut self, def: TableTypeDef) -> Result<(), DbError> {
        if self.find_table_type(&def.schema, &def.name).is_some() {
            return Err(DbError::duplicate_type(&def.schema, &def.name));
        }
        let idx = self.table_types.len();
        self.type_map.insert(
            (
                normalize_identifier(&def.schema),
                normalize_identifier(&def.name),
            ),
            idx,
        );
        self.table_types.push(def);
        Ok(())
    }

    fn drop_table_type(&mut self, schema: &str, name: &str) -> Result<(), DbError> {
        let Some(idx) = self
            .type_map
            .get(&(normalize_identifier(schema), normalize_identifier(name)))
            .copied()
        else {
            return Err(DbError::type_not_found(schema, name));
        };
        self.table_types.remove(idx);
        self.rebuild_maps();
        Ok(())
    }
}

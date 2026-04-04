use super::*;
use crate::error::DbError;

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

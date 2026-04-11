use super::*;
use crate::error::DbError;
use crate::executor::string_norm::normalize_identifier;

impl TableRegistry for CatalogImpl {
    fn get_tables(&self) -> &[TableDef] {
        &self.tables
    }

    fn find_table(&self, schema: &str, name: &str) -> Option<&TableDef> {
        let schema_id = self.get_schema_id(schema)?;
        let idx = self
            .table_map
            .get(&(schema_id, normalize_identifier(name)))?;
        Some(&self.tables[*idx])
    }

    fn find_table_mut(&mut self, schema: &str, name: &str) -> Option<&mut TableDef> {
        let schema_id = self.get_schema_id(schema)?;
        let idx = self
            .table_map
            .get(&(schema_id, normalize_identifier(name)))?;
        Some(&mut self.tables[*idx])
    }

    fn register_table(&mut self, table: TableDef) {
        let idx = self.tables.len();
        self.table_map
            .insert((table.schema_id, normalize_identifier(&table.name)), idx);
        self.tables.push(table);
    }

    fn unregister_table_by_id(&mut self, id: u32) {
        if let Some(idx) = self.tables.iter().position(|t| t.id == id) {
            self.remove_table_at(idx);
        }
    }

    fn drop_table(&mut self, schema: &str, name: &str) -> Result<u32, DbError> {
        let schema_id = self
            .get_schema_id(schema)
            .ok_or_else(|| DbError::schema_not_found(schema))?;

        let idx = *self
            .table_map
            .get(&(schema_id, normalize_identifier(name)))
            .ok_or_else(|| DbError::table_not_found(schema, name))?;

        let table_id = self.tables[idx].id;
        self.remove_table_at(idx);
        Ok(table_id)
    }

    fn next_identity_value(&mut self, table_id: u32, column_name: &str) -> Result<i64, DbError> {
        let table = self
            .tables
            .iter_mut()
            .find(|t| t.id == table_id)
            .ok_or_else(|| DbError::object_not_found(format!("table ID {}", table_id)))?;

        let col = table
            .columns
            .iter_mut()
            .find(|c| c.name.eq_ignore_ascii_case(column_name))
            .ok_or_else(|| DbError::column_not_found(column_name))?;

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

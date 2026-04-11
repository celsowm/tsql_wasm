use super::*;
use crate::error::DbError;

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
            .ok_or_else(|| DbError::schema_not_found(schema))?;
        let table = self
            .find_table(table_schema, table_name)
            .ok_or_else(|| DbError::table_not_found(table_schema, table_name))?
            .clone();

        if self.indexes.iter().any(|idx| {
            idx.schema_id == index_schema_id
                && idx.table_id == table.id
                && idx.name.eq_ignore_ascii_case(name)
        }) {
            return Err(DbError::Execution(format!(
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
                .ok_or_else(|| DbError::column_not_found(column))?;
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
        self.index_map.insert(
            (index_schema_id, name.to_lowercase()),
            self.indexes.len() - 1,
        );
        Ok(())
    }

    fn create_index_with_options(
        &mut self,
        schema: &str,
        name: &str,
        table_schema: &str,
        table_name: &str,
        columns: &[String],
        is_clustered: bool,
        is_unique: bool,
    ) -> Result<(), DbError> {
        let index_schema_id = self
            .get_schema_id(schema)
            .ok_or_else(|| DbError::schema_not_found(schema))?;
        let table = self
            .find_table(table_schema, table_name)
            .ok_or_else(|| DbError::table_not_found(table_schema, table_name))?
            .clone();

        if self.indexes.iter().any(|idx| {
            idx.schema_id == index_schema_id
                && idx.table_id == table.id
                && idx.name.eq_ignore_ascii_case(name)
        }) {
            return Err(DbError::Execution(format!(
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
                .ok_or_else(|| DbError::column_not_found(column))?;
            column_ids.push(col_id);
        }

        // For clustered index, use index_id = 1; for non-clustered, allocate new ID
        let new_index_id = if is_clustered { 1 } else { self.alloc_index_id() };
        
        self.indexes.push(IndexDef {
            id: new_index_id,
            schema_id: index_schema_id,
            table_id: table.id,
            name: name.to_string(),
            column_ids,
            is_unique,
            is_clustered,
        });
        self.index_map.insert(
            (index_schema_id, name.to_lowercase()),
            self.indexes.len() - 1,
        );
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
            .ok_or_else(|| DbError::schema_not_found(schema))?;
        let table_id = self
            .find_table(table_schema, table_name)
            .map(|t| t.id)
            .ok_or_else(|| DbError::table_not_found(table_schema, table_name))?;

        let Some(pos) = self.indexes.iter().position(|idx| {
            idx.schema_id == schema_id
                && idx.table_id == table_id
                && idx.name.eq_ignore_ascii_case(name)
        }) else {
            return Err(DbError::index_not_found(table_name, name));
        };

        self.indexes.remove(pos);
        self.rebuild_maps();
        Ok(())
    }
}

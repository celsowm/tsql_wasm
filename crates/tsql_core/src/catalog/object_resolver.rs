use super::*;
use crate::executor::string_norm::normalize_identifier;

impl ObjectResolver for CatalogImpl {
    fn object_id(&self, schema: &str, name: &str) -> Option<i32> {
        if let Some(table) = self.find_table(schema, name) {
            return Some(table.id as i32);
        }
        if let Some(schema_id) = self.get_schema_id(schema) {
            if let Some(idx) = self.index_map.get(&(schema_id, normalize_identifier(name))) {
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
            .get(&(normalize_identifier(schema), normalize_identifier(name)))?;
        Some(self.triggers[*trigger_idx].object_id)
    }
}

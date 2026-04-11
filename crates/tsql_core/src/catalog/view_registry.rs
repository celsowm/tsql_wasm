use super::*;
use crate::error::DbError;
use crate::executor::string_norm::normalize_identifier;

impl ViewRegistry for CatalogImpl {
    fn get_views(&self) -> &[ViewDef] {
        &self.views
    }

    fn find_view(&self, schema: &str, name: &str) -> Option<&ViewDef> {
        let idx = self
            .view_map
            .get(&(normalize_identifier(schema), normalize_identifier(name)))?;
        Some(&self.views[*idx])
    }

    fn create_view(&mut self, view: ViewDef) -> Result<(), DbError> {
        if self.find_view(&view.schema, &view.name).is_some() {
            return Err(DbError::duplicate_view(&view.schema, &view.name));
        }
        let idx = self.views.len();
        self.view_map
            .insert((normalize_identifier(&view.schema), normalize_identifier(&view.name)), idx);
        self.views.push(view);
        Ok(())
    }

    fn drop_view(&mut self, schema: &str, name: &str) -> Result<(), DbError> {
        let idx = *self
            .view_map
            .get(&(normalize_identifier(schema), normalize_identifier(name)))
            .ok_or_else(|| DbError::view_not_found(schema, name))?;
        self.views.remove(idx);
        self.rebuild_maps();
        Ok(())
    }
}

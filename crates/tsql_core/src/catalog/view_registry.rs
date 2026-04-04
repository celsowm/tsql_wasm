use super::*;
use crate::error::DbError;

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

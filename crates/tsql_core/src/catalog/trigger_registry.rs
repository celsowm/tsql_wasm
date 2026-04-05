use super::*;
use crate::error::DbError;

impl TriggerRegistry for CatalogImpl {
    fn get_triggers(&self) -> &[TriggerDef] {
        &self.triggers
    }

    fn find_triggers_for_table(&self, schema: &str, name: &str) -> Vec<&TriggerDef> {
        self.triggers
            .iter()
            .filter(|t| {
                t.table_schema.eq_ignore_ascii_case(schema)
                    && t.table_name.eq_ignore_ascii_case(name)
            })
            .collect()
    }

    fn create_trigger(&mut self, trigger: TriggerDef) -> Result<(), DbError> {
        if self
            .trigger_map
            .get(&(trigger.schema.to_lowercase(), trigger.name.to_lowercase()))
            .is_some()
        {
            return Err(DbError::duplicate_trigger(&trigger.schema, &trigger.name));
        }
        let idx = self.triggers.len();
        self.trigger_map.insert(
            (trigger.schema.to_lowercase(), trigger.name.to_lowercase()),
            idx,
        );
        self.triggers.push(trigger);
        Ok(())
    }

    fn drop_trigger(&mut self, schema: &str, name: &str) -> Result<(), DbError> {
        let idx = *self
            .trigger_map
            .get(&(schema.to_lowercase(), name.to_lowercase()))
            .ok_or_else(|| DbError::trigger_not_found(schema, name))?;
        self.triggers.remove(idx);
        self.rebuild_maps();
        Ok(())
    }
}

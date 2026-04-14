use super::*;
use crate::error::DbError;
use crate::executor::string_norm::normalize_identifier;

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
        if self.trigger_map.contains_key(&(
            normalize_identifier(&trigger.schema),
            normalize_identifier(&trigger.name),
        )) {
            return Err(DbError::duplicate_trigger(&trigger.schema, &trigger.name));
        }
        let idx = self.triggers.len();
        self.trigger_map.insert(
            (
                normalize_identifier(&trigger.schema),
                normalize_identifier(&trigger.name),
            ),
            idx,
        );
        self.triggers.push(trigger);
        Ok(())
    }

    fn drop_trigger(&mut self, schema: &str, name: &str) -> Result<(), DbError> {
        let idx = *self
            .trigger_map
            .get(&(normalize_identifier(schema), normalize_identifier(name)))
            .ok_or_else(|| DbError::trigger_not_found(schema, name))?;
        self.triggers.remove(idx);
        self.rebuild_maps();
        Ok(())
    }
}

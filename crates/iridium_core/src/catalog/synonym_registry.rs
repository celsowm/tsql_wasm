use super::*;
use crate::error::DbError;
use crate::executor::string_norm::normalize_identifier;

impl SynonymRegistry for CatalogImpl {
    fn get_synonyms(&self) -> &[SynonymDef] {
        &self.synonyms
    }

    fn find_synonym(&self, schema: &str, name: &str) -> Option<&SynonymDef> {
        let idx = self.synonym_map.get(&(
            normalize_identifier(schema),
            normalize_identifier(name),
        ))?;
        Some(&self.synonyms[*idx])
    }

    fn create_synonym(&mut self, synonym: SynonymDef) -> Result<(), DbError> {
        if self.find_synonym(&synonym.schema, &synonym.name).is_some() {
            return Err(DbError::Execution(format!(
                "synonym '{}' already exists in schema '{}'",
                synonym.name, synonym.schema
            )));
        }

        let idx = self.synonyms.len();
        self.synonym_map.insert(
            (
                normalize_identifier(&synonym.schema),
                normalize_identifier(&synonym.name),
            ),
            idx,
        );
        self.synonyms.push(synonym);
        Ok(())
    }

    fn drop_synonym(&mut self, schema: &str, name: &str) -> Result<(), DbError> {
        let key = (normalize_identifier(schema), normalize_identifier(name));
        let idx = self
            .synonym_map
            .get(&key)
            .ok_or_else(|| DbError::object_not_found(format!("{}.{}", schema, name)))?;

        let idx = *idx;
        self.synonyms.remove(idx);
        self.rebuild_maps();
        Ok(())
    }
}

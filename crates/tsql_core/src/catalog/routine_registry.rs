use super::*;
use crate::error::DbError;
use crate::executor::string_norm::normalize_identifier;

impl RoutineRegistry for CatalogImpl {
    fn get_routines(&self) -> &[RoutineDef] {
        &self.routines
    }

    fn find_routine(&self, schema: &str, name: &str) -> Option<&RoutineDef> {
        let idx = self
            .routine_map
            .get(&(normalize_identifier(schema), normalize_identifier(name)))?;
        Some(&self.routines[*idx])
    }

    fn create_routine(&mut self, routine: RoutineDef) -> Result<(), DbError> {
        if self.find_routine(&routine.schema, &routine.name).is_some() {
            return Err(DbError::object_not_found(format!(
                "routine '{}.{}'",
                routine.schema, routine.name
            )));
        }
        let idx = self.routines.len();
        self.routine_map.insert(
            (
                normalize_identifier(&routine.schema),
                normalize_identifier(&routine.name),
            ),
            idx,
        );
        self.routines.push(routine);
        Ok(())
    }

    fn drop_routine(
        &mut self,
        schema: &str,
        name: &str,
        expect_function: bool,
    ) -> Result<(), DbError> {
        let Some(idx) = self
            .routine_map
            .get(&(normalize_identifier(schema), normalize_identifier(name)))
            .copied()
        else {
            let kind = if expect_function {
                "function"
            } else {
                "procedure"
            };
            return Err(DbError::object_not_found(format!(
                "{} '{}.{}'",
                kind, schema, name
            )));
        };

        let is_function = matches!(self.routines[idx].kind, RoutineKind::Function { .. });
        if is_function != expect_function {
            return Err(DbError::invalid_identifier(format!(
                "'{}.{}' has different routine kind",
                schema, name
            )));
        }
        self.routines.remove(idx);
        self.rebuild_maps();
        Ok(())
    }
}

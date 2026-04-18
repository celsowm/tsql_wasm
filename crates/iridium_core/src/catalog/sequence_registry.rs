use super::*;
use crate::error::DbError;
use crate::executor::string_norm::normalize_identifier;

impl SequenceRegistry for CatalogImpl {
    fn get_sequences(&self) -> &[SequenceDef] {
        &self.sequences
    }

    fn find_sequence(&self, schema: &str, name: &str) -> Option<&SequenceDef> {
        let idx = self
            .sequence_map
            .get(&(normalize_identifier(schema), normalize_identifier(name)))?;
        Some(&self.sequences[*idx])
    }

    fn create_sequence(&mut self, sequence: SequenceDef) -> Result<(), DbError> {
        if self
            .find_sequence(&sequence.schema, &sequence.name)
            .is_some()
        {
            return Err(DbError::Execution(format!(
                "sequence '{}' already exists in schema '{}'",
                sequence.name, sequence.schema
            )));
        }

        let idx = self.sequences.len();
        self.sequence_map.insert(
            (
                normalize_identifier(&sequence.schema),
                normalize_identifier(&sequence.name),
            ),
            idx,
        );
        self.sequences.push(sequence);
        Ok(())
    }

    fn drop_sequence(&mut self, schema: &str, name: &str) -> Result<(), DbError> {
        let key = (normalize_identifier(schema), normalize_identifier(name));
        let idx = self
            .sequence_map
            .get(&key)
            .ok_or_else(|| DbError::object_not_found(format!("{}.{}", schema, name)))?;

        let idx = *idx;
        self.sequences.remove(idx);
        self.rebuild_maps();
        Ok(())
    }

    fn next_sequence_value(&mut self, schema: &str, name: &str) -> Result<i64, DbError> {
        let key = (normalize_identifier(schema), normalize_identifier(name));
        let idx = self
            .sequence_map
            .get(&key)
            .ok_or_else(|| DbError::object_not_found(format!("{}.{}", schema, name)))?;

        let seq = &mut self.sequences[*idx];
        let val = seq.current_value;

        let next_val = if seq.increment >= 0 {
            if val > seq.maximum_value - seq.increment {
                if seq.is_cycling {
                    seq.minimum_value
                } else {
                    return Err(DbError::Execution(format!(
                        "sequence '{}' has reached its maximum value",
                        name
                    )));
                }
            } else {
                val + seq.increment
            }
        } else {
            // Negative increment
            if val < seq.minimum_value - seq.increment {
                if seq.is_cycling {
                    seq.maximum_value
                } else {
                    return Err(DbError::Execution(format!(
                        "sequence '{}' has reached its minimum value",
                        name
                    )));
                }
            } else {
                val + seq.increment
            }
        };

        seq.current_value = next_val;
        Ok(val)
    }
}

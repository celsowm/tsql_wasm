use std::collections::HashMap;

use crate::ast::{Expr, JoinType};
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::executor::clock::Clock;
use crate::executor::context::ExecutionContext;
use crate::executor::evaluator::eval_predicate;
use crate::executor::joins::eval_key;
use crate::executor::model::{BoundTable, ContextTable, JoinedRow};
use crate::storage::Storage;
use crate::types::Value;

pub trait RowIterator {
    fn next_row(
        &mut self,
        ctx: &mut ExecutionContext,
        catalog: &dyn Catalog,
        storage: &dyn Storage,
        clock: &dyn Clock,
    ) -> Result<Option<JoinedRow>, DbError>;

    fn reset(&mut self) -> Result<(), DbError>;
}

pub struct ScanIterator {
    pub rows: Vec<JoinedRow>,
    pub current_index: usize,
}

impl ScanIterator {
    pub fn new(rows: Vec<JoinedRow>) -> Self {
        Self {
            rows,
            current_index: 0,
        }
    }
}

impl RowIterator for ScanIterator {
    fn next_row(
        &mut self,
        _ctx: &mut ExecutionContext,
        _catalog: &dyn Catalog,
        _storage: &dyn Storage,
        _clock: &dyn Clock,
    ) -> Result<Option<JoinedRow>, DbError> {
        if self.current_index < self.rows.len() {
            let row = self.rows[self.current_index].clone();
            self.current_index += 1;
            Ok(Some(row))
        } else {
            Ok(None)
        }
    }

    fn reset(&mut self) -> Result<(), DbError> {
        self.current_index = 0;
        Ok(())
    }
}

pub struct FilterIterator {
    pub source: Box<dyn RowIterator>,
    pub predicate: Expr,
}

impl RowIterator for FilterIterator {
    fn next_row(
        &mut self,
        ctx: &mut ExecutionContext,
        catalog: &dyn Catalog,
        storage: &dyn Storage,
        clock: &dyn Clock,
    ) -> Result<Option<JoinedRow>, DbError> {
        while let Some(row) = self.source.next_row(ctx, catalog, storage, clock)? {
            if eval_predicate(&self.predicate, &row, ctx, catalog, storage, clock)? {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }

    fn reset(&mut self) -> Result<(), DbError> {
        self.source.reset()
    }
}

pub struct NestedLoopJoinIterator {
    pub left: Box<dyn RowIterator>,
    pub right: Box<dyn RowIterator>,
    pub current_left: Option<JoinedRow>,
    pub join_type: JoinType,
    pub on: Option<Expr>,
    pub right_shape: Vec<ContextTable>,
    pub matched_current_left: bool,
}

impl RowIterator for NestedLoopJoinIterator {
    fn next_row(
        &mut self,
        ctx: &mut ExecutionContext,
        catalog: &dyn Catalog,
        storage: &dyn Storage,
        clock: &dyn Clock,
    ) -> Result<Option<JoinedRow>, DbError> {
        loop {
            if self.current_left.is_none() {
                self.current_left = self.left.next_row(ctx, catalog, storage, clock)?;
                if self.current_left.is_none() {
                    return Ok(None);
                }
                self.right.reset()?;
                self.matched_current_left = false;
            }

            while let Some(right_row) = self.right.next_row(ctx, catalog, storage, clock)? {
                let mut candidate = self.current_left.as_ref().unwrap().clone();
                candidate.extend(right_row);

                let matches = if let Some(on_expr) = &self.on {
                    eval_predicate(on_expr, &candidate, ctx, catalog, storage, clock)?
                } else {
                    true // Cross join
                };

                if matches {
                    self.matched_current_left = true;
                    return Ok(Some(candidate));
                }
            }

            // Exhausted right side for current_left
            let left_row = self.current_left.take().unwrap();
            if !self.matched_current_left
                && (self.join_type == JoinType::Left || self.join_type == JoinType::Full)
            {
                let mut candidate = left_row;
                candidate.extend(self.right_shape.iter().map(ContextTable::null_row));
                return Ok(Some(candidate));
            }
        }
    }

    fn reset(&mut self) -> Result<(), DbError> {
        self.left.reset()?;
        self.current_left = None;
        self.matched_current_left = false;
        Ok(())
    }
}

pub struct HashJoinIterator {
    pub left: Box<dyn RowIterator>,
    pub right: Box<dyn RowIterator>,
    pub left_keys: Vec<Expr>,
    pub right_keys: Vec<Expr>,
    pub join_type: JoinType,
    pub left_shape: Vec<ContextTable>,
    pub right_shape: Vec<ContextTable>,

    // Build state
    pub build_done: bool,
    pub right_materialized: Vec<JoinedRow>,
    pub hash_map: HashMap<Vec<Value>, Vec<usize>>,
    pub right_matched: Vec<bool>,

    // Probe state
    pub current_left: Option<JoinedRow>,
    pub current_matches: Vec<usize>,
    pub current_match_idx: usize,

    // Post-probe state (for RIGHT/FULL OUTER)
    pub finishing_right: bool,
    pub finishing_idx: usize,
}

impl RowIterator for HashJoinIterator {
    fn next_row(
        &mut self,
        ctx: &mut ExecutionContext,
        catalog: &dyn Catalog,
        storage: &dyn Storage,
        clock: &dyn Clock,
    ) -> Result<Option<JoinedRow>, DbError> {
        if !self.build_done {
            while let Some(right_row) = self.right.next_row(ctx, catalog, storage, clock)? {
                let ri = self.right_materialized.len();
                let key = eval_key(&self.right_keys, &right_row, ctx, catalog, storage, clock)?;
                if !key.iter().any(|v| v.is_null()) {
                    self.hash_map.entry(key).or_default().push(ri);
                }
                self.right_materialized.push(right_row);
                self.right_matched.push(false);
            }
            self.build_done = true;
        }

        if !self.finishing_right {
            loop {
                if self.current_left.is_none() {
                    self.current_left = self.left.next_row(ctx, catalog, storage, clock)?;
                    if self.current_left.is_none() {
                        if self.join_type == JoinType::Right || self.join_type == JoinType::Full {
                            self.finishing_right = true;
                            break;
                        } else {
                            return Ok(None);
                        }
                    }

                    let key = eval_key(
                        &self.left_keys,
                        self.current_left.as_ref().unwrap(),
                        ctx,
                        catalog,
                        storage,
                        clock,
                    )?;
                    if !key.iter().any(|v| v.is_null()) {
                        if let Some(indices) = self.hash_map.get(&key) {
                            self.current_matches = indices.clone();
                            self.current_match_idx = 0;
                        } else {
                            self.current_matches = Vec::new();
                        }
                    } else {
                        self.current_matches = Vec::new();
                    }
                }

                if self.current_match_idx < self.current_matches.len() {
                    let ri = self.current_matches[self.current_match_idx];
                    self.current_match_idx += 1;
                    self.right_matched[ri] = true;
                    let mut candidate = self.current_left.as_ref().unwrap().clone();
                    candidate.extend(self.right_materialized[ri].clone());
                    return Ok(Some(candidate));
                }

                // Exhausted matches for current_left
                let left_row = self.current_left.take().unwrap();
                if self.current_matches.is_empty()
                    && (self.join_type == JoinType::Left || self.join_type == JoinType::Full)
                {
                    let mut candidate = left_row;
                    candidate.extend(self.right_shape.iter().map(ContextTable::null_row));
                    return Ok(Some(candidate));
                }
            }
        }

        // Finishing right side (for RIGHT/FULL)
        while self.finishing_idx < self.right_matched.len() {
            let ri = self.finishing_idx;
            self.finishing_idx += 1;
            if !self.right_matched[ri] {
                let mut candidate: JoinedRow =
                    self.left_shape.iter().map(ContextTable::null_row).collect();
                candidate.extend(self.right_materialized[ri].clone());
                return Ok(Some(candidate));
            }
        }

        Ok(None)
    }

    fn reset(&mut self) -> Result<(), DbError> {
        self.left.reset()?;
        self.current_left = None;
        self.current_matches = Vec::new();
        self.current_match_idx = 0;
        self.finishing_right = false;
        self.finishing_idx = 0;
        // We don't need to rebuild the right side hash map.
        Ok(())
    }
}

pub struct TableScanIterator {
    pub bound: BoundTable,
    pub next_index: usize,
}

impl RowIterator for TableScanIterator {
    fn next_row(
        &mut self,
        _ctx: &mut ExecutionContext,
        _catalog: &dyn Catalog,
        storage: &dyn Storage,
        _clock: &dyn Clock,
    ) -> Result<Option<JoinedRow>, DbError> {
        loop {
            let row = storage.get_row(self.bound.table.id, self.next_index)?;
            match row {
                Some(stored) => {
                    let idx = self.next_index;
                    self.next_index += 1;
                    if !stored.deleted {
                        return Ok(Some(vec![ContextTable {
                            table: self.bound.table.clone(),
                            alias: self.bound.alias.clone(),
                            row: Some(stored),
                            storage_index: Some(idx),
                        }]));
                    }
                }
                None => return Ok(None),
            }
        }
    }

    fn reset(&mut self) -> Result<(), DbError> {
        self.next_index = 0;
        Ok(())
    }
}

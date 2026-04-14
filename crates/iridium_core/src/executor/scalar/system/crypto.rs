use crate::ast::Expr;
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::storage::Storage;
use crate::types::Value;

use super::super::super::clock::Clock;
use super::super::super::context::ExecutionContext;
use super::super::super::evaluator::eval_expr;
use super::super::super::model::ContextTable;

pub(crate) fn eval_hashbytes(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 2 {
        return Err(DbError::Execution("HASHBYTES expects 2 arguments".into()));
    }
    let algo_val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let data_val = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;

    if algo_val.is_null() || data_val.is_null() {
        return Ok(Value::Null);
    }

    let algo = algo_val.to_string_value().to_uppercase();
    let data = data_val.to_string_value();

    let hash_bytes = match algo.as_str() {
        "MD5" => {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            data.hash(&mut hasher);
            let h = hasher.finish();
            let mut bytes = h.to_be_bytes().to_vec();
            bytes.resize(16, 0);
            bytes
        }
        "SHA1" | "SHA_1" => {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher1 = DefaultHasher::new();
            let mut hasher2 = DefaultHasher::new();
            data.hash(&mut hasher1);
            data.len().hash(&mut hasher2);
            let mut bytes = Vec::with_capacity(20);
            bytes.extend_from_slice(&hasher1.finish().to_be_bytes());
            bytes.extend_from_slice(&hasher2.finish().to_be_bytes());
            bytes.extend_from_slice(&[0u8; 4]);
            bytes
        }
        "SHA2_256" | "SHA256" => {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut bytes = Vec::with_capacity(32);
            for i in 0..4 {
                let mut hasher = DefaultHasher::new();
                data.hash(&mut hasher);
                i.hash(&mut hasher);
                bytes.extend_from_slice(&hasher.finish().to_be_bytes());
            }
            bytes
        }
        "SHA2_512" | "SHA512" => {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut bytes = Vec::with_capacity(64);
            for i in 0..8 {
                let mut hasher = DefaultHasher::new();
                data.hash(&mut hasher);
                i.hash(&mut hasher);
                bytes.extend_from_slice(&hasher.finish().to_be_bytes());
            }
            bytes
        }
        _ => {
            return Err(DbError::Execution(format!(
                "Unsupported hash algorithm '{}'. Supported: MD5, SHA1, SHA2_256, SHA2_512",
                algo
            )))
        }
    };

    Ok(Value::VarBinary(hash_bytes))
}

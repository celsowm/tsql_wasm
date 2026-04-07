use crate::types::Value;
use super::super::super::context::ExecutionContext;

pub(crate) fn deterministic_uuid(state: &mut u64) -> uuid::Uuid {
    *state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    let bytes = state.to_be_bytes();
    let mut uuid_bytes = [0u8; 16];
    uuid_bytes[0] = bytes[0];
    uuid_bytes[1] = bytes[1];
    uuid_bytes[2] = bytes[2];
    uuid_bytes[3] = bytes[3];
    uuid_bytes[4] = bytes[4];
    uuid_bytes[5] = bytes[5];
    uuid_bytes[6] = bytes[6];
    uuid_bytes[7] = bytes[7];
    uuid_bytes[8] = bytes[0] ^ bytes[4];
    uuid_bytes[9] = bytes[1] ^ bytes[5];
    uuid_bytes[10] = bytes[2] ^ bytes[6];
    uuid_bytes[11] = bytes[3] ^ bytes[7];
    uuid_bytes[12] = bytes[4] ^ bytes[0];
    uuid_bytes[13] = bytes[5] ^ bytes[1];
    uuid_bytes[14] = bytes[6] ^ bytes[2];
    uuid_bytes[15] = bytes[7] ^ bytes[3];
    uuid::Uuid::from_bytes(uuid_bytes)
}

pub(crate) fn deterministic_rand(state: &mut u64) -> f64 {
    *state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    let bits = (*state >> 33) as u32;
    bits as f64 / (1u64 << 31) as f64
}

pub(crate) fn eval_newid(ctx: &mut ExecutionContext) -> Value {
    let uuid = deterministic_uuid(&mut *ctx.session.random_state);
    Value::UniqueIdentifier(uuid)
}

pub(crate) fn eval_rand(ctx: &mut ExecutionContext) -> Value {
    let val = deterministic_rand(&mut *ctx.session.random_state);
    Value::Decimal((val * 1_000_000_000.0) as i128, 9)
}

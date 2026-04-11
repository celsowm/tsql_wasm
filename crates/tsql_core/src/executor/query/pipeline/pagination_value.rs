pub(crate) fn value_to_usize(value: crate::types::Value) -> usize {
    match value {
        crate::types::Value::Int(n) => n.max(0) as usize,
        crate::types::Value::BigInt(n) => n.max(0) as usize,
        crate::types::Value::SmallInt(n) => n.max(0) as usize,
        crate::types::Value::TinyInt(n) => n as usize,
        _ => 0,
    }
}

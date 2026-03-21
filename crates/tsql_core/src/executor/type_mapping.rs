use crate::ast::DataTypeSpec;
use crate::types::DataType;

pub(crate) fn data_type_spec_to_runtime(spec: &DataTypeSpec) -> DataType {
    match spec {
        DataTypeSpec::Bit => DataType::Bit,
        DataTypeSpec::Int => DataType::Int,
        DataTypeSpec::BigInt => DataType::BigInt,
        DataTypeSpec::VarChar(max_len) => DataType::VarChar { max_len: *max_len },
        DataTypeSpec::NVarChar(max_len) => DataType::NVarChar { max_len: *max_len },
        DataTypeSpec::DateTime => DataType::DateTime,
    }
}

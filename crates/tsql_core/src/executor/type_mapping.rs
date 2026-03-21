use crate::ast::DataTypeSpec;
use crate::types::DataType;

pub(crate) fn data_type_spec_to_runtime(spec: &DataTypeSpec) -> DataType {
    match spec {
        DataTypeSpec::Bit => DataType::Bit,
        DataTypeSpec::TinyInt => DataType::TinyInt,
        DataTypeSpec::SmallInt => DataType::SmallInt,
        DataTypeSpec::Int => DataType::Int,
        DataTypeSpec::BigInt => DataType::BigInt,
        DataTypeSpec::Decimal(p, s) => DataType::Decimal {
            precision: *p,
            scale: *s,
        },
        DataTypeSpec::Char(len) => DataType::Char { len: *len },
        DataTypeSpec::VarChar(max_len) => DataType::VarChar { max_len: *max_len },
        DataTypeSpec::NChar(len) => DataType::NChar { len: *len },
        DataTypeSpec::NVarChar(max_len) => DataType::NVarChar { max_len: *max_len },
        DataTypeSpec::Date => DataType::Date,
        DataTypeSpec::Time => DataType::Time,
        DataTypeSpec::DateTime => DataType::DateTime,
        DataTypeSpec::DateTime2 => DataType::DateTime2,
        DataTypeSpec::UniqueIdentifier => DataType::UniqueIdentifier,
    }
}

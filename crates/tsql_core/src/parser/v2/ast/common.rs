use std::borrow::Cow;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectName<'a> {
    pub schema: Option<Cow<'a, str>>,
    pub name: Cow<'a, str>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DataType<'a> {
    Int,
    BigInt,
    SmallInt,
    TinyInt,
    Bit,
    Float,
    Real,
    Decimal(u8, u8),
    Numeric(u8, u8),
    Money,
    SmallMoney,
    Char(Option<u32>),
    NChar(Option<u32>),
    VarChar(Option<u32>),
    NVarChar(Option<u32>),
    Binary(Option<u32>),
    VarBinary(Option<u32>),
    Date,
    Time,
    DateTime,
    DateTime2,
    DateTimeOffset,
    SmallDateTime,
    UniqueIdentifier,
    Xml,
    Image,
    Text,
    NText,
    SqlVariant,
    Table,
    Custom(Cow<'a, str>),
}

use std::borrow::Cow;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectName<'a> {
    pub schema: Option<Cow<'a, str>>,
    pub name: Cow<'a, str>,
}

impl<'a> ObjectName<'a> {
    pub fn schema_or_dbo(&self) -> &str {
        self.schema.as_deref().unwrap_or("dbo")
    }
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TableFactor<'a> {
    Named(ObjectName<'a>),
    Derived(Box<crate::parser::ast::statements::query::SelectStmt<'a>>),
    Values {
        rows: Vec<Vec<crate::parser::ast::expressions::Expr<'a>>>,
        columns: Vec<Cow<'a, str>>,
    },
    TableValuedFunction {
        name: Vec<Cow<'a, str>>,
        args: Vec<crate::parser::ast::expressions::Expr<'a>>,
        alias: Option<Cow<'a, str>>,
    },
}

impl<'a> TableFactor<'a> {
    pub fn as_object_name(&self) -> Option<&ObjectName<'a>> {
        match self {
            TableFactor::Named(name) => Some(name),
            TableFactor::Derived(_) | TableFactor::Values { .. } | TableFactor::TableValuedFunction { .. } => None,
        }
    }

    pub fn is_derived(&self) -> bool {
        matches!(self, TableFactor::Derived(_))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableRef<'a> {
    pub factor: TableFactor<'a>,
    pub alias: Option<Cow<'a, str>>,
    pub pivot: Option<Box<PivotSpec<'a>>>,
    pub unpivot: Option<Box<UnpivotSpec<'a>>>,
    pub hints: Vec<Cow<'a, str>>,
}

impl<'a> TableRef<'a> {
    pub fn name_as_object(&self) -> Option<&ObjectName<'a>> {
        self.factor.as_object_name()
    }

    pub fn object(name: ObjectName<'a>) -> Self {
        Self {
            factor: TableFactor::Named(name),
            alias: None,
            pivot: None,
            unpivot: None,
            hints: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PivotSpec<'a> {
    pub aggregate_func: Cow<'a, str>,
    pub aggregate_col: Cow<'a, str>,
    pub pivot_col: Cow<'a, str>,
    pub pivot_values: Vec<Cow<'a, str>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UnpivotSpec<'a> {
    pub value_col: Cow<'a, str>,
    pub pivot_col: Cow<'a, str>,
    pub column_list: Vec<Cow<'a, str>>,
}

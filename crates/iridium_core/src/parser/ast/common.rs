use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ObjectName {
    pub schema: Option<String>,
    pub name: String,
}

impl ObjectName {
    pub fn schema_or_dbo(&self) -> &str {
        self.schema.as_deref().unwrap_or("dbo")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataType {
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
    Vector(u16),
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
    Custom(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TableFactor {
    Named(ObjectName),
    Derived(Box<crate::parser::ast::statements::query::SelectStmt>),
    JoinedGroup {
        base: Box<TableRef>,
        joins: Vec<crate::parser::ast::statements::query::JoinClause>,
    },
    Values {
        rows: Vec<Vec<crate::parser::ast::expressions::Expr>>,
        columns: Vec<String>,
    },
    TableValuedFunction {
        name: Vec<String>,
        args: Vec<crate::parser::ast::expressions::Expr>,
        alias: Option<String>,
    },
}

impl TableFactor {
    pub fn as_object_name(&self) -> Option<&ObjectName> {
        match self {
            TableFactor::Named(name) => Some(name),
            TableFactor::Derived(_)
            | TableFactor::JoinedGroup { .. }
            | TableFactor::Values { .. }
            | TableFactor::TableValuedFunction { .. } => None,
        }
    }

    pub fn is_derived(&self) -> bool {
        matches!(self, TableFactor::Derived(_))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableRef {
    pub factor: TableFactor,
    pub alias: Option<String>,
    pub pivot: Option<Box<PivotSpec>>,
    pub unpivot: Option<Box<UnpivotSpec>>,
    pub hints: Vec<String>,
}

impl TableRef {
    pub fn name_as_object(&self) -> Option<&ObjectName> {
        self.factor.as_object_name()
    }

    pub fn object(name: ObjectName) -> Self {
        Self {
            factor: TableFactor::Named(name),
            alias: None,
            pivot: None,
            unpivot: None,
            hints: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PivotSpec {
    pub aggregate_func: String,
    pub aggregate_col: String,
    pub pivot_col: String,
    pub pivot_values: Vec<String>,
    #[serde(default)]
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UnpivotSpec {
    pub value_col: String,
    pub pivot_col: String,
    pub column_list: Vec<String>,
    #[serde(default)]
    pub alias: Option<String>,
    #[serde(default)]
    pub source_alias: Option<String>,
}

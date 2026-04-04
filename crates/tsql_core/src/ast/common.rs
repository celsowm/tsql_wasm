use serde::{Deserialize, Serialize};
use crate::ast::statements::query::SelectStmt;
use crate::ast::Expr;

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
pub enum TableFactor {
    Named(ObjectName),
    Derived(Box<SelectStmt>),
    Values {
        rows: Vec<Vec<Expr>>,
        columns: Vec<String>,
    },
}

impl TableFactor {
    pub fn as_object_name(&self) -> Option<&ObjectName> {
        match self {
            TableFactor::Named(o) => Some(o),
            TableFactor::Derived(_) => None,
            TableFactor::Values { .. } => None,
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
    #[serde(default)]
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
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UnpivotSpec {
    pub value_col: String,
    pub pivot_col: String,
    pub column_list: Vec<String>,
}

use serde::{Deserialize, Serialize};
use crate::ast::statements::query::SelectStmt;

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
pub enum TableName {
    Object(ObjectName),
    Subquery(Box<SelectStmt>),
}

impl TableName {
    pub fn name(&self) -> &str {
        match self {
            TableName::Object(o) => &o.name,
            TableName::Subquery(_) => "subquery",
        }
    }

    pub fn name_string(&self) -> String {
        self.name().to_string()
    }

    pub fn is_subquery(&self) -> bool {
        matches!(self, TableName::Subquery(_))
    }

    pub fn schema_or_dbo(&self) -> &str {
        match self {
            TableName::Object(o) => o.schema_or_dbo(),
            TableName::Subquery(_) => "dbo",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableRef {
    pub name: TableName,
    pub alias: Option<String>,
    pub pivot: Option<Box<PivotSpec>>,
    pub unpivot: Option<Box<UnpivotSpec>>,
    #[serde(default)]
    pub hints: Vec<String>,
}

impl TableRef {
    pub fn name_as_object(&self) -> Option<&ObjectName> {
        match &self.name {
            TableName::Object(o) => Some(o),
            TableName::Subquery(_) => None,
        }
    }

    pub fn object(name: ObjectName) -> Self {
        Self {
            name: TableName::Object(name),
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

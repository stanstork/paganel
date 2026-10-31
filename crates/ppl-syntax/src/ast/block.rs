use crate::ast::{
    attribute::Attribute,
    dotpath::DotPath,
    pipeline::{NestedBlock, WhereClause},
    span::Span,
};
use serde::{Deserialize, Serialize};

/// Define block for constants/computed values
/// Syntax: define { tax_rate = 1.4, cutoff_date = "2024-01-01" }
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DefineBlock {
    pub attributes: Vec<Attribute>,
    pub span: Span,
}

/// Execution block for DAG execution configuration
/// Syntax: execution { strategy = "parallel", max_concurrency = 8, on_failure = "continue" }
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionBlock {
    pub attributes: Vec<Attribute>,
    pub span: Span,
}

/// Connection block for data sources
/// Syntax: connection "mysql_prod" { driver = "mysql", ... }
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConnectionBlock {
    pub name: String,
    pub attributes: Vec<Attribute>,
    pub nested_blocks: Vec<NestedBlock>,
    pub span: Span,
}

// Plugin block for WASM plugin definitions
// Syntax: plugin "my_plugin" { path = "./plugins/my_plugin.wasm", allow_http = true, ... }
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PluginBlock {
    pub name: String,
    pub attributes: Vec<Attribute>,
    pub nested_blocks: Vec<NestedBlock>,
    pub span: Span,
}

/// Pair block: a live source and target table that `pag drift` diffs
/// Syntax: pair "orders" { source { ... } target { ... } map { ... } where { ... } ignore = [...] }
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PairBlock {
    pub name: String,
    pub attributes: Vec<Attribute>,
    pub sides: Vec<PairSideBlock>,
    pub map: Vec<ColumnMatch>,
    pub span: Span,
}

/// `source { ... }` or `target { ... }`: one table of a pair, in its own
/// column names.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PairSideBlock {
    pub kind: String,
    pub attributes: Vec<Attribute>,
    pub where_clauses: Vec<WhereClause>,
    pub span: Span,
}

/// `source.customer_id = target.cust_id`: two columns that hold one value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnMatch {
    pub left: DotPath,
    pub right: DotPath,
    pub span: Span,
}

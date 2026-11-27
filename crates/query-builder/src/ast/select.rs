//! Defines the Abstract Syntax Tree (AST) for a SELECT query.

use crate::ast::{
    common::{JoinKind, OrderDir, TableRef},
    expr::Expr,
};

#[derive(Debug, Default, Clone, PartialEq)]
pub struct Select {
    /// The list of columns or expressions to be returned.
    /// e.g., `id`, `name`, `COUNT(*)`
    pub columns: Vec<Expr>,

    /// The primary table for the query.
    /// e.g., `FROM users`
    pub from: Option<FromClause>,

    /// A list of JOIN clauses.
    pub joins: Vec<JoinClause>,

    /// The WHERE clause condition.
    pub where_clause: Option<Expr>,

    /// The ORDER BY clause.
    pub order_by: Vec<OrderByExpr>,

    /// The GROUP BY clause.
    pub group_by: Vec<Expr>,

    /// The LIMIT clause.
    pub limit: Option<Expr>,

    /// The OFFSET clause.
    pub offset: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FromClause {
    pub table: TableSource,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JoinClause {
    pub kind: JoinKind,
    pub table: TableRef,
    pub alias: Option<String>,
    /// The join condition, e.g., `ON users.id = posts.user_id`.
    pub on: Expr,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderByExpr {
    pub expr: Expr,
    pub direction: Option<OrderDir>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableSource {
    Table(TableRef),
    Subquery(Box<Select>),
    Values {
        columns: Vec<String>,
        rows: Vec<Vec<Expr>>,
    },
}

impl From<TableRef> for TableSource {
    fn from(table: TableRef) -> Self {
        TableSource::Table(table)
    }
}

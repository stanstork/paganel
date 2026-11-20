use crate::sql::{filter::SqlFilter, join::clause::JoinClause, query::select::SelectField};
use model::{core::value::Value, pagination::cursor::Cursor};
use query_builder::offsets::{DefaultOffset, OffsetStrategy};
use std::sync::Arc;

#[derive(Clone)]
pub struct FetchRowsRequest {
    pub table: String,
    pub alias: Option<String>,
    pub columns: Vec<SelectField>,
    pub joins: Vec<JoinClause>,
    pub filter: Option<SqlFilter>,
    pub limit: usize,
    pub cursor: Cursor,
    pub strategy: Arc<dyn OffsetStrategy>,
    /// Optional IN clause: column name and list of values
    pub in_clause: Option<(String, Vec<Value>)>,
    /// Whether to order results randomly (ORDER BY RANDOM()/RAND())
    pub order_random: bool,
    /// Optional key ranges: column name and closed `[lo, hi]` bounds
    pub key_ranges: Option<(String, Vec<(Value, Value)>)>,
}

pub struct FetchRowsRequestBuilder {
    table: String,
    alias: Option<String>,
    columns: Vec<SelectField>,
    joins: Vec<JoinClause>,
    filter: Option<SqlFilter>,
    limit: usize,
    cursor: Cursor,
    strategy: Arc<dyn OffsetStrategy>,
    in_clause: Option<(String, Vec<Value>)>,
    order_random: bool,
    key_ranges: Option<(String, Vec<(Value, Value)>)>,
}

impl FetchRowsRequestBuilder {
    pub fn new(table: String) -> Self {
        FetchRowsRequestBuilder {
            table,
            alias: None,
            columns: Vec::new(),
            joins: Vec::new(),
            filter: None,
            limit: 0,
            cursor: Cursor::Default { offset: 0 },
            strategy: Arc::new(DefaultOffset { offset: 0 }),
            in_clause: None,
            order_random: false,
            key_ranges: None,
        }
    }

    pub fn alias(mut self, alias: String) -> Self {
        self.alias = Some(alias);
        self
    }

    pub fn columns(mut self, columns: Vec<SelectField>) -> Self {
        self.columns = columns;
        self
    }

    pub fn joins(mut self, joins: Vec<JoinClause>) -> Self {
        self.joins = joins;
        self
    }

    pub fn filter(mut self, filter: Option<SqlFilter>) -> Self {
        self.filter = filter;
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    pub fn cursor(mut self, cursor: Cursor) -> Self {
        self.cursor = cursor;
        self
    }

    pub fn strategy(mut self, strategy: Arc<dyn OffsetStrategy>) -> Self {
        self.strategy = strategy;
        self
    }

    /// Adds an IN clause for filtering by a list of values
    /// e.g., `WHERE id IN (1, 2, 3)`
    pub fn in_clause(mut self, column: String, values: Vec<Value>) -> Self {
        self.in_clause = Some((column, values));
        self
    }

    /// Enables random ordering (ORDER BY RANDOM() for PostgreSQL, ORDER BY RAND() for MySQL)
    pub fn order_random(mut self, enable: bool) -> Self {
        self.order_random = enable;
        self
    }

    /// Keeps only rows whose `column` falls in one of `ranges`
    pub fn key_ranges(mut self, column: String, ranges: Vec<(Value, Value)>) -> Self {
        self.key_ranges = Some((column, ranges));
        self
    }

    pub fn build(self) -> FetchRowsRequest {
        FetchRowsRequest {
            table: self.table,
            alias: self.alias,
            columns: self.columns,
            joins: self.joins,
            filter: self.filter,
            limit: self.limit,
            cursor: self.cursor,
            strategy: self.strategy,
            in_clause: self.in_clause,
            order_random: self.order_random,
            key_ranges: self.key_ranges,
        }
    }
}

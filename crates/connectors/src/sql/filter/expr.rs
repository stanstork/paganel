use super::condition::Condition;
use crate::sql::join::clause::JoinClause;
use std::fmt;

/// A full boolean expression for SQL filtering
#[derive(Debug, Clone, PartialEq)]
pub enum SqlFilterExpr {
    /// A single leaf condition
    Leaf(Condition),

    /// An AND of 1+ sub‐expressions
    /// (e.g. `WHERE a = 1 AND b = 2`)
    And(Vec<SqlFilterExpr>),

    /// An OR of 1+ sub‐expressions
    /// (e.g. `WHERE a = 1 OR b = 2`)
    Or(Vec<SqlFilterExpr>),
}

impl SqlFilterExpr {
    pub fn leaf(cond: Condition) -> Self {
        SqlFilterExpr::Leaf(cond)
    }

    pub fn and(exprs: Vec<SqlFilterExpr>) -> Self {
        SqlFilterExpr::And(exprs)
    }

    pub fn or(exprs: Vec<SqlFilterExpr>) -> Self {
        SqlFilterExpr::Or(exprs)
    }

    /// Render this expression as SQL
    pub fn to_sql(&self) -> String {
        match self {
            SqlFilterExpr::Leaf(cond) => cond.to_sql_fragment(),
            SqlFilterExpr::And(exprs) => {
                let exprs = exprs.iter().map(SqlFilterExpr::to_sql).collect::<Vec<_>>();
                format!("({})", exprs.join(" AND "))
            }
            SqlFilterExpr::Or(exprs) => {
                let exprs = exprs.iter().map(SqlFilterExpr::to_sql).collect::<Vec<_>>();
                format!("({})", exprs.join(" OR "))
            }
        }
    }

    pub fn filter_for_table(&self, table: &str, joins: &[JoinClause]) -> Option<SqlFilterExpr> {
        match self {
            SqlFilterExpr::Leaf(cond) => {
                if cond.applies_to(table, joins) {
                    Some(SqlFilterExpr::Leaf(cond.clone()))
                } else {
                    None
                }
            }
            // AND: keep children that survive, then
            // * if 0 remain -> drop the whole AND
            // * if 1 remains -> collapse to that child
            // * else -> rebuild AND
            SqlFilterExpr::And(children) => {
                let kept = children
                    .iter()
                    .filter_map(|c| c.filter_for_table(table, joins))
                    .collect::<Vec<_>>();
                match kept.len() {
                    0 => None,
                    1 => kept.into_iter().next(),
                    _ => Some(SqlFilterExpr::And(kept)),
                }
            }
            // OR: same logic as AND, but rebuild with Or()
            SqlFilterExpr::Or(children) => {
                let kept = children
                    .iter()
                    .filter_map(|c| c.filter_for_table(table, joins))
                    .collect::<Vec<_>>();
                match kept.len() {
                    0 => None,
                    1 => kept.into_iter().next(),
                    _ => Some(SqlFilterExpr::Or(kept)),
                }
            }
        }
    }

    /// Return a deduplicated list of all tables referenced in this filter.
    pub fn tables(&self) -> Vec<String> {
        let mut tables: Vec<String> = match self {
            SqlFilterExpr::Leaf(cond) => {
                // Single‐table leaf
                vec![cond.table.clone()]
            }
            SqlFilterExpr::And(exprs) | SqlFilterExpr::Or(exprs) => {
                // Recursively collect from children
                exprs.iter().flat_map(|e| e.tables()).collect()
            }
        };
        // Remove duplicates in‐place
        tables.sort_unstable();
        tables.dedup();
        tables
    }

    pub fn columns(&self) -> Vec<String> {
        let mut columns: Vec<String> = match self {
            SqlFilterExpr::Leaf(cond) => {
                // Single‐column leaf
                vec![format!("{}.{}", cond.table, cond.column)]
            }
            SqlFilterExpr::And(exprs) | SqlFilterExpr::Or(exprs) => {
                // Recursively collect from children
                exprs.iter().flat_map(|e| e.columns()).collect()
            }
        };
        // Remove duplicates in‐place
        columns.sort_unstable();
        columns.dedup();
        columns
    }

    /// The same expression with every condition on `table`.
    pub fn on_table(&self, table: &str) -> SqlFilterExpr {
        match self {
            SqlFilterExpr::Leaf(cond) => SqlFilterExpr::Leaf(Condition {
                table: table.to_string(),
                ..cond.clone()
            }),
            SqlFilterExpr::And(exprs) => {
                SqlFilterExpr::And(exprs.iter().map(|e| e.on_table(table)).collect())
            }
            SqlFilterExpr::Or(exprs) => {
                SqlFilterExpr::Or(exprs.iter().map(|e| e.on_table(table)).collect())
            }
        }
    }
}

impl fmt::Display for SqlFilterExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_sql())
    }
}

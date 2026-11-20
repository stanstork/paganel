//! Defines the AST for SQL expressions.

use model::core::value::Value;

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// A column or table identifier, e.g., `users` or `users.id`.
    Identifier(Ident),

    /// A literal value, such as a string, number, boolean, or NULL.
    Value(Value),

    /// A binary operation, e.g., `column = 'value'` or `a + b`.
    BinaryOp(Box<BinaryOp>),

    /// A function call, e.g., `COUNT(*)` or `MAX(price)`.
    FunctionCall(FunctionCall),

    /// An aliased expression, e.g. `COUNT(*) AS total_count`
    Alias { expr: Box<Expr>, alias: String },

    /// Represents a CAST expression, e.g., CAST(value AS type).
    Cast {
        expr: Box<Expr>,
        data_type: String, // The name of the SQL data type
    },

    /// A raw SQL literal, e.g., `NULL` or `CURRENT_TIMESTAMP`.
    Literal(String),

    /// A CASE expression for conditional logic
    /// e.g., `CASE WHEN condition THEN value ELSE default END`
    Case {
        when_branches: Vec<(Expr, Expr)>, // (condition, value) pairs
        else_expr: Option<Box<Expr>>,
    },

    /// An aggregate function with a FILTER clause (PostgreSQL)
    /// e.g., `COUNT(*) FILTER (WHERE condition)`
    FilteredAggregate {
        function: Box<FunctionCall>,
        filter: Box<Expr>,
    },

    /// NOT expression
    /// e.g., `NOT (age >= 18)`
    Not(Box<Expr>),

    /// IN expression
    /// e.g., `id IN (1, 2, 3)` or `status IN ('active', 'pending')`
    In { expr: Box<Expr>, values: Vec<Expr> },

    /// NULL check
    /// e.g., `return_date IS NULL` or `return_date IS NOT NULL`
    IsNull { expr: Box<Expr>, negated: bool },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ident {
    pub qualifier: Option<String>, // e.g., the 'users' in 'users.id'
    pub name: String,              // e.g., the 'id' in 'users.id'
}

#[derive(Debug, Clone, PartialEq)]
pub struct BinaryOp {
    pub left: Expr,
    pub op: BinaryOperator,
    pub right: Expr,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FunctionCall {
    pub name: String,
    pub args: Vec<Expr>,
    pub wildcard: bool, // represents the '*' in 'COUNT(*)'
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BinaryOperator {
    // Comparison
    Eq,    // =
    NotEq, // <>
    Lt,    // <
    LtEq,  // <=
    Gt,    // >
    GtEq,  // >=

    // Logical
    And,
    Or,

    // Arithmetic
    Plus,     // +
    Minus,    // -
    Multiply, // *
    Modulo,   // %

    /// Integer division, truncating: MySQL `DIV`, PostgreSQL `/` on integers.
    IntDiv,
}

#[allow(clippy::should_implement_trait)]
impl Expr {
    /// Creates a NOT expression
    pub fn not(expr: Expr) -> Self {
        Expr::Not(Box::new(expr))
    }

    /// Creates a CASE expression with a single WHEN/THEN branch and optional ELSE
    pub fn case_when(condition: Expr, then_value: Expr, else_value: Option<Expr>) -> Self {
        Expr::Case {
            when_branches: vec![(condition, then_value)],
            else_expr: else_value.map(Box::new),
        }
    }

    /// Creates a CASE expression with multiple WHEN/THEN branches and optional ELSE
    pub fn case_when_many(when_branches: Vec<(Expr, Expr)>, else_value: Option<Expr>) -> Self {
        Expr::Case {
            when_branches,
            else_expr: else_value.map(Box::new),
        }
    }

    /// Creates an alias for this expression
    pub fn alias(self, alias: impl Into<String>) -> Self {
        Expr::Alias {
            expr: Box::new(self),
            alias: alias.into(),
        }
    }

    /// Creates an IN expression
    /// e.g., `id.in_list(vec![Value::Int(1), Value::Int(2)])`
    pub fn in_list(self, values: Vec<Expr>) -> Self {
        Expr::In {
            expr: Box::new(self),
            values,
        }
    }

    /// Creates an unqualified column identifier, e.g. `id`.
    pub fn column(name: impl Into<String>) -> Self {
        Expr::Identifier(Ident {
            qualifier: None,
            name: name.into(),
        })
    }
}

impl FunctionCall {
    /// Creates a COUNT(*) function call
    pub fn count_all() -> Self {
        FunctionCall {
            name: "COUNT".to_string(),
            args: vec![],
            wildcard: true,
        }
    }

    /// Creates a COUNT(expr) function call
    pub fn count(expr: Expr) -> Self {
        FunctionCall {
            name: "COUNT".to_string(),
            args: vec![expr],
            wildcard: false,
        }
    }

    /// Creates a SUM(expr) function call
    pub fn sum(expr: Expr) -> Self {
        FunctionCall {
            name: "SUM".to_string(),
            args: vec![expr],
            wildcard: false,
        }
    }

    /// Creates a MIN(expr) function call
    pub fn min(expr: Expr) -> Self {
        FunctionCall {
            name: "MIN".to_string(),
            args: vec![expr],
            wildcard: false,
        }
    }

    /// Creates a MAX(expr) function call
    pub fn max(expr: Expr) -> Self {
        FunctionCall {
            name: "MAX".to_string(),
            args: vec![expr],
            wildcard: false,
        }
    }

    /// Creates a RANDOM() or RAND() function call (database-specific)
    /// Use this with order_by() to get random ordering
    /// Note: The actual function name (RANDOM vs RAND) is determined by the dialect during rendering
    pub fn random() -> Self {
        FunctionCall {
            name: "RANDOM".to_string(),
            args: vec![],
            wildcard: false,
        }
    }

    /// Creates a filtered aggregate (PostgreSQL FILTER syntax)
    /// e.g., `COUNT(*) FILTER (WHERE condition)`
    pub fn with_filter(self, filter: Expr) -> Expr {
        Expr::FilteredAggregate {
            function: Box::new(self),
            filter: Box::new(filter),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        dialect::{MySql, Postgres},
        renderer::Renderer,
    };

    fn render_expr_postgres(expr: &Expr) -> String {
        use crate::renderer::Render;
        let mut renderer = Renderer::new(&Postgres);
        expr.render(&mut renderer);
        renderer.sql
    }

    fn render_expr_mysql(expr: &Expr) -> String {
        use crate::renderer::Render;
        let mut renderer = Renderer::new(&MySql);
        expr.render(&mut renderer);
        renderer.sql
    }

    #[test]
    fn test_not_expression() {
        let expr = Expr::not(Expr::BinaryOp(Box::new(BinaryOp {
            left: Expr::Identifier(Ident {
                qualifier: None,
                name: "age".to_string(),
            }),
            op: BinaryOperator::GtEq,
            right: Expr::Value(Value::Int(18)),
        })));

        let sql = render_expr_postgres(&expr);
        assert_eq!(sql, "NOT ((\"age\" >= $1))");
    }

    #[test]
    fn test_filtered_aggregate_postgres() {
        // COUNT(*) FILTER (WHERE NOT (age >= 18))
        let validation_condition = Expr::not(Expr::BinaryOp(Box::new(BinaryOp {
            left: Expr::Identifier(Ident {
                qualifier: None,
                name: "age".to_string(),
            }),
            op: BinaryOperator::GtEq,
            right: Expr::Value(Value::Int(18)),
        })));

        let expr = FunctionCall::count_all()
            .with_filter(validation_condition)
            .alias("failures");

        let sql = render_expr_postgres(&expr);
        assert_eq!(
            sql,
            "COUNT(*) FILTER (WHERE NOT ((\"age\" >= $1))) AS \"failures\""
        );
    }

    #[test]
    fn test_case_when_for_mysql() {
        // SUM(CASE WHEN NOT (age >= 18) THEN 1 ELSE 0 END)
        let validation_condition = Expr::not(Expr::BinaryOp(Box::new(BinaryOp {
            left: Expr::Identifier(Ident {
                qualifier: None,
                name: "age".to_string(),
            }),
            op: BinaryOperator::GtEq,
            right: Expr::Value(Value::Int(18)),
        })));

        let case_expr = Expr::case_when(
            validation_condition,
            Expr::Value(Value::Int(1)),
            Some(Expr::Value(Value::Int(0))),
        );

        let expr = Expr::FunctionCall(FunctionCall::sum(case_expr)).alias("failures");

        let sql = render_expr_mysql(&expr);
        assert_eq!(
            sql,
            "SUM(CASE WHEN NOT ((`age` >= ?)) THEN ? ELSE ? END) AS `failures`"
        );
    }

    #[test]
    fn test_validation_estimation_postgres() {
        // Complete validation estimation query for PostgreSQL:
        // COUNT(*) FILTER (WHERE NOT (users.age >= 18 AND profiles.verified = true)) as failures
        let age_check = Expr::BinaryOp(Box::new(BinaryOp {
            left: Expr::Identifier(Ident {
                qualifier: Some("users".to_string()),
                name: "age".to_string(),
            }),
            op: BinaryOperator::GtEq,
            right: Expr::Value(Value::Int(18)),
        }));

        let verified_check = Expr::BinaryOp(Box::new(BinaryOp {
            left: Expr::Identifier(Ident {
                qualifier: Some("profiles".to_string()),
                name: "verified".to_string(),
            }),
            op: BinaryOperator::Eq,
            right: Expr::Value(Value::Boolean(true)),
        }));

        let combined = Expr::BinaryOp(Box::new(BinaryOp {
            left: age_check,
            op: BinaryOperator::And,
            right: verified_check,
        }));

        let failures = FunctionCall::count_all()
            .with_filter(Expr::not(combined))
            .alias("failures");

        let sql = render_expr_postgres(&failures);
        assert_eq!(
            sql,
            "COUNT(*) FILTER (WHERE NOT (((\"users\".\"age\" >= $1) AND (\"profiles\".\"verified\" = $2)))) AS \"failures\""
        );
    }

    #[test]
    fn test_validation_estimation_mysql() {
        // Complete validation estimation query for MySQL:
        // SUM(CASE WHEN NOT (users.age >= 18 AND profiles.verified = true) THEN 1 ELSE 0 END) as failures
        let age_check = Expr::BinaryOp(Box::new(BinaryOp {
            left: Expr::Identifier(Ident {
                qualifier: Some("users".to_string()),
                name: "age".to_string(),
            }),
            op: BinaryOperator::GtEq,
            right: Expr::Value(Value::Int(18)),
        }));

        let verified_check = Expr::BinaryOp(Box::new(BinaryOp {
            left: Expr::Identifier(Ident {
                qualifier: Some("profiles".to_string()),
                name: "verified".to_string(),
            }),
            op: BinaryOperator::Eq,
            right: Expr::Value(Value::Boolean(true)),
        }));

        let combined = Expr::BinaryOp(Box::new(BinaryOp {
            left: age_check,
            op: BinaryOperator::And,
            right: verified_check,
        }));

        let case_expr = Expr::case_when(
            Expr::not(combined),
            Expr::Value(Value::Int(1)),
            Some(Expr::Value(Value::Int(0))),
        );

        let failures = Expr::FunctionCall(FunctionCall::sum(case_expr)).alias("failures");

        let sql = render_expr_mysql(&failures);
        assert_eq!(
            sql,
            "SUM(CASE WHEN NOT (((`users`.`age` >= ?) AND (`profiles`.`verified` = ?))) THEN ? ELSE ? END) AS `failures`"
        );
    }

    #[test]
    fn test_random_function_postgres() {
        let expr = Expr::FunctionCall(FunctionCall::random());
        let sql = render_expr_postgres(&expr);
        assert_eq!(sql, "RANDOM()");
    }

    #[test]
    fn test_random_function_mysql() {
        let expr = Expr::FunctionCall(FunctionCall::random());
        let sql = render_expr_mysql(&expr);
        assert_eq!(sql, "RAND()");
    }

    #[test]
    fn test_in_expression_postgres() {
        let expr = Expr::Identifier(Ident {
            qualifier: None,
            name: "id".to_string(),
        })
        .in_list(vec![
            Expr::Value(Value::Int(1)),
            Expr::Value(Value::Int(2)),
            Expr::Value(Value::Int(3)),
        ]);

        let sql = render_expr_postgres(&expr);
        assert_eq!(sql, r#""id" IN ($1, $2, $3)"#);
    }

    #[test]
    fn test_in_expression_mysql() {
        let expr = Expr::Identifier(Ident {
            qualifier: None,
            name: "status".to_string(),
        })
        .in_list(vec![
            Expr::Value(Value::String("active".to_string())),
            Expr::Value(Value::String("pending".to_string())),
        ]);

        let sql = render_expr_mysql(&expr);
        assert_eq!(sql, "`status` IN (?, ?)");
    }

    #[test]
    fn test_in_expression_qualified_column_postgres() {
        let expr = Expr::Identifier(Ident {
            qualifier: Some("users".to_string()),
            name: "role".to_string(),
        })
        .in_list(vec![
            Expr::Value(Value::String("admin".to_string())),
            Expr::Value(Value::String("moderator".to_string())),
            Expr::Value(Value::String("editor".to_string())),
        ]);

        let sql = render_expr_postgres(&expr);
        assert_eq!(sql, r#""users"."role" IN ($1, $2, $3)"#);
    }
}

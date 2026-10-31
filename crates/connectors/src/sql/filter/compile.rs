use crate::{
    error::FilterCompileError,
    sql::filter::{SqlFilter, condition::Condition, expr::SqlFilterExpr},
};
use model::execution::expr::{BinaryOp, CompiledExpression};

/// Compile a `where` expression into a SQL filter, or say what it cannot express.
pub fn compile_filter(expr: &CompiledExpression) -> Result<SqlFilter, FilterCompileError> {
    Ok(SqlFilter::with_expr(compile_sql_expr(expr)?))
}

fn compile_sql_expr(expr: &CompiledExpression) -> Result<SqlFilterExpr, FilterCompileError> {
    match expr {
        // Binary expression represents a condition (e.g., table.column = value)
        CompiledExpression::Binary { left, op, right } => {
            // Check if this is a logical operator (AND/OR) or a comparison
            if matches!(op, BinaryOp::And | BinaryOp::Or) {
                // Logical operator - recursively compile both sides
                let children = vec![compile_sql_expr(left)?, compile_sql_expr(right)?];

                Ok(match op {
                    BinaryOp::And => SqlFilterExpr::and(children),
                    BinaryOp::Or => SqlFilterExpr::or(children),
                    _ => unreachable!(),
                })
            } else {
                // Comparison operator - create a leaf condition
                let condition = from_compiled_condition(left, op, right)
                    .map_err(|e| FilterCompileError::UnsupportedExpression(e.to_string()))?;
                Ok(SqlFilterExpr::leaf(condition))
            }
        }

        // Function call with logical operators
        CompiledExpression::FunctionCall { name, args } => {
            let children = args
                .iter()
                .map(compile_sql_expr)
                .collect::<Result<Vec<_>, _>>()?;

            match name.to_ascii_uppercase().as_str() {
                "AND" => Ok(SqlFilterExpr::and(children)),
                "OR" => Ok(SqlFilterExpr::or(children)),
                _ => Err(FilterCompileError::UnsupportedFunction(name.clone())),
            }
        }

        // NULL checks render as `col IS NULL` / `col IS NOT NULL`.
        CompiledExpression::IsNull(inner) => Ok(SqlFilterExpr::leaf(null_condition(inner, "IS")?)),
        CompiledExpression::IsNotNull(inner) => {
            Ok(SqlFilterExpr::leaf(null_condition(inner, "IS NOT")?))
        }

        // Parentheses only affect precedence, which is already encoded in the tree.
        CompiledExpression::Grouped(inner) => compile_sql_expr(inner),

        _ => Err(FilterCompileError::UnsupportedExpression(format!(
            "{expr:?}"
        ))),
    }
}

fn null_condition(
    field: &CompiledExpression,
    comparator: &str,
) -> Result<Condition, FilterCompileError> {
    let (table, column) = match field {
        CompiledExpression::DotPath(segments) if segments.len() >= 2 => {
            (segments[0].clone(), segments[1].clone())
        }
        CompiledExpression::Identifier(name) => (String::new(), name.clone()),
        other => {
            return Err(FilterCompileError::UnsupportedExpression(format!(
                "null check on non-field expression: {other:?}"
            )));
        }
    };

    Ok(Condition {
        table,
        column,
        comparator: comparator.to_string(),
        value: "NULL".to_string(),
    })
}

fn from_compiled_condition(
    left: &CompiledExpression,
    op: &BinaryOp,
    right: &CompiledExpression,
) -> Result<Condition, Box<dyn std::error::Error>> {
    // Extract table & column from left side (expected to be DotPath like table.column)
    let (table, column) = match left {
        CompiledExpression::DotPath(segments) if segments.len() >= 2 => {
            (segments[0].clone(), segments[1].clone())
        }
        CompiledExpression::Identifier(name) => {
            // Single identifier without table prefix
            (String::new(), name.clone())
        }
        other => {
            return Err(format!("Unsupported expression type for filter field: {other:?}").into());
        }
    };

    // Convert right side to string value
    let value = format_expr_value(right)?;

    // Map BinaryOp to SQL comparator
    let comparator = match op {
        BinaryOp::Equal => "=",
        BinaryOp::NotEqual => "!=",
        BinaryOp::GreaterThan => ">",
        BinaryOp::GreaterOrEqual => ">=",
        BinaryOp::LessThan => "<",
        BinaryOp::LessOrEqual => "<=",
        _ => return Err(format!("Unsupported operator for filter: {:?}", op).into()),
    }
    .to_string();

    Ok(Condition {
        table,
        column,
        comparator,
        value,
    })
}

fn format_expr_value(expr: &CompiledExpression) -> Result<String, Box<dyn std::error::Error>> {
    use model::core::value::Value;

    match expr {
        CompiledExpression::Literal(value) => {
            let formatted = match value {
                Value::Int(v) => v.to_string(),
                Value::UInt(v) => v.to_string(),
                Value::Float(v) => v.to_string(),
                Value::Decimal(v) => v.to_string(),
                Value::String(v) => v.clone(),
                Value::Boolean(v) => v.to_string(),
                Value::Null => "NULL".to_string(),
                Value::Date(d) => d.to_string(),
                Value::Time { value, .. } => value.to_string(),
                Value::Timestamp { value, .. } => value.to_string(),
                Value::Uuid(u) => u.to_string(),
                Value::Enum { value, .. } => value.clone(),
                _ => return Err(format!("Unsupported value type for filter: {:?}", value).into()),
            };
            Ok(formatted)
        }
        CompiledExpression::Identifier(name) => Ok(name.clone()),
        CompiledExpression::DotPath(segments) => Ok(segments.join(".")),
        _ => Err(format!("Unsupported expression type for filter value: {:?}", expr).into()),
    }
}

use crate::{
    drivers::csv::filter::{CsvComparator, CsvCondition, CsvFilter, CsvFilterExpr},
    error::FilterCompileError,
};
use model::{
    core::value::Value,
    execution::expr::{BinaryOp, CompiledExpression},
};
use std::str::FromStr;

/// Compile a `where` expression into a CSV row filter, or say what it cannot express.
pub fn compile_filter(expr: &CompiledExpression) -> Result<CsvFilter, FilterCompileError> {
    Ok(CsvFilter::with_expr(compile_csv_expr(expr)?))
}

fn from_compiled_condition(
    left: &CompiledExpression,
    op: &BinaryOp,
    right: &CompiledExpression,
) -> Result<CsvCondition, Box<dyn std::error::Error>> {
    // Extract the field name from left side
    let field = match left {
        CompiledExpression::DotPath(segments) if segments.len() >= 2 => {
            // For CSV, we just need the column name (second segment)
            segments[1].clone()
        }
        CompiledExpression::Identifier(name) => name.clone(),
        other => {
            return Err(format!("Unsupported expression type for filter field: {other:?}").into());
        }
    };

    // Convert right side to string value
    let value = format_expr_value(right)?
        .trim_start_matches('\'')
        .trim_end_matches('\'')
        .trim_start_matches('"')
        .trim_end_matches('"')
        .to_string();

    // Map BinaryOp to CsvComparator
    let op_str = match op {
        BinaryOp::Equal => "=",
        BinaryOp::NotEqual => "!=",
        BinaryOp::GreaterThan => ">",
        BinaryOp::GreaterOrEqual => ">=",
        BinaryOp::LessThan => "<",
        BinaryOp::LessOrEqual => "<=",
        _ => return Err(format!("Unsupported operator for CSV filter: {:?}", op).into()),
    };

    let csv_op = CsvComparator::from_str(op_str)
        .map_err(|_| format!("Unsupported comparator: {}", op_str))?;

    Ok(CsvCondition {
        left: field,
        op: csv_op,
        right: value,
    })
}

fn format_expr_value(expr: &CompiledExpression) -> Result<String, Box<dyn std::error::Error>> {
    match expr {
        CompiledExpression::Literal(value) => Ok(literal_to_string(value)),
        CompiledExpression::Identifier(name) => Ok(name.clone()),
        CompiledExpression::DotPath(segments) => Ok(segments.join(".")),
        _ => Err(format!("Unsupported expression type for filter value: {:?}", expr).into()),
    }
}

/// Render a literal `Value` as the bare string a CSV cell would contain, in the
/// same textual form the column parser (`CsvType::get_value`) expects, so a
/// `where` filter re-parses it correctly.
fn literal_to_string(value: &Value) -> String {
    match value {
        Value::Int(v) => v.to_string(),
        Value::UInt(v) => v.to_string(),
        Value::Float(v) => v.to_string(),
        Value::Decimal(v) => v.to_string(),
        Value::String(v) => v.clone(),
        Value::Boolean(v) => v.to_string(),
        Value::Date(d) => d.format("%Y-%m-%d").to_string(),
        Value::Timestamp { value: ts, .. } => ts.format("%Y-%m-%d %H:%M:%S").to_string(),
        Value::Json(v) => v.to_string(),
        other => format!("{other:?}"),
    }
}

/// Recursively compiles a filter expression into a CSV filter AST.
fn compile_csv_expr(expr: &CompiledExpression) -> Result<CsvFilterExpr, FilterCompileError> {
    match expr {
        // Binary expression represents a condition
        CompiledExpression::Binary { left, op, right } => {
            // Check if this is a logical operator (AND/OR) or a comparison
            if matches!(op, BinaryOp::And | BinaryOp::Or) {
                // Logical operator - recursively compile both sides
                let children = vec![compile_csv_expr(left)?, compile_csv_expr(right)?];

                Ok(match op {
                    BinaryOp::And => CsvFilterExpr::and(children),
                    BinaryOp::Or => CsvFilterExpr::or(children),
                    _ => unreachable!(),
                })
            } else {
                // Comparison operator - create a leaf condition
                let csv_cond = from_compiled_condition(left, op, right)
                    .map_err(|e| FilterCompileError::UnsupportedExpression(e.to_string()))?;
                Ok(CsvFilterExpr::leaf(csv_cond))
            }
        }

        // Function call with logical operators
        CompiledExpression::FunctionCall { name, args } => {
            let children = args
                .iter()
                .map(compile_csv_expr)
                .collect::<Result<Vec<_>, _>>()?;
            match name.to_ascii_uppercase().as_str() {
                "AND" => Ok(CsvFilterExpr::and(children)),
                "OR" => Ok(CsvFilterExpr::or(children)),
                _ => Err(FilterCompileError::UnsupportedFunction(name.clone())),
            }
        }

        _ => Err(FilterCompileError::UnsupportedExpression(format!(
            "{expr:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::literal_to_string;
    use chrono::NaiveDate;
    use model::core::value::Value;

    #[test]
    fn renders_literals_as_bare_csv_strings() {
        assert_eq!(literal_to_string(&Value::Int(90)), "90");
        assert_eq!(literal_to_string(&Value::Float(95.5)), "95.5");
        assert_eq!(literal_to_string(&Value::Boolean(true)), "true");
        assert_eq!(literal_to_string(&Value::String("active".into())), "active");

        let date = NaiveDate::from_ymd_opt(2023, 1, 2).unwrap();
        assert_eq!(literal_to_string(&Value::Date(date)), "2023-01-02");

        let ts = date.and_hms_opt(3, 4, 5).unwrap();
        assert_eq!(
            literal_to_string(&Value::Timestamp {
                value: ts,
                offset_secs: None,
            }),
            "2023-01-02 03:04:05"
        );

        assert_eq!(
            literal_to_string(&Value::Json(serde_json::json!({ "a": 1 }))),
            r#"{"a":1}"#
        );
    }
}

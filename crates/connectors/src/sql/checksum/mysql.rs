use super::{
    ColumnKind, NormalSql,
    expr::{cast, func, text},
};
use crate::sql::metadata::column::ColumnMetadata;
use query_builder::{
    ast::expr::{BinaryOp, BinaryOperator, Expr},
    dialect::{MySql, QueryDialect},
};

const TWO_POW_64: &str = "18446744073709551616";

/// MySQL 8.0+ normal text. Runs in a session with `time_zone = '+00:00'` and `NAMES utf8mb4`.
pub struct MySqlNormal;

impl NormalSql for MySqlNormal {
    fn dialect(&self) -> &'static dyn QueryDialect {
        &MySql
    }

    fn kind(&self, column: &ColumnMetadata) -> Option<ColumnKind> {
        let data_type = column.data_type.to_ascii_lowercase();

        Some(match data_type.as_str() {
            "tinyint" if is_tinyint_bool(column) => ColumnKind::Bool,
            "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint" | "year" => {
                ColumnKind::Int
            }
            "decimal" | "numeric" => ColumnKind::Decimal {
                scale: column.num_scale.unwrap_or(0),
            },
            "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext" => {
                ColumnKind::Text
            }
            "enum" => ColumnKind::Enum,
            "date" => ColumnKind::Date,
            "datetime" => ColumnKind::Timestamp,
            "timestamp" => ColumnKind::TimestampTz,
            "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" => {
                ColumnKind::Binary
            }
            _ => return None,
        })
    }

    fn normal(&self, kind: ColumnKind, column: Expr) -> Expr {
        match kind {
            ColumnKind::Int | ColumnKind::Bool | ColumnKind::Decimal { scale: 0 } => {
                cast(column, "CHAR")
            }
            // `CAST` writes exactly `scale` fractional digits, so a `.` is
            // always there to stop the zero trim: 1.50 -> 1.5, 100.00 -> 100,
            // 0.00 -> 0.
            ColumnKind::Decimal { .. } => {
                trim_trailing(trim_trailing(cast(column, "CHAR"), "0"), ".")
            }
            // `CONVERT(c USING utf8mb4)`, written as a cast; an enum casts to
            // its label.
            ColumnKind::Text | ColumnKind::Enum | ColumnKind::Uuid => {
                cast(column, "CHAR CHARACTER SET utf8mb4")
            }
            ColumnKind::Date => func("DATE_FORMAT", vec![column, text("%Y-%m-%d")]),
            ColumnKind::Timestamp | ColumnKind::TimestampTz => {
                func("DATE_FORMAT", vec![column, text("%Y-%m-%d %H:%i:%s.%f")])
            }
            ColumnKind::Binary => func("LOWER", vec![func("HEX", vec![column])]),
        }
    }

    fn fold(&self, hex16: Expr) -> Expr {
        let as_unsigned = cast(
            func(
                "CONV",
                vec![
                    hex16,
                    Expr::Literal("16".into()),
                    Expr::Literal("10".into()),
                ],
            ),
            "UNSIGNED",
        );
        // SUM over BIGINT UNSIGNED is a DECIMAL(65), so it does not overflow.
        Expr::BinaryOp(Box::new(BinaryOp {
            left: func("SUM", vec![as_unsigned]),
            op: BinaryOperator::Modulo,
            right: Expr::Literal(TWO_POW_64.into()),
        }))
    }

    fn int_key_type(&self) -> &'static str {
        "SIGNED"
    }
}

fn is_tinyint_bool(column: &ColumnMetadata) -> bool {
    const PREFIX: &str = "tinyint(1)";
    column
        .full_column_type
        .as_deref()
        .and_then(|s| s.get(..PREFIX.len()))
        .is_some_and(|p| p.eq_ignore_ascii_case(PREFIX))
}

fn trim_trailing(expr: Expr, chars: &str) -> Expr {
    Expr::TrimTrailing {
        expr: Box::new(expr),
        chars: chars.to_owned(),
    }
}

use super::{
    ColumnKind, NormalSql,
    expr::{cast, func, text},
};
use crate::sql::metadata::column::ColumnMetadata;
use query_builder::{
    ast::expr::{BinaryOp, BinaryOperator, Expr},
    dialect::{Postgres, QueryDialect},
};

const TWO_TO_64: &str = "18446744073709551616";
const TS_FORMAT: &str = "YYYY-MM-DD HH24:MI:SS.US";

/// PostgreSQL 13+ normal text (`trim_scale`). Runs in a session with
/// `TIME ZONE 'UTC'` and `client_encoding = 'UTF8'`.
pub struct PostgresNormal;

impl NormalSql for PostgresNormal {
    fn dialect(&self) -> &'static dyn QueryDialect {
        &Postgres
    }

    fn kind(&self, column: &ColumnMetadata) -> Option<ColumnKind> {
        // An enum's `data_type` is its type's own name.
        if column.is_enum() {
            return Some(ColumnKind::Enum);
        }

        Some(match column.data_type.to_ascii_lowercase().as_str() {
            "smallint" | "integer" | "bigint" => ColumnKind::Int,
            "boolean" => ColumnKind::Bool,
            "numeric" => ColumnKind::Decimal {
                scale: column.num_scale.unwrap_or(0),
            },
            "text" | "character varying" | "character" => ColumnKind::Text,
            "date" => ColumnKind::Date,
            "timestamp without time zone" => ColumnKind::Timestamp,
            "timestamp with time zone" => ColumnKind::TimestampTz,
            "uuid" => ColumnKind::Uuid,
            "bytea" => ColumnKind::Binary,
            _ => return None,
        })
    }

    fn normal(&self, kind: ColumnKind, column: Expr) -> Expr {
        match kind {
            // `char(n)::text` strips the padding, as the reader does.
            ColumnKind::Int | ColumnKind::Text | ColumnKind::Enum | ColumnKind::Uuid => {
                cast(column, "text")
            }
            // Through integer, so NULL stays NULL (a `CASE WHEN c` would turn
            // it into '0').
            ColumnKind::Bool => cast(cast(column, "integer"), "text"),
            ColumnKind::Decimal { .. } => cast(func("trim_scale", vec![column]), "text"),
            ColumnKind::Date => func("to_char", vec![column, text("YYYY-MM-DD")]),
            ColumnKind::Timestamp => func("to_char", vec![column, text(TS_FORMAT)]),
            // `timezone('UTC', c)` is `c AT TIME ZONE 'UTC'`.
            ColumnKind::TimestampTz => func(
                "to_char",
                vec![func("timezone", vec![text("UTC"), column]), text(TS_FORMAT)],
            ),
            ColumnKind::Binary => func("encode", vec![column, text("hex")]),
        }
    }

    fn fold(&self, hex16: Expr) -> Expr {
        // The hex read as a signed bigint, summed as numeric, then brought
        // into [0, 2^64): the same residue as MySQL's unsigned sum.
        let signed = cast(
            cast(
                cast(func("CONCAT", vec![text("x"), hex16]), "bit(64)"),
                "bigint",
            ),
            "numeric",
        );
        let m = || Expr::Literal(TWO_TO_64.into());
        let residue = func("mod", vec![func("SUM", vec![signed]), m()]);
        func(
            "mod",
            vec![
                Expr::BinaryOp(Box::new(BinaryOp {
                    left: residue,
                    op: BinaryOperator::Plus,
                    right: m(),
                })),
                m(),
            ],
        )
    }

    fn int_key_type(&self) -> &'static str {
        "BIGINT"
    }
}

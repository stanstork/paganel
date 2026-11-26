use super::{
    NormalColumn, NormalSql, Segments,
    expr::{cast, column, framed, func, op, table},
};
use crate::{error::DbError, sql::filter::SqlFilter, sql_filter_expr};
use model::core::value::Value;
use query_builder::{
    ast::{
        common::JoinKind,
        expr::{BinaryOperator, Expr, FunctionCall},
        select::{Select, TableSource},
    },
    builder::select::SelectBuilder,
};

fn normal_columns<'a>(
    sql: &'a dyn NormalSql,
    table_name: &'a str,
    columns: &'a [NormalColumn],
) -> impl Iterator<Item = Expr> + 'a {
    columns.iter().enumerate().map(move |(i, c)| {
        sql.normal(c.kind, column(Some(table_name), &c.name))
            .alias(format!("c{i}"))
    })
}

/// A pair side's `where` filter as an expression.
pub fn filter_expr(filter: Option<&SqlFilter>) -> Result<Option<Expr>, DbError> {
    match filter.and_then(|f| f.expr.as_ref()) {
        Some(expr) => sql_filter_expr!(expr).map(Some),
        None => Ok(None),
    }
}

/// `SELECT MIN(key) AS lo, MAX(key) AS hi`: the range a level-0 grid covers.
pub fn key_range_query(table_name: &str, key: &str, filter: Option<&Expr>) -> Select {
    let k = || column(Some(table_name), key);
    let builder = SelectBuilder::new()
        .select(vec![
            func("MIN", vec![k()]).alias("lo"),
            func("MAX", vec![k()]).alias("hi"),
        ])
        .from(table(table_name), Some(table_name));

    match filter {
        Some(f) => builder.where_clause(f.clone()).build(),
        None => builder.build(),
    }
}

/// `SELECT k0, k1, ..., <normal text of each column> ... ORDER BY the keys
/// LIMIT n`: the key columns as stored (aliased `k0`, `k1`, ...) and each
/// column's normal text (aliased by name).
pub fn normal_select(
    sql: &dyn NormalSql,
    table_name: &str,
    keys: &[String],
    columns: &[NormalColumn],
    filter: Option<&Expr>,
    limit: Option<usize>,
) -> Select {
    let mut select: Vec<Expr> = keys
        .iter()
        .enumerate()
        .map(|(i, k)| column(Some(table_name), k).alias(format!("k{i}")))
        .collect();
    select.extend(columns.iter().map(|c| {
        sql.normal(c.kind, column(Some(table_name), &c.name))
            .alias(c.name.clone())
    }));

    let mut builder = SelectBuilder::new()
        .select(select)
        .from(table(table_name), Some(table_name));
    for k in keys {
        builder = builder.order_by(column(Some(table_name), k), None);
    }
    if let Some(f) = filter {
        builder = builder.where_clause(f.clone());
    }
    if let Some(n) = limit {
        builder = builder.limit(Expr::Literal(n.to_string()));
    }
    builder.build()
}

/// Row count and fingerprint per segment of `table` on its integer `key`:
/// `(seg, n, h1, h2)`, one row per non-empty segment, ordered by `seg`.
pub fn fingerprint_query(
    sql: &dyn NormalSql,
    table_name: &str,
    key: &str,
    columns: &[NormalColumn],
    segments: &Segments,
    filter: Option<&Expr>,
) -> Select {
    let inner = match segments {
        Segments::Grid { lo, width } => {
            let k = || column(Some(table_name), key);
            let lo = || Expr::Value(Value::Int(*lo));
            let seg = Expr::Case {
                when_branches: vec![(
                    op(k(), BinaryOperator::Lt, lo()),
                    Expr::Literal("-1".into()),
                )],
                else_expr: Some(Box::new(op(
                    op(k(), BinaryOperator::Minus, lo()),
                    BinaryOperator::IntDiv,
                    Expr::Value(Value::Int(*width)),
                ))),
            };
            let mut select = vec![seg.alias("seg")];
            select.extend(normal_columns(sql, table_name, columns));
            SelectBuilder::new()
                .select(select)
                .from(table(table_name), Some(table_name))
        }
        Segments::Ranges(ranges) => {
            let bound = |i: i64| cast(Expr::Value(Value::Int(i)), sql.int_key_type());
            let rows = ranges
                .iter()
                .map(|&(seg, lo, hi)| vec![bound(seg), bound(lo), bound(hi)])
                .collect();
            let k = || column(Some(table_name), key);
            let on = op(
                op(k(), BinaryOperator::GtEq, column(Some("r"), "lo")),
                BinaryOperator::And,
                op(k(), BinaryOperator::LtEq, column(Some("r"), "hi")),
            );
            let mut select = vec![column(Some("r"), "seg").alias("seg")];
            select.extend(normal_columns(sql, table_name, columns));
            SelectBuilder::new()
                .select(select)
                .from_source(
                    TableSource::Values {
                        columns: vec!["seg".into(), "lo".into(), "hi".into()],
                        rows,
                    },
                    "r",
                )
                .join(JoinKind::Inner, table(table_name), Some(table_name), on)
        }
    };

    let inner = match filter {
        Some(f) => inner.where_clause(f.clone()).build(),
        None => inner.build(),
    };

    // Middle: the row's MD5.
    let row_text = sql.concat(
        (0..columns.len())
            .map(|i| framed(sql, column(None, &format!("c{i}"))))
            .collect(),
    );
    let middle = SelectBuilder::new()
        .select(vec![column(None, "seg"), sql.md5_hex(row_text).alias("h")])
        .from_source(TableSource::Subquery(Box::new(inner)), "v")
        .build();

    // Outer: count and the two folded halves per segment.
    let h = || column(None, "h");
    SelectBuilder::new()
        .select(vec![
            column(None, "seg"),
            Expr::FunctionCall(FunctionCall::count_all()).alias("n"),
            sql.fold(func("LEFT", vec![h(), Expr::Literal("16".into())]))
                .alias("h1"),
            sql.fold(func("SUBSTR", vec![h(), Expr::Literal("17".into())]))
                .alias("h2"),
        ])
        .from_source(TableSource::Subquery(Box::new(middle)), "x")
        .group_by(vec![column(None, "seg")])
        .order_by(column(None, "seg"), None)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::checksum::{ColumnKind, MySqlNormal, PostgresNormal};
    use query_builder::renderer::{Render, Renderer};

    fn render(select: &Select, sql: &dyn NormalSql) -> String {
        let mut r = Renderer::new(sql.dialect());
        select.render(&mut r);
        r.finish().0
    }

    fn columns() -> Vec<NormalColumn> {
        vec![
            NormalColumn {
                name: "id".into(),
                kind: ColumnKind::Int,
            },
            NormalColumn {
                name: "amount".into(),
                kind: ColumnKind::Decimal { scale: 2 },
            },
        ]
    }

    #[test]
    fn level_zero_query_on_mysql() {
        let q = fingerprint_query(
            &MySqlNormal,
            "orders",
            "id",
            &columns(),
            &Segments::Grid { lo: 1, width: 10 },
            None,
        );
        assert_eq!(
            render(&q, &MySqlNormal),
            "SELECT `seg`, COUNT(*) AS `n`, \
             (SUM(CAST(CONV(LEFT(`h`, 16), 16, 10) AS UNSIGNED)) % 18446744073709551616) AS `h1`, \
             (SUM(CAST(CONV(SUBSTR(`h`, 17), 16, 10) AS UNSIGNED)) % 18446744073709551616) AS `h2` \
             FROM (SELECT `seg`, MD5(CONCAT(\
             CASE WHEN `c0` IS NULL THEN '~' ELSE CONCAT(OCTET_LENGTH(`c0`), ':', `c0`) END, \
             CASE WHEN `c1` IS NULL THEN '~' ELSE CONCAT(OCTET_LENGTH(`c1`), ':', `c1`) END)) AS `h` \
             FROM (SELECT CASE WHEN (`orders`.`id` < ?) THEN -1 ELSE ((`orders`.`id` - ?) DIV ?) END AS `seg`, \
             CAST(`orders`.`id` AS CHAR) AS `c0`, \
             TRIM(TRAILING '.' FROM TRIM(TRAILING '0' FROM CAST(`orders`.`amount` AS CHAR))) AS `c1` \
             FROM `orders` AS `orders`) AS `v`) AS `x` GROUP BY `seg` ORDER BY `seg`"
        );
    }

    #[test]
    fn ranges_query_on_postgres() {
        let q = fingerprint_query(
            &PostgresNormal,
            "orders",
            "id",
            &columns(),
            &Segments::Ranges(vec![(3, 30, 39)]),
            None,
        );
        let sql = render(&q, &PostgresNormal);
        assert!(
            sql.contains(
                r#"FROM (VALUES (CAST($1 AS BIGINT), CAST($2 AS BIGINT), CAST($3 AS BIGINT))) AS "r" ("seg", "lo", "hi") INNER JOIN "orders" AS "orders" ON (("orders"."id" >= "r"."lo") AND ("orders"."id" <= "r"."hi"))"#
            ),
            "{sql}"
        );
        assert!(
            sql.contains(r#"CAST(trim_scale("orders"."amount") AS text) AS "c1""#),
            "{sql}"
        );
    }

    /// MySQL's SQL with ClickHouse-style hashing primitives, to check that
    /// the row hash is built only from `NormalSql`'s hooks.
    struct OtherEngine;

    impl NormalSql for OtherEngine {
        fn dialect(&self) -> &'static dyn query_builder::dialect::QueryDialect {
            MySqlNormal.dialect()
        }
        fn kind(
            &self,
            column: &crate::sql::metadata::column::ColumnMetadata,
        ) -> Option<ColumnKind> {
            MySqlNormal.kind(column)
        }
        fn normal(&self, kind: ColumnKind, column: Expr) -> Expr {
            MySqlNormal.normal(kind, column)
        }
        fn fold(&self, hex16: Expr) -> Expr {
            MySqlNormal.fold(hex16)
        }
        fn int_key_type(&self) -> &'static str {
            MySqlNormal.int_key_type()
        }
        fn byte_length(&self, text: Expr) -> Expr {
            func("length", vec![text])
        }
        fn concat(&self, parts: Vec<Expr>) -> Expr {
            func("concat", parts)
        }
        fn md5_hex(&self, text: Expr) -> Expr {
            func("lower", vec![func("hex", vec![func("MD5", vec![text])])])
        }
    }

    #[test]
    fn the_row_hash_uses_the_dialects_primitives() {
        let q = fingerprint_query(
            &OtherEngine,
            "orders",
            "id",
            &columns(),
            &Segments::Grid { lo: 1, width: 10 },
            None,
        );
        let sql = render(&q, &OtherEngine);
        assert!(
            sql.contains("lower(hex(MD5(concat(CASE WHEN `c0` IS NULL THEN '~' ELSE concat(length(`c0`), ':', `c0`) END"),
            "{sql}"
        );
        assert!(!sql.contains("OCTET_LENGTH"), "{sql}");
        assert!(!sql.contains("CONCAT("), "{sql}");
    }
}

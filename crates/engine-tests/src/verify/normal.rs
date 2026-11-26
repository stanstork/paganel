#[cfg(test)]
mod tests {
    use crate::harness::Direction;
    use connectors::{
        session::SessionSettings,
        sql::{
            checksum::{
                self, MySqlNormal, NormalColumn, NormalSql, PostgresNormal, Segments,
                fingerprint_query, normal_select, parse_segment_hash,
            },
            request::FetchRowsRequestBuilder,
        },
        traits::{executor::QueryExecutor, reader::DataReader},
    };
    use engine_core::{dispatch_driver, drivers::DriverRef, plan::execution::ExecutionPlan};
    use engine_processing::EnvContext;
    use model::{
        core::value::Value,
        integrity::normal::{Family, normal_text},
        records::Record,
    };
    use ppl_syntax::builder::parse;
    use query_builder::{
        ast::select::Select,
        renderer::{Render, Renderer},
    };
    use std::{collections::BTreeMap, sync::Arc};
    use tracing_test::traced_test;

    const COLUMNS: [&str; 12] = [
        "i", "flag", "yr", "amount", "wide", "name", "code", "status", "d", "dt", "ts", "raw",
    ];

    const MYSQL_TABLE: &str = "CREATE TABLE blk_corpus (
        id INT PRIMARY KEY, i BIGINT, flag TINYINT(1), yr YEAR,
        amount DECIMAL(12,2), wide DECIMAL(65,30), name VARCHAR(50), code CHAR(3),
        status ENUM('new','paid'), d DATE, dt DATETIME(6), ts TIMESTAMP(6) NULL,
        raw VARBINARY(16))";

    const PG_TABLE: &str = "CREATE TABLE blk_corpus (
        id integer PRIMARY KEY, i bigint, flag boolean, yr smallint,
        amount numeric(12,2), wide numeric(65,30), name varchar(50), code char(3),
        status blk_status, d date, dt timestamp(6), ts timestamptz(6), raw bytea)";

    /// One row per entry; each value as (MySQL literal, Postgres literal).
    fn rows() -> Vec<Vec<(&'static str, &'static str)>> {
        let same = |s: &'static str| (s, s);
        vec![
            vec![
                same("1"),
                same("0"),
                ("0", "false"),
                same("2006"),
                same("0.00"),
                same("0"),
                same("''"),
                same("'US'"),
                same("'new'"),
                same("'1000-01-01'"),
                same("'1970-01-01 00:00:00'"),
                ("'1970-01-01 00:00:01+00:00'", "'1970-01-01 00:00:01+00'"),
                ("X''", "''::bytea"),
            ],
            vec![
                same("2"),
                same("-1"),
                ("1", "true"),
                same("1999"),
                same("-0.50"),
                same("1.000000000000000000000000000001"),
                same("' '"),
                same("'USD'"),
                same("'paid'"),
                same("'1969-12-31'"),
                same("'1969-12-31 23:59:59.999999'"),
                (
                    "'2024-01-15 05:00:00.000001+00:00'",
                    "'2024-01-15 05:00:00.000001+00'",
                ),
                ("X'00'", "'\\x00'::bytea"),
            ],
            vec![
                same("3"),
                same("9223372036854775807"),
                ("1", "true"),
                same("2155"),
                same("100.00"),
                same("12345678901234567890123456789012345.5"),
                same("'a  '"),
                same("'X'"),
                same("NULL"),
                same("'2024-02-29'"),
                same("'2024-02-29 12:00:00.5'"),
                (
                    "'2038-01-19 03:14:07.999999+00:00'",
                    "'2038-01-19 03:14:07.999999+00'",
                ),
                ("X'ff00'", "'\\xff00'::bytea"),
            ],
            vec![
                same("4"),
                same("-9223372036854775808"),
                ("0", "false"),
                same("NULL"),
                same("1.10"),
                same("-0.000000000000000000000000000001"),
                same("'e\u{301} \u{e9} \u{1F600}'"),
                same("'1:x'"),
                same("'new'"),
                same("'9999-12-31'"),
                same("'9999-12-31 23:59:59.999999'"),
                same("NULL"),
                ("X'616263'", "'\\x616263'::bytea"),
            ],
            vec![
                same("5"),
                same("NULL"),
                same("NULL"),
                same("NULL"),
                same("NULL"),
                same("NULL"),
                same("NULL"),
                same("NULL"),
                same("NULL"),
                same("NULL"),
                same("NULL"),
                same("NULL"),
                same("NULL"),
            ],
            vec![
                same("6"),
                same("42"),
                ("0", "false"),
                same("2000"),
                same("12345.67"),
                same("100"),
                same("'~'"),
                same("'~'"),
                same("'paid'"),
                same("'2000-01-01'"),
                same("'2000-01-01 00:00:00.000100'"),
                ("'2000-01-01 00:00:00+00:00'", "'2000-01-01 00:00:00+00'"),
                ("X'c328'", "'\\xc328'::bytea"),
            ],
            vec![
                same("7"),
                same("7"),
                ("1", "true"),
                same("2024"),
                same("0.01"),
                same("-12.5"),
                same("'it''s'"),
                same("'A  '"),
                same("'new'"),
                same("'2024-03-10'"),
                same("'2024-03-10 02:30:00'"),
                ("'2024-03-10 02:30:00+00:00'", "'2024-03-10 02:30:00+00'"),
                ("X'0a'", "'\\x0a'::bytea"),
            ],
        ]
    }

    fn insert(row: &[(&str, &str)], mysql: bool) -> String {
        let values: Vec<&str> = row
            .iter()
            .map(|(m, p)| if mysql { *m } else { *p })
            .collect();
        format!("INSERT INTO blk_corpus VALUES ({})", values.join(", "))
    }

    async fn seed() {
        let mysql = Direction::POSTGRES_TO_MYSQL;
        let pg = Direction::MYSQL_TO_POSTGRES;

        mysql.dst_execute("DROP TABLE IF EXISTS blk_corpus").await;
        mysql.dst_execute(MYSQL_TABLE).await;
        pg.dst_execute("DROP TABLE IF EXISTS blk_corpus").await;
        pg.dst_execute("DROP TYPE IF EXISTS blk_status").await;
        pg.dst_execute("CREATE TYPE blk_status AS ENUM ('new', 'paid')")
            .await;
        pg.dst_execute(PG_TABLE).await;

        for row in rows() {
            mysql.dst_execute(&insert(&row, true)).await;
            pg.dst_execute(&insert(&row, false)).await;
        }
    }

    async fn session(dir: Direction) -> DriverRef {
        let doc = parse(&dir.ppl("")).expect("parse");
        let plan = ExecutionPlan::build(&doc, Arc::new(EnvContext::empty())).expect("plan");
        DriverRef::open_session(
            plan.get_connection("dst").expect("dst"),
            &SessionSettings::default(),
        )
        .await
        .expect("session")
    }

    fn render(select: &Select, sql: &dyn NormalSql) -> (String, Vec<Value>) {
        let mut r = Renderer::new(sql.dialect());
        select.render(&mut r);
        r.finish()
    }

    /// The row's id: `id` in reader rows, `k0` in `normal_select` rows.
    fn id(record: &Record) -> i64 {
        match record.value("id").or_else(|| record.value("k0")) {
            Some(Value::Int(i)) => *i,
            Some(Value::UInt(u)) => *u as i64,
            other => panic!("id: {other:?}"),
        }
    }

    /// A text the database returned: `None` for NULL.
    fn returned_text(value: Option<&Value>) -> Option<String> {
        match value? {
            Value::Null => None,
            Value::String(s) => Some(s.clone()),
            Value::Binary(b) => Some(String::from_utf8(b.clone()).expect("utf-8")),
            other => panic!("not text: {other:?}"),
        }
    }

    /// Everything one side says about the corpus.
    struct Side {
        kinds: Vec<NormalColumn>,
        /// Per row id: the SQL text of each column.
        sql_text: BTreeMap<i64, Vec<Option<String>>>,
        /// Per row id: each column's value as the v1 reader returns it.
        values: BTreeMap<i64, Vec<Value>>,
    }

    async fn side(driver: &DriverRef, sql: &dyn NormalSql) -> Side {
        let meta = driver.table_metadata("blk_corpus").await.expect("metadata");
        let kinds: Vec<NormalColumn> = COLUMNS
            .iter()
            .map(|name| {
                let column = &meta.columns[*name];
                let kind = sql
                    .kind(column)
                    .unwrap_or_else(|| panic!("{name} ({}) has no kind", column.data_type));
                NormalColumn {
                    name: name.to_string(),
                    kind,
                }
            })
            .collect();

        let select = normal_select(sql, "blk_corpus", &["id".to_string()], &kinds, None, None);
        let (query, params) = render(&select, sql);
        let rows = dispatch_driver!(driver, |d| d.query_params(&query, &params).await)
            .unwrap_or_else(|e| panic!("{query}: {e}"));
        let sql_text = rows
            .iter()
            .map(|r| {
                let texts = COLUMNS.iter().map(|c| returned_text(r.value(c))).collect();
                (id(r), texts)
            })
            .collect();

        let request = FetchRowsRequestBuilder::new("blk_corpus".into())
            .alias("blk_corpus".into())
            .columns(meta.select_fields())
            .limit(100)
            .build();
        let rows = dispatch_driver!(driver, |d| d.fetch(request).await).expect("fetch");
        let values = rows
            .iter()
            .map(|r| {
                let vals = COLUMNS
                    .iter()
                    .map(|c| r.value(c).cloned().unwrap_or(Value::Null))
                    .collect();
                (id(r), vals)
            })
            .collect();

        Side {
            kinds,
            sql_text,
            values,
        }
    }

    async fn hashes(
        driver: &DriverRef,
        sql: &dyn NormalSql,
        kinds: &[NormalColumn],
        segments: &Segments,
    ) -> Vec<checksum::SegmentHash> {
        let select = fingerprint_query(sql, "blk_corpus", "id", kinds, segments, None);
        let (query, params) = render(&select, sql);
        let rows = dispatch_driver!(driver, |d| d.query_params(&query, &params).await)
            .unwrap_or_else(|e| panic!("{query}: {e}"));
        rows.iter()
            .map(|r| parse_segment_hash(r).expect("segment row"))
            .collect()
    }

    /// For every row and column: MySQL's SQL text, Postgres's SQL text, and
    /// `normal_text` of each reader's value are the same. Then the
    /// fingerprints agree too, at level 0 and over ranges.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn normal_text_agrees_across_dialects_and_rust() {
        seed().await;
        let mysql_driver = session(Direction::POSTGRES_TO_MYSQL).await;
        let pg_driver = session(Direction::MYSQL_TO_POSTGRES).await;
        let mysql = side(&mysql_driver, &MySqlNormal).await;
        let pg = side(&pg_driver, &PostgresNormal).await;

        let families: Vec<Family> = mysql
            .kinds
            .iter()
            .zip(&pg.kinds)
            .map(|(m, p)| {
                checksum::family(m.kind, p.kind)
                    .unwrap_or_else(|| panic!("{}: {:?} vs {:?}", m.name, m.kind, p.kind))
            })
            .collect();

        assert_eq!(mysql.sql_text.len(), rows().len());
        let mut wrong = Vec::new();
        for (row, mysql_sql) in &mysql.sql_text {
            for (c, family) in families.iter().enumerate() {
                let rust = |side: &Side| {
                    normal_text(*family, &side.values[row][c]).unwrap_or_else(|e| panic!("{e}"))
                };
                let seen = [
                    ("mysql sql", mysql_sql[c].clone()),
                    ("pg sql", pg.sql_text[row][c].clone()),
                    ("mysql rust", rust(&mysql)),
                    ("pg rust", rust(&pg)),
                ];
                if seen.iter().any(|(_, t)| *t != seen[0].1) {
                    wrong.push(format!("row {row} {}: {seen:?}", COLUMNS[c]));
                }
            }
        }
        assert!(
            wrong.is_empty(),
            "normal text differs:\n{}",
            wrong.join("\n")
        );

        for segments in [
            Segments::Grid { lo: 1, width: 3 },
            Segments::Ranges(vec![(0, 1, 2), (5, 6, 100)]),
        ] {
            let m = hashes(&mysql_driver, &MySqlNormal, &mysql.kinds, &segments).await;
            let p = hashes(&pg_driver, &PostgresNormal, &pg.kinds, &segments).await;
            assert!(!m.is_empty());
            assert_eq!(m, p, "{segments:?}");
        }
    }
}

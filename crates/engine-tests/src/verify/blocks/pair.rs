#[cfg(test)]
mod tests {
    use crate::harness::{Dbms, Direction, ppl::dest_connection};
    use engine_core::plan::{execution::ExecutionPlan, pair::BlockMode};
    use engine_processing::EnvContext;
    use engine_verify::{NoopProgress, VerifyOptions, VerifyState, discover_targets, run_drift};
    use model::integrity::{
        drift::DriftRun,
        report::{ReportResult, TableReport},
    };
    use ppl_syntax::builder::parse;
    use std::sync::Arc;
    use tracing_test::traced_test;

    const ROWS: u64 = 40_000;

    /// The same 40,000 orders in the scratch MySQL and Postgres.
    async fn seed(table: &str) {
        let mysql = Direction::POSTGRES_TO_MYSQL;
        let pg = Direction::MYSQL_TO_POSTGRES;

        mysql
            .dst_execute(&format!("DROP TABLE IF EXISTS {table}"))
            .await;
        mysql
            .dst_execute(&format!(
                "CREATE TABLE {table} (id BIGINT PRIMARY KEY, customer_id INT,
                 amount DECIMAL(12,2), status VARCHAR(10), created_at DATETIME(6),
                 is_gift TINYINT(1), note TEXT NULL, ratio DOUBLE)"
            ))
            .await;
        let digits = "(SELECT 0 d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3
            UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7
            UNION ALL SELECT 8 UNION ALL SELECT 9)";
        mysql
            .dst_execute(&format!(
                "INSERT INTO {table}
                 SELECT n, n % 997, (n % 10000) / 100,
                        ELT(1 + n % 3, 'new', 'paid', 'shipped'),
                        TIMESTAMP('2024-01-01') + INTERVAL n SECOND, n % 2,
                        IF(n % 5 = 0, NULL, CONCAT('note ', n)), n / 7
                 FROM (SELECT a.d + 10 * b.d + 100 * c.d + 1000 * e.d + 10000 * f.d + 1 AS n
                       FROM {digits} a, {digits} b, {digits} c, {digits} e, {digits} f) s
                 WHERE n <= {ROWS}"
            ))
            .await;

        pg.dst_execute(&format!("DROP TABLE IF EXISTS {table}"))
            .await;
        pg.dst_execute(&format!(
            "CREATE TABLE {table} (id bigint PRIMARY KEY, customer_id integer,
             amount numeric(12,2), status varchar(10), created_at timestamp(6),
             is_gift boolean, note text, ratio double precision)"
        ))
        .await;
        pg.dst_execute(&format!(
            "INSERT INTO {table}
             SELECT n, n % 997, (n % 10000) / 100.0,
                    (ARRAY['new', 'paid', 'shipped'])[1 + n % 3],
                    timestamp '2024-01-01' + n * interval '1 second', n % 2 = 1,
                    CASE WHEN n % 5 = 0 THEN NULL ELSE 'note ' || n END, n / 7.0
             FROM generate_series(1, {ROWS}) n"
        ))
        .await;
    }

    /// Edit the Postgres copy: one row missing, three changed, one extra
    /// past the end of the key range.
    async fn tamper(table: &str) {
        let pg = Direction::MYSQL_TO_POSTGRES;
        for sql in [
            format!("DELETE FROM {table} WHERE id = 777"),
            format!("UPDATE {table} SET amount = amount + 0.01 WHERE id = 12345"),
            format!("UPDATE {table} SET note = NULL WHERE id = 30001"),
            format!("UPDATE {table} SET is_gift = NOT is_gift WHERE id = 20000"),
            format!(
                "INSERT INTO {table} VALUES (50000, 1, 1.00, 'new', '2024-06-01', false, NULL, 0)"
            ),
        ] {
            pg.dst_execute(&sql).await;
        }
    }

    /// Two pairs over the copies: the whole table, and `id <= 20000`.
    /// `ignore_ratio` leaves out the `DOUBLE` column block mode can't
    /// compare (MySQL's `n / 7` and Postgres's `n / 7.0` differ anyway).
    fn config(table: &str, ignore_ratio: bool) -> String {
        let ignore = if ignore_ratio {
            r#"ignore = ["ratio"]"#
        } else {
            ""
        };
        format!(
            r#"{}{}
            pair "orders" {{
                source {{ connection = connection.my table = "{table}" {ignore} }}
                target {{ connection = connection.pg table = "{table}" {ignore} }}
                grace  = "0s"
            }}
            pair "orders_where" {{
                source {{
                    connection = connection.my
                    table      = "{table}"
                    where      {{ id <= 20000 }}
                    {ignore}
                }}
                target {{
                    connection = connection.pg
                    table      = "{table}"
                    where      {{ id <= 20000 }}
                    {ignore}
                }}
                grace = "0s"
            }}
            "#,
            dest_connection("my", Dbms::MySql),
            dest_connection("pg", Dbms::Postgres),
        )
    }

    /// One drift run over every pair, each set to `mode` and `leaf_rows`.
    async fn drift(ppl: &str, mode: BlockMode, leaf_rows: u64) -> DriftRun {
        let doc = parse(ppl).expect("parse");
        let env = Arc::new(EnvContext::empty());
        let mut plan = ExecutionPlan::build(&doc, env.clone()).expect("plan");
        for pair in &mut plan.pairs {
            pair.block.mode = mode;
            pair.block.leaf_rows = leaf_rows;
        }
        let state = VerifyState::open_default().expect("state");
        let (targets, _) = discover_targets(&plan, &state, &[]).await.expect("targets");
        run_drift(
            &plan,
            env,
            &state,
            &targets,
            &mut NoopProgress,
            VerifyOptions::default(),
        )
        .await
        .expect("drift")
    }

    /// Result, row counts, counts per kind and the divergent keys, sorted
    /// (divergences come in key-byte order, not numeric order).
    fn summary(t: &TableReport) -> (ReportResult, u64, Option<u64>, [u64; 3], Vec<String>) {
        let mut keys: Vec<String> = t.divergences.iter().map(|d| d.key.clone()).collect();
        keys.sort();
        (
            t.result,
            t.expected_rows,
            t.actual_rows,
            [t.missing, t.changed, t.extra],
            keys,
        )
    }

    /// Block mode, narrowing all the way down, reports exactly what a full
    /// read reports, with and without a `where`. In `Blocks` mode a
    /// fallback is an error, so a result at all shows block mode ran.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn block_mode_finds_what_a_full_read_finds() {
        let table = "blk_orders_full";
        seed(table).await;
        let ppl = config(table, true);

        let clean = drift(&ppl, BlockMode::Blocks, 5).await;
        for t in &clean.tables {
            assert_eq!(
                t.result,
                ReportResult::Match,
                "{}: {:?}",
                t.pipeline,
                t.message
            );
        }
        assert_eq!(clean.tables[0].expected_rows, ROWS);
        assert_eq!(clean.tables[1].expected_rows, 20_000);

        tamper(table).await;
        let blocks = drift(&ppl, BlockMode::Blocks, 5).await;
        let full = drift(&ppl, BlockMode::Full, 5).await;

        for (b, f) in blocks.tables.iter().zip(&full.tables) {
            assert_eq!(summary(b), summary(f), "{}", b.pipeline);
        }
        let orders = summary(&blocks.tables[0]);
        assert_eq!(orders.3, [1, 3, 1]);
        assert_eq!(
            orders.4,
            ["id=12345", "id=20000", "id=30001", "id=50000", "id=777"]
        );
        assert_eq!(summary(&blocks.tables[1]).3, [1, 2, 0], "the where applies");
    }

    /// A column block mode can't compare: `Auto` reads in full and gets
    /// the same answer as `Full`; `Blocks` refuses and names the column.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn an_unsupported_column_falls_back_or_refuses() {
        let table = "blk_orders_ratio";
        seed(table).await;
        tamper(table).await;
        let ppl = config(table, false);

        let auto = drift(&ppl, BlockMode::Auto, 5).await;
        let full = drift(&ppl, BlockMode::Full, 5).await;
        assert_eq!(summary(&auto.tables[0]), summary(&full.tables[0]));

        let refused = drift(&ppl, BlockMode::Blocks, 5).await;
        let message = refused.tables[0].message.clone().unwrap_or_default();
        assert!(
            message.contains("block mode") && message.contains("ratio"),
            "{message}"
        );
    }
}

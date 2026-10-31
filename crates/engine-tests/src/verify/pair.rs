#[cfg(test)]
mod tests {
    use crate::{
        harness::{Direction, runner::run_ppl},
        verify::support::drift_run,
    };
    use model::integrity::report::{Recheck, ReportResult};
    use std::time::Duration;
    use tracing_test::traced_test;

    /// A pair reads both live tables. Through `map` and each side's `where`, a
    /// deleted and a changed target row are reported by key, and a change outside
    /// the `where` is not. A column one side lacks fails that pair alone.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn pair_reports_target_rows_that_differ() {
        for dir in [
            Direction::MYSQL_TO_POSTGRES,
            Direction::POSTGRES_TO_POSTGRES,
        ] {
            dir.reset().await;
            let ppl = dir.ppl(
                r#"
            pipeline "copy_actor" {
                from { connection = connection.src table = "actor" }
                to   { connection = connection.dst table = "actor_copy" }
                select {
                    actor_id    = actor.actor_id
                    given_name  = actor.first_name
                    last_name   = actor.last_name
                    last_update = actor.last_update
                }
                settings { create_missing_tables = true  batch_size = 100 }
            }

            pair "actor" {
                source {
                    connection = connection.src
                    table      = "actor"
                    where      { actor_id <= 150 }
                }
                target {
                    connection = connection.dst
                    table      = "actor_copy"
                    where      { actor_id <= 150 }
                }
                map   { source.first_name = target.given_name }
                grace = "0s"
            }

            pair "unmapped" {
                source { connection = connection.src table = "actor" }
                target { connection = connection.dst table = "actor_copy" }
            }
            "#,
            );
            run_ppl(&ppl, false).await.expect("apply failed");

            dir.dst_execute("DELETE FROM actor_copy WHERE actor_id = 1")
                .await;
            dir.dst_execute(
                "UPDATE actor_copy SET given_name = 'TAMPERED' WHERE actor_id IN (2, 160)",
            )
            .await;

            let run = drift_run(&ppl, &["actor", "unmapped"]).await;
            assert_eq!(run.exit_code(), 3, "[{dir}]");

            let actor = &run.tables[0];
            assert_eq!(actor.pipeline, "@pair/actor");
            assert_eq!(
                actor.result,
                ReportResult::Mismatch,
                "[{dir}] {:?}",
                actor.message
            );
            assert_eq!(actor.expected_rows, 150, "[{dir}] the where applies");
            assert_eq!((actor.missing, actor.changed, actor.extra), (1, 1, 0));
            let keys: Vec<&str> = actor.divergences.iter().map(|d| d.key.as_str()).collect();
            assert_eq!(keys, ["actor_id=1", "actor_id=2"], "[{dir}]");
            assert_eq!(
                actor.recheck,
                Some(Recheck::Skipped {
                    reason: "grace is 0".into()
                })
            );

            let unmapped = &run.tables[1];
            assert_eq!(unmapped.result, ReportResult::Error);
            let message = unmapped.message.as_deref().unwrap_or_default();
            assert!(
                message.contains("source has first_name not in target")
                    && message.contains("target has given_name not in source"),
                "[{dir}] {message}"
            );
        }
    }

    /// A row in the source but not yet in the copy when both are read is not
    /// drift once it lands within `grace`; a row still different after it is.
    /// Both tables share one connection, so they are read one after the other.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn pair_rechecks_differing_keys_after_grace() {
        let dir = Direction::POSTGRES_TO_POSTGRES;
        dir.reset().await;
        for table in ["orders_a", "orders_b"] {
            dir.dst_execute(&format!(
                "CREATE TABLE {table} (id INT PRIMARY KEY, total NUMERIC(10,2));
             INSERT INTO {table} SELECT g, g * 1.5 FROM generate_series(1, 100) g"
            ))
            .await;
        }
        // In flight: written to the source, not copied yet.
        dir.dst_execute("INSERT INTO orders_a VALUES (101, 7.00)")
            .await;
        // Real drift: the copy holds another total.
        dir.dst_execute("UPDATE orders_b SET total = 0 WHERE id = 50")
            .await;

        let ppl = dir.ppl(
            r#"
        pair "orders" {
            source { connection = connection.dst table = "orders_a" }
            target { connection = connection.dst table = "orders_b" }
            grace  = "3s"
        }
        "#,
        );

        // The copy catches up a second into the grace period.
        let copier = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            dir.dst_execute("INSERT INTO orders_b VALUES (101, 7.00)")
                .await;
        });

        let run = drift_run(&ppl, &[]).await;
        copier.await.expect("copier");

        let orders = &run.tables[0];
        assert_eq!(
            orders.result,
            ReportResult::Mismatch,
            "{:?}",
            orders.message
        );
        assert_eq!((orders.missing, orders.changed, orders.extra), (0, 1, 0));
        assert_eq!(orders.divergences[0].key, "id=50");
        assert_eq!(orders.recheck, Some(Recheck::Done { settled: 1 }));
    }

    /// The same logical rows, typed the way each engine types them, must hash
    /// alike, or pair mode reports drift that is not there. One table per case
    /// so a failure names the type.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn pair_matches_the_same_rows_across_mysql_and_postgres() {
        use crate::harness::{Dbms, ppl::dest_connection};

        // (case, MySQL type, PostgreSQL type, rows as SQL tuples after the id)
        let cases = [
            (
                "bool",
                "TINYINT(1)",
                "BOOLEAN",
                ["0", "1"],
                ["false", "true"],
            ),
            (
                "datetime",
                "DATETIME(6)",
                "TIMESTAMP",
                ["'2024-01-15 10:20:30.123456'", "'1999-12-31 23:59:59'"],
                ["'2024-01-15 10:20:30.123456'", "'1999-12-31 23:59:59'"],
            ),
            (
                "decimal",
                "DECIMAL(10,2)",
                "NUMERIC",
                ["1.50", "-0.10"],
                ["1.5", "-0.1"],
            ),
            (
                "text",
                "VARCHAR(50) CHARACTER SET utf8mb4",
                "TEXT",
                ["'café ☕ 😀'", "''"],
                ["'café ☕ 😀'", "''"],
            ),
            (
                "json",
                "JSON",
                "JSONB",
                [r#"'{"b": 2, "a": [1, "x"]}'"#, "'[]'"],
                [r#"'{"a": [1, "x"], "b": 2}'"#, "'[]'"],
            ),
            (
                "unsigned",
                "INT UNSIGNED",
                "BIGINT",
                ["4000000000", "0"],
                ["4000000000", "0"],
            ),
        ];

        let (mysql, pg) = (Direction::MYSQL_TO_MYSQL, Direction::POSTGRES_TO_POSTGRES);
        mysql.reset().await;
        pg.reset().await;

        let mut ppl = format!(
            "{}{}",
            dest_connection("my", Dbms::MySql),
            dest_connection("pg", Dbms::Postgres)
        );
        for (case, my_type, pg_type, my_rows, pg_rows) in &cases {
            let table = format!("t_{case}");
            mysql
                .dst_execute(&format!(
                    "CREATE TABLE {table} (id INT PRIMARY KEY, v {my_type})"
                ))
                .await;
            pg.dst_execute(&format!(
                "CREATE TABLE {table} (id INT PRIMARY KEY, v {pg_type})"
            ))
            .await;

            // Every case also holds a NULL.
            let values =
                |rows: &[&str; 2]| format!("(1, {}), (2, {}), (3, NULL)", rows[0], rows[1]);
            mysql
                .dst_execute(&format!("INSERT INTO {table} VALUES {}", values(my_rows)))
                .await;
            pg.dst_execute(&format!("INSERT INTO {table} VALUES {}", values(pg_rows)))
                .await;

            ppl.push_str(&format!(
                r#"
            pair "{case}" {{
                source {{ connection = connection.my table = "{table}" }}
                target {{ connection = connection.pg table = "{table}" }}
                grace  = "0s"
            }}
            "#
            ));
        }

        let run = drift_run(&ppl, &[]).await;

        let failures: Vec<String> = run
            .tables
            .iter()
            .filter(|t| t.result != ReportResult::Match)
            .map(|t| {
                let keys: Vec<&str> = t.divergences.iter().map(|d| d.key.as_str()).collect();
                format!("{}: {:?} {:?} {keys:?}", t.pipeline, t.result, t.message)
            })
            .collect();
        assert_eq!(run.tables.len(), cases.len());
        assert!(failures.is_empty(), "{failures:#?}");
    }
}

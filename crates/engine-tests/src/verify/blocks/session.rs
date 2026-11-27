#[cfg(test)]
mod tests {
    use crate::verify::blocks::support::{plan, settings};
    use connectors::{
        sql::request::FetchRowsRequestBuilder,
        traits::{executor::QueryExecutor, reader::DataReader},
    };
    use engine_core::{dispatch_driver, drivers::DriverRef};
    use model::core::value::Value;
    use tracing_test::traced_test;

    fn text(v: Option<&Value>) -> String {
        match v {
            Some(Value::String(s)) => s.clone(),
            Some(other) => format!("{other:?}"),
            None => "NULL".into(),
        }
    }

    /// A pinned session keeps its settings on every query: MySQL's pool has
    /// one connection that re-runs its setup statements after each reset,
    /// Postgres ran its SETs on its own client.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn a_session_keeps_utc_and_its_timeout() {
        let plan = plan();

        let mysql = DriverRef::open_session(plan.get_connection("src").unwrap(), &settings())
            .await
            .expect("mysql session");
        // Three queries, because the pool resets the connection between
        // them; one query would pass even with `init` instead of `setup`.
        for _ in 0..3 {
            let rows = dispatch_driver!(&mysql, |d| d
                .query(
                    "SELECT @@session.time_zone AS tz, CAST(@@session.max_execution_time AS CHAR) AS t"
                )
                .await)
            .expect("query");
            assert_eq!(text(rows[0].value("tz")), "+00:00");
            assert_eq!(text(rows[0].value("t")), "600000");
        }

        let pg = DriverRef::open_session(plan.get_connection("dst").unwrap(), &settings())
            .await
            .expect("postgres session");
        let rows = dispatch_driver!(&pg, |d| d
            .query(
                "SELECT current_setting('TimeZone') AS tz, current_setting('statement_timeout') AS t"
            )
            .await)
        .expect("query");
        assert_eq!(text(rows[0].value("tz")), "UTC");
        assert_eq!(text(rows[0].value("t")), "10min");
    }

    /// `key_ranges` keeps exactly the rows in its closed ranges. It also
    /// reads `table_metadata` on the one-connection pool, so it hangs
    /// without the introspector fix.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn key_ranges_read_only_those_keys() {
        let plan = plan();
        let mysql = DriverRef::open_session(plan.get_connection("src").unwrap(), &settings())
            .await
            .expect("mysql session");
        let meta = mysql.table_metadata("actor").await.expect("metadata");

        let request = FetchRowsRequestBuilder::new("actor".into())
            .alias("actor".into())
            .columns(meta.select_fields())
            .key_ranges(
                "actor_id".into(),
                vec![
                    (Value::Int(1), Value::Int(10)),
                    (Value::Int(100), Value::Int(104)),
                ],
            )
            .limit(1_000)
            .build();
        let rows = dispatch_driver!(&mysql, |d| d.fetch(request).await).expect("fetch");

        let mut ids: Vec<i64> = rows
            .iter()
            .map(|r| match r.value("actor_id") {
                Some(Value::Int(i)) => *i,
                Some(Value::UInt(u)) => *u as i64,
                other => panic!("actor_id: {other:?}"),
            })
            .collect();
        ids.sort();
        let expected: Vec<i64> = (1..=10).chain(100..=104).collect();
        assert_eq!(ids, expected);
    }
}

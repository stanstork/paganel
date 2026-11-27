#[cfg(test)]
mod tests {
    use crate::verify::blocks::support::{plan, settings};
    use connectors::traits::executor::QueryExecutor;
    use engine_core::{dispatch_driver, drivers::DriverRef, plan::execution::ExecutionPlan};
    use engine_processing::EnvContext;
    use model::core::value::Value;
    use ppl_syntax::builder::parse;
    use std::sync::Arc;
    use tracing_test::traced_test;

    /// The two query shapes block mode builds, run for real: a segment
    /// number per row grouped over a derived table (level 0), and a `VALUES`
    /// list of key ranges joined to the table (narrowing). Same answers on
    /// Sakila (MySQL) and Pagila (Postgres), whose `actor` ids are 1..=200.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn segment_queries_run_on_both_dialects() {
        use crate::harness::Direction;
        use query_builder::{
            ast::{
                common::{JoinKind, TableRef},
                expr::{BinaryOp, BinaryOperator, Expr, FunctionCall, Ident},
                select::{Select, TableSource},
            },
            builder::select::SelectBuilder,
            dialect::{self, QueryDialect},
            renderer::{Render, Renderer},
        };

        fn op(left: Expr, op: BinaryOperator, right: Expr) -> Expr {
            Expr::BinaryOp(Box::new(BinaryOp { left, op, right }))
        }
        fn int(i: i64) -> Expr {
            Expr::Value(Value::Int(i))
        }
        fn col(q: &str, c: &str) -> Expr {
            Expr::Identifier(Ident {
                qualifier: Some(q.into()),
                name: c.into(),
            })
        }
        let actor = || TableRef {
            schema: None,
            name: "actor".into(),
        };

        // Level 0: segments of 50 ids.
        let level0 = || {
            let seg = op(
                op(Expr::column("actor_id"), BinaryOperator::Minus, int(1)),
                BinaryOperator::IntDiv,
                int(50),
            );
            let inner = SelectBuilder::new()
                .select(vec![seg.alias("seg")])
                .from(actor(), None)
                .build();
            SelectBuilder::new()
                .select(vec![
                    Expr::column("seg"),
                    Expr::FunctionCall(FunctionCall::count_all()).alias("n"),
                ])
                .from_source(TableSource::Subquery(Box::new(inner)), "v")
                .group_by(vec![Expr::column("seg")])
                .order_by(Expr::column("seg"), None)
                .build()
        };

        // Narrowing: two closed ranges as a VALUES list. Postgres types an
        // uncast VALUES parameter as text, so each bound value is cast to the
        // dialect's integer type.
        let narrowing = |cast: &str| {
            let bound = |i: i64| Expr::Cast {
                expr: Box::new(int(i)),
                data_type: cast.to_string(),
            };
            let on = op(
                op(col("t", "actor_id"), BinaryOperator::GtEq, col("r", "lo")),
                BinaryOperator::And,
                op(col("t", "actor_id"), BinaryOperator::LtEq, col("r", "hi")),
            );
            SelectBuilder::new()
                .select(vec![
                    col("r", "seg"),
                    Expr::FunctionCall(FunctionCall::count_all()).alias("n"),
                ])
                .from_source(
                    TableSource::Values {
                        columns: vec!["seg".into(), "lo".into(), "hi".into()],
                        rows: vec![
                            vec![bound(7), bound(1), bound(10)],
                            vec![bound(9), bound(100), bound(104)],
                        ],
                    },
                    "r",
                )
                .join(JoinKind::Inner, actor(), Some("t"), on)
                .group_by(vec![col("r", "seg")])
                .order_by(col("r", "seg"), None)
                .build()
        };

        fn render(select: &Select, dialect: &dyn QueryDialect) -> (String, Vec<Value>) {
            let mut r = Renderer::new(dialect);
            select.render(&mut r);
            r.finish()
        }
        fn counts(rows: &[model::records::Record]) -> Vec<(i64, i64)> {
            let int = |v: Option<&Value>| match v {
                Some(Value::Int(i)) => *i,
                Some(Value::UInt(u)) => *u as i64,
                Some(Value::Decimal(d)) => d.to_string().parse().expect("integer"),
                other => panic!("not an integer: {other:?}"),
            };
            rows.iter()
                .map(|r| (int(r.value("seg")), int(r.value("n"))))
                .collect()
        }

        let mysql_plan = plan();
        let mysql = DriverRef::open_session(mysql_plan.get_connection("src").unwrap(), &settings())
            .await
            .expect("mysql session");
        let pg_doc = parse(&Direction::POSTGRES_TO_POSTGRES.ppl("")).expect("parse");
        let pg_plan = ExecutionPlan::build(&pg_doc, Arc::new(EnvContext::empty())).expect("plan");
        let pg = DriverRef::open_session(pg_plan.get_connection("src").unwrap(), &settings())
            .await
            .expect("postgres session");

        let sides: [(&DriverRef, &dyn QueryDialect, &str); 2] = [
            (&mysql, &dialect::MySql, "SIGNED"),
            (&pg, &dialect::Postgres, "BIGINT"),
        ];
        for (driver, dialect, cast) in sides {
            let (sql, params) = render(&level0(), dialect);
            let rows = dispatch_driver!(driver, |d| d.query_params(&sql, &params).await)
                .unwrap_or_else(|e| panic!("{sql}: {e}"));
            assert_eq!(counts(&rows), [(0, 50), (1, 50), (2, 50), (3, 50)], "{sql}");

            let (sql, params) = render(&narrowing(cast), dialect);
            let rows = dispatch_driver!(driver, |d| d.query_params(&sql, &params).await)
                .unwrap_or_else(|e| panic!("{sql}: {e}"));
            assert_eq!(counts(&rows), [(7, 10), (9, 5)], "{sql}");
        }
    }
}

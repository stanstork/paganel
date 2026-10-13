#[cfg(test)]
mod tests {
    use crate::harness::ppl::feature_ppl;
    use crate::{
        harness::runner::{
            DbType, execute, get_row_count, run_ppl, run_ppl_with_pause, run_ppl_with_pause_mode,
            run_verify_ppl,
        },
        reset_postgres_schema,
    };
    use engine_core::plan::execution::ExecutionPlan;
    use engine_processing::EnvContext;
    use engine_state::StateStore;
    use engine_state::models::{PipelineStatus, RunState, RunStatus};
    use ppl_syntax::builder::parse;
    use std::sync::Arc;
    use tracing_test::traced_test;

    /// MySQL `film` (1000 rows, film_id PK) -> Postgres, only the mapped columns
    /// (avoids ENUM/SET so the created table is simple). Small batches so a pause
    /// reliably lands mid-migration.
    fn ppl(dest: &str) -> String {
        feature_ppl(&format!(
            r#"

            pipeline "copy_film" {{
                from {{ connection = connection.src table = "film" }}
                to   {{ connection = connection.dst table = "{dest}" }}

                select {{
                    film_id = film.film_id
                    title   = film.title
                }}

                settings {{
                    create_missing_tables = true
                    batch_size            = 20
                }}
            }}
            "#,
            dest = dest,
        ))
    }

    async fn distinct_film_ids(table: &str) -> i64 {
        let pg = crate::pg_pool().await;
        pg.query_one(&format!("SELECT COUNT(DISTINCT film_id) FROM {table}"), &[])
            .await
            .unwrap()
            .get(0)
    }

    /// Pause mid-migration, then resume: every source row lands exactly once
    /// (no gaps, no duplicates).
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn db_migration_resumes_after_pause() {
        reset_postgres_schema().await;
        let ppl = ppl("film_resume");
        let total = get_row_count("film", "sakila", DbType::MySql).await;

        // Run 1: stop gracefully after partial progress (do NOT reset afterward).
        run_ppl_with_pause(&ppl, "film_resume", 100).await;
        let partial = get_row_count("film_resume", "testdb", DbType::Postgres).await;
        assert!(
            partial > 0 && partial < total,
            "expected partial progress after pause, got {partial} of {total}"
        );

        // Run 2: resume from the checkpoint (same plan, state intact).
        run_ppl(&ppl, false).await.expect("resume run failed");

        let final_count = get_row_count("film_resume", "testdb", DbType::Postgres).await;
        assert_eq!(final_count, total, "all rows present after resume");
        assert_eq!(
            distinct_film_ids("film_resume").await,
            total,
            "no duplicate or missing rows after resume"
        );
    }

    /// Resuming/re-running an already-completed migration is a no-op: the row
    /// count is unchanged and no duplicates are introduced.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn completed_db_migration_rerun_is_noop() {
        reset_postgres_schema().await;
        let ppl = ppl("film_done");
        let total = get_row_count("film", "sakila", DbType::MySql).await;

        run_ppl(&ppl, false).await.expect("first run failed");
        assert_eq!(
            get_row_count("film_done", "testdb", DbType::Postgres).await,
            total
        );

        // Re-run the identical plan without clearing state.
        run_ppl(&ppl, false).await.expect("rerun failed");

        assert_eq!(
            get_row_count("film_done", "testdb", DbType::Postgres).await,
            total,
            "re-running a completed migration must not change the row count"
        );
        assert_eq!(distinct_film_ids("film_done").await, total, "no duplicates");
    }

    /// Pausing and resuming twice still converges to the full, duplicate-free
    /// result (multiple checkpoints exercised).
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn db_migration_survives_repeated_pauses() {
        reset_postgres_schema().await;
        let ppl = ppl("film_multi");
        let total = get_row_count("film", "sakila", DbType::MySql).await;

        run_ppl_with_pause(&ppl, "film_multi", 100).await;
        run_ppl_with_pause(&ppl, "film_multi", 400).await;
        run_ppl(&ppl, false).await.expect("final resume failed");

        assert_eq!(
            get_row_count("film_multi", "testdb", DbType::Postgres).await,
            total
        );
        assert_eq!(
            distinct_film_ids("film_multi").await,
            total,
            "no duplicates"
        );
    }

    /// An interrupted integrity run still produces a receipt for the *whole*
    /// table once resumed.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn integrity_receipt_survives_pause_and_resume() {
        reset_postgres_schema().await;
        let ppl = ppl("film_integrity_resume");
        let total = get_row_count("film", "sakila", DbType::MySql).await;

        // Run 1: hash part of the table, then stop gracefully.
        run_ppl_with_pause_mode(&ppl, "film_integrity_resume", 100, true).await;
        let partial = get_row_count("film_integrity_resume", "testdb", DbType::Postgres).await;
        assert!(
            partial > 0 && partial < total,
            "expected partial progress after pause, got {partial} of {total}"
        );

        // Run 2: resume with integrity still on, then verify.
        run_ppl(&ppl, true).await.expect("resume run failed");
        assert_eq!(
            get_row_count("film_integrity_resume", "testdb", DbType::Postgres).await,
            total
        );

        run_verify_ppl(&ppl)
            .await
            .expect("verify should match after a resumed integrity run");
    }

    /// A run where one pipeline fails must be recorded as `failed`, not `completed`.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn run_with_a_failed_pipeline() {
        reset_postgres_schema().await;

        // Pre-fill the destination so the `actor` pipeline collides on its
        // primary key, while the `category` pipeline in the same run succeeds.
        let seed = feature_ppl(
            r#"
            pipeline "seed_actor" {
                from { connection = connection.src  table = "actor" }
                to   { connection = connection.dst  table = "actor" }
                settings { create_missing_tables = true  batch_size = 100 }
            }
            "#,
        );
        run_ppl(&seed, false).await.expect("seed apply failed");

        let ppl = feature_ppl(
            r#"
            pipeline "copy_category" {
                from { connection = connection.src  table = "category" }
                to   { connection = connection.dst  table = "category" }
                settings { create_missing_tables = true  batch_size = 100 }
            }

            pipeline "copy_actor" {
                from { connection = connection.src  table = "actor" }
                to   { connection = connection.dst  table = "actor" }
                settings { create_missing_tables = true  batch_size = 100 }
            }
            "#,
        );

        run_ppl(&ppl, false)
            .await
            .expect_err("the colliding pipeline must fail the run");

        let run = load_run_state(&ppl).await.expect("run state was saved");

        assert!(
            matches!(run.status, RunStatus::Failed { .. }),
            "expected the run to be recorded as failed, got {:?}",
            run.status
        );
        assert_eq!(
            status_of(&run, "copy_category"),
            Some(PipelineStatus::Completed)
        );
        assert!(matches!(
            status_of(&run, "copy_actor"),
            Some(PipelineStatus::Failed { .. })
        ));

        // Clear the collision; re-running must retry the failed pipeline instead
        // of reporting the whole migration already done.
        execute("TRUNCATE TABLE actor").await;
        run_ppl(&ppl, false).await.expect("re-run should succeed");

        let run = load_run_state(&ppl).await.expect("run state was saved");

        assert!(
            matches!(run.status, RunStatus::Completed { .. }),
            "expected the re-run to complete, got {:?}",
            run.status
        );
        assert_eq!(
            get_row_count("actor", "sakila", DbType::Postgres).await,
            200,
            "the retried pipeline should have migrated its rows"
        );
    }

    fn status_of(run: &RunState, name: &str) -> Option<PipelineStatus> {
        run.pipelines
            .iter()
            .find(|p| p.name == name)
            .map(|p| p.status.clone())
    }

    /// Read back what the engine persisted for this config's run.
    async fn load_run_state(ppl: &str) -> Option<RunState> {
        let doc = parse(ppl).expect("parse");
        let env = Arc::new(EnvContext::empty());
        let plan = ExecutionPlan::build(&doc, env).expect("build plan");

        let dir = dirs::home_dir().expect("home dir").join(".paganel/state");
        let store = StateStore::open(dir).expect("open state store");

        store
            .load_run_state(&plan.run_id())
            .await
            .expect("load run state")
    }
}

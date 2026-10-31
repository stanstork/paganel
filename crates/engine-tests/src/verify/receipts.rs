#[cfg(test)]
mod tests {
    use crate::{
        harness::runner::{execute, run_ppl, run_verify_ppl},
        reset_postgres_schema,
    };
    use engine_core::{context::exec::ExecutionContext, plan::execution::ExecutionPlan};
    use engine_processing::EnvContext;
    use engine_state::{RowHashScope, fold_root};
    use engine_verify::{
        HashSink, ImportRequest, NoopProgress, VerifyState, error::VerifyError, hash_table,
        import_table, spec_from_table, verify,
    };
    use model::integrity::result::VerificationResult;
    use ppl_syntax::builder::parse;
    use std::sync::Arc;
    use tracing_test::traced_test;

    /// Verify returns NoPriorRun (no error) when no receipt exists yet.
    // Config: crates/engine-tests/configs/verify/actor.ppl
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_no_receipt_is_not_error() {
        reset_postgres_schema().await;
        let ppl = verify_config!("actor.ppl");
        // Migrate without --integrity so no receipt is written.
        run_ppl(&ppl, false).await.expect("apply failed");
        run_verify_ppl(&ppl)
            .await
            .expect("verify should not error on missing receipt");
    }

    /// Verify holds the run's lock, so it cannot run beside an apply, resume
    /// or another verify of the same config (they share the run's row-hash
    /// files). Once the holder lets go, the same verify goes through.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_refuses_while_the_run_is_held() {
        reset_postgres_schema().await;
        let ppl = verify_config!("actor.ppl");
        run_ppl(&ppl, true).await.expect("apply failed");

        let doc = parse(&ppl).expect("parse");
        let env = Arc::new(EnvContext::empty());
        let plan = ExecutionPlan::build(&doc, env.clone()).expect("build plan");
        let state_dir = engine_state::default_state_dir().expect("state dir");

        let held = engine_state::RunLock::acquire(&state_dir, &plan.run_id()).expect("lock");
        let err = verify(plan.clone(), env.clone())
            .await
            .expect_err("verify must not run while the run is held");
        assert!(
            matches!(
                err,
                engine_verify::error::VerifyError::Store(
                    engine_state::error::StateStoreError::RunLocked { .. }
                )
            ),
            "{err}"
        );

        drop(held);
        let results = verify(plan, env).await.expect("verify after release");
        assert!(
            results
                .iter()
                .all(|r| matches!(r, VerificationResult::Match { .. })),
            "{results:?}"
        );
    }

    /// Hashing a destination table with `spec_from_table` + `hash_table`
    /// and folding it with `fold_root` gives exactly the receipt
    /// `apply --integrity` wrote for it.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn hashing_a_table_reproduces_the_apply_receipt() {
        reset_postgres_schema().await;
        let ppl = verify_config!("actor.ppl");
        run_ppl(&ppl, true).await.expect("apply failed");

        let doc = parse(&ppl).expect("parse");
        let env = Arc::new(EnvContext::empty());
        let plan = ExecutionPlan::build(&doc, env.clone()).expect("build plan");
        let pipeline = &plan.pipelines[0];
        let table = pipeline.destination.table.clone();

        let state = VerifyState::open_default().expect("state");
        let receipt = state
            .store
            .load_receipt(&pipeline.name, &table)
            .await
            .expect("load receipt")
            .expect("apply --integrity writes a receipt");

        let ctx = ExecutionContext::new(&plan, state.store.clone(), state.hash_log.clone(), env);
        let driver = ctx
            .resolve_driver(&pipeline.destination.connection)
            .await
            .expect("driver");

        let (spec, meta) = spec_from_table(&driver, &table, None, &[])
            .await
            .expect("spec");
        assert_eq!(spec.column_order, receipt.column_order);
        assert_eq!(spec.key_columns, receipt.key_columns);

        // A log name of its own, so the baseline set is left untouched.
        let log_name = "@test/reproduce";
        let sink = HashSink {
            log: &state.hash_log,
            scope: RowHashScope::Baseline,
            pipeline: log_name,
            table: &table,
        };
        state
            .hash_log
            .clear(RowHashScope::Baseline, log_name, &table)
            .expect("clear");
        let rows = hash_table(&driver, meta, &spec, &sink, None, None)
            .await
            .expect("hash table");

        state
            .hash_log
            .seal(RowHashScope::Baseline, log_name, &table)
            .expect("seal");
        let stream = state
            .hash_log
            .stream(RowHashScope::Baseline, log_name, &table)
            .expect("stream");
        let (leaves, root) = fold_root(stream, spec.algorithm).expect("fold");

        assert_eq!(rows, receipt.total_rows);
        assert_eq!(leaves, receipt.total_rows);
        assert_eq!(root, receipt.table_root, "same rows, same spec, same root");

        state.hash_log.clear_pipeline(log_name).expect("cleanup");
    }

    /// `state import` hashes a table the way apply hashes its destination: an
    /// import of a migrated table reproduces apply's receipt, under the
    /// import's own pipeline name, and `delete_import` takes the receipt away.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn importing_a_migrated_table_reproduces_its_receipt() {
        reset_postgres_schema().await;
        let ppl = verify_config!("actor.ppl");
        run_ppl(&ppl, true).await.expect("apply failed");

        let doc = parse(&ppl).expect("parse");
        let env = Arc::new(EnvContext::empty());
        let plan = ExecutionPlan::build(&doc, env.clone()).expect("build plan");
        let pipeline = &plan.pipelines[0];
        let table = pipeline.destination.table.clone();

        let state = VerifyState::open_default().expect("state");
        let applied = state
            .store
            .load_receipt(&pipeline.name, &table)
            .await
            .expect("load receipt")
            .expect("apply --integrity writes a receipt");

        let ctx = ExecutionContext::new(&plan, state.store.clone(), state.hash_log.clone(), env);
        let driver = ctx
            .resolve_driver(&pipeline.destination.connection)
            .await
            .expect("driver");

        let req = ImportRequest {
            name: "actor_dst".into(),
            connection: pipeline.destination.connection.name.clone(),
            target: None,
            table: table.clone(),
            key_override: None,
        };
        let (receipt, record) = import_table(&driver, &req, &state, &mut NoopProgress)
            .await
            .expect("import");

        assert_eq!(receipt.pipeline_name, "@import/actor_dst");
        assert_eq!(receipt.table_root, applied.table_root);
        assert_eq!(receipt.total_rows, applied.total_rows);
        assert_eq!(receipt.column_order, applied.column_order);
        assert_eq!(record.rows, applied.total_rows);
        assert_eq!(record.connection, pipeline.destination.connection.name);

        let stored = state.store.load_import("actor_dst").await.expect("load");
        assert_eq!(stored, Some(record));
        assert!(
            state
                .store
                .load_receipt("@import/actor_dst", &table)
                .await
                .expect("load receipt")
                .is_some()
        );

        assert!(
            state
                .store
                .delete_import("actor_dst")
                .await
                .expect("delete")
        );
        state
            .hash_log
            .clear_pipeline("@import/actor_dst")
            .expect("cleanup");
        assert!(
            state
                .store
                .load_receipt("@import/actor_dst", &table)
                .await
                .expect("load receipt")
                .is_none()
        );
    }

    /// A table without a primary key needs `--key`, and invalid names are
    /// refused before anything is read.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn import_requires_a_key_and_a_valid_name() {
        reset_postgres_schema().await;
        let ppl = verify_config!("actor.ppl");
        run_ppl(&ppl, true).await.expect("apply failed");
        execute(
            "CREATE TABLE no_key (code TEXT, label TEXT); \
             INSERT INTO no_key VALUES ('a', 'one'), ('b', 'two');",
        )
        .await;

        let doc = parse(&ppl).expect("parse");
        let env = Arc::new(EnvContext::empty());
        let plan = ExecutionPlan::build(&doc, env.clone()).expect("build plan");
        let connection = &plan.pipelines[0].destination.connection;

        let state = VerifyState::open_default().expect("state");
        let ctx = ExecutionContext::new(&plan, state.store.clone(), state.hash_log.clone(), env);
        let driver = ctx.resolve_driver(connection).await.expect("driver");

        let request = |name: &str, key: Option<Vec<String>>| ImportRequest {
            name: name.into(),
            connection: connection.name.clone(),
            target: None,
            table: "no_key".into(),
            key_override: key,
        };

        let err = import_table(&driver, &request("no_key", None), &state, &mut NoopProgress)
            .await
            .expect_err("no primary key and no --key");
        assert!(
            matches!(err, VerifyError::NoKey(ref t) if t == "no_key"),
            "{err}"
        );

        let err = import_table(
            &driver,
            &request("sales.no_key", None),
            &state,
            &mut NoopProgress,
        )
        .await
        .expect_err("invalid name");
        assert!(err.to_string().contains("invalid import name"), "{err}");

        let (receipt, _) = import_table(
            &driver,
            &request("no_key", Some(vec!["code".into()])),
            &state,
            &mut NoopProgress,
        )
        .await
        .expect("import with --key");
        assert_eq!(receipt.key_columns, ["code"]);
        assert_eq!(receipt.total_rows, 2);

        state.store.delete_import("no_key").await.expect("delete");
        state
            .hash_log
            .clear_pipeline("@import/no_key")
            .expect("cleanup");
    }
}

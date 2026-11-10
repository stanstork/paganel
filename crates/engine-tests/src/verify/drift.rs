#[cfg(test)]
mod tests {
    use crate::{
        harness::runner::{execute, run_ppl},
        reset_postgres_schema,
    };
    use engine_core::{context::exec::ExecutionContext, plan::execution::ExecutionPlan};
    use engine_processing::EnvContext;
    use engine_state::{RunLock, StateStore};
    use engine_verify::{
        ImportRequest, NoopProgress, VerifyOptions, VerifyState, discover_targets, import_table,
        run_drift,
    };
    use model::integrity::report::ReportResult;
    use ppl_syntax::builder::parse;
    use std::sync::Arc;
    use tokio_util::sync::CancellationToken;
    use tracing_test::traced_test;

    /// Drift checks the pipeline's receipt and an import of the same table:
    /// one hand edit shows up in both, naming the row. An import whose
    /// connection the config lacks is skipped, and while an apply holds the
    /// config's lock, drift refuses to run at all. Importing a table that
    /// does not exist names it. Another store writing to the same file does
    /// not block the run.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn drift_reports_a_hand_edit_in_pipelines_and_imports() {
        reset_postgres_schema().await;
        let ppl = verify_config!("actor.ppl");
        run_ppl(&ppl, true).await.expect("apply failed");

        let doc = parse(&ppl).expect("parse");
        let env = Arc::new(EnvContext::empty());
        let plan = ExecutionPlan::build(&doc, env.clone()).expect("build plan");
        let pipeline = &plan.pipelines[0];
        let table = pipeline.destination.table.clone();

        let state = VerifyState::open_default().expect("state");
        let ctx = ExecutionContext::new(
            &plan,
            state.store.clone(),
            state.hash_log.clone(),
            env.clone(),
        );

        let driver = ctx
            .resolve_driver(&pipeline.destination.connection)
            .await
            .expect("driver");

        let mut req = ImportRequest {
            name: "actor_dst".into(),
            connection: pipeline.destination.connection.name.clone(),
            target: None,
            table: table.clone(),
            key_override: None,
        };
        import_table(&driver, &req, &state, &mut NoopProgress)
            .await
            .expect("import");

        // A missing table is named.
        let missing = ImportRequest {
            name: "missing".into(),
            table: "no_such_table".into(),
            ..req.clone()
        };
        let err = import_table(&driver, &missing, &state, &mut NoopProgress)
            .await
            .expect_err("no table to import");
        assert_eq!(err.to_string(), "table 'no_such_table' not found");

        // An import made against a config this one does not share.
        req.name = "elsewhere".into();
        let (receipt, mut record) = import_table(&driver, &req, &state, &mut NoopProgress)
            .await
            .expect("import");
        record.connection = "warehouse".into();
        state
            .store
            .save_import(&record, &receipt)
            .await
            .expect("save");

        execute("UPDATE actor SET first_name = 'TAMPERED' WHERE actor_id = 1").await;

        // A second store on the same file keeps writing while drift runs.
        let writer_store = StateStore::open(state.store.dir()).expect("second store");
        let stop = CancellationToken::new();
        let writer = tokio::spawn({
            let stop = stop.clone();
            let mut receipt = receipt.clone();
            receipt.pipeline_name = "concurrent_writer".into();
            async move {
                let mut writes = 0u32;
                while !stop.is_cancelled() {
                    writer_store
                        .save_receipt(&receipt)
                        .await
                        .expect("concurrent write");
                    writes += 1;
                }
                writes
            }
        });

        let (targets, skipped) = discover_targets(&plan, &state, &[]).await.expect("targets");
        let run = run_drift(
            &plan,
            env.clone(),
            &state,
            &targets,
            &mut NoopProgress,
            VerifyOptions::default(),
        )
        .await
        .expect("drift");

        stop.cancel();
        assert!(writer.await.expect("writer") > 0);

        assert_eq!(run.exit_code(), 3);
        let names: Vec<&str> = run.tables.iter().map(|t| t.pipeline.as_str()).collect();
        assert_eq!(names, [pipeline.name.as_str(), "@import/actor_dst"]);
        for t in &run.tables {
            assert_eq!(t.result, ReportResult::Mismatch, "{}", t.pipeline);
            assert_eq!(t.changed, 1);
            assert!(t.divergences.iter().any(|d| d.key == "actor_id=1"));
        }
        assert_eq!(skipped.len(), 1);
        assert_eq!(skipped[0].name, "elsewhere");
        assert!(skipped[0].reason.contains("'warehouse'"));

        // --filters narrows, and an unknown name is an error.
        let (filters, _) = discover_targets(&plan, &state, &["actor_dst".into()])
            .await
            .expect("targets");
        assert_eq!(filters.len(), 1);
        assert!(
            discover_targets(&plan, &state, &["nope".into()])
                .await
                .is_err()
        );

        // While an apply of this config runs, drift checks nothing.
        let lock = RunLock::acquire(state.store.dir(), &plan.run_id()).expect("lock");
        let err = run_drift(
            &plan,
            env,
            &state,
            &targets,
            &mut NoopProgress,
            VerifyOptions::default(),
        )
        .await
        .expect_err("the config is being applied");
        drop(lock);
        assert!(err.to_string().contains("already running"), "{err}");

        for name in ["actor_dst", "elsewhere"] {
            state.store.delete_import(name).await.expect("delete");
            state
                .hash_log
                .clear_pipeline(&format!("@import/{name}"))
                .expect("cleanup");
        }
    }
}

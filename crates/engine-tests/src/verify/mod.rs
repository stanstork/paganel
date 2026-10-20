#[cfg(test)]
mod tests {
    use crate::{
        harness::{
            Direction,
            runner::{DbType, execute, get_row_count, run_ppl, run_verify_ppl},
        },
        reset_postgres_schema,
    };
    use engine_core::{context::exec::ExecutionContext, plan::execution::ExecutionPlan};
    use engine_processing::EnvContext;
    use engine_state::{RowHashScope, fold_root};
    use engine_verify::{
        HashSink, ImportRequest, NoopProgress, VerifyOptions, VerifyState, error::VerifyError,
        hash_table, import_table, spec_from_table, verify, verify_with_progress,
    };
    use model::integrity::result::{DivergenceKind, VerificationResult};
    use ppl_syntax::builder::parse;
    use std::sync::Arc;
    use tracing_test::traced_test;

    /// Render a verify config for the MySQL -> PostgreSQL direction.
    /// The file holds pipelines only; the harness supplies the connections.
    macro_rules! verify_config {
        ($name:expr) => {
            $crate::harness::Direction::MYSQL_TO_POSTGRES.config(concat!("verify/", $name))
        };
    }

    /// Verify must hold in the reverse cross-engine direction too - PostgreSQL
    /// source -> MySQL destination - not only MySQL -> PostgreSQL. A single
    /// `actor` table, integrity apply, then verify.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_cross_engine_pg_to_mysql() {
        use crate::harness::Direction;

        let dir = Direction::POSTGRES_TO_MYSQL;
        let body = r#"
            pipeline "actor" {
                from { connection = connection.src  table = "actor" }
                to   { connection = connection.dst  table = "actor" }
                settings { create_missing_tables = true  batch_size = 100 }
            }
        "#;
        dir.reset().await;
        let ppl = dir.ppl(body);
        run_ppl(&ppl, true).await.expect("apply failed");
        run_verify_ppl(&ppl)
            .await
            .expect("verify should match for PG -> MySQL");
    }

    /// Simplest case: actor table, small row count, straightforward types.
    // Config: crates/engine-tests/configs/verify/actor.ppl
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_actor_matches() {
        reset_postgres_schema().await;
        let ppl = verify_config!("actor.ppl");
        run_ppl(&ppl, true).await.expect("apply failed");
        run_verify_ppl(&ppl).await.expect("verify failed");
    }

    /// Larger table with DECIMAL, TIMESTAMP, nullable INT columns.
    // Config: crates/engine-tests/configs/verify/payment.ppl
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_payment_matches() {
        reset_postgres_schema().await;
        let ppl = verify_config!("payment.ppl");
        run_ppl(&ppl, true).await.expect("apply failed");
        run_verify_ppl(&ppl).await.expect("verify failed");
    }

    /// Type-rich table: ENUM, SET, YEAR, DECIMAL, TEXT, nullable SMALLINT.
    // Config: crates/engine-tests/configs/verify/language.ppl (prereq, no integrity)
    //         examples/configs/verify/film.ppl      (with integrity)
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_film_matches() {
        reset_postgres_schema().await;
        // Language must exist first to satisfy film's FK constraint.
        // No integrity receipt is needed for this prerequisite step.
        run_ppl(&verify_config!("language.ppl"), false)
            .await
            .expect("language apply failed");
        let film_ppl = verify_config!("film.ppl");
        run_ppl(&film_ppl, true).await.expect("film apply failed");
        run_verify_ppl(&film_ppl).await.expect("film verify failed");
    }

    /// Verify works when batch_size divides row count evenly (no partial last batch).
    // Config: crates/engine-tests/configs/verify/actor_exact_batch.ppl  (batch_size=200, actor=200 rows)
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_with_exact_batch_boundary() {
        reset_postgres_schema().await;
        let ppl = verify_config!("actor_exact_batch.ppl");
        run_ppl(&ppl, true).await.expect("apply failed");
        run_verify_ppl(&ppl).await.expect("verify failed");
    }

    /// Verify works when the last batch is partial (total rows % batch_size != 0).
    // Config: crates/engine-tests/configs/verify/actor_partial_batch.ppl  (batch_size=77, actor=200 rows)
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_with_partial_last_batch() {
        reset_postgres_schema().await;
        let ppl = verify_config!("actor_partial_batch.ppl");
        run_ppl(&ppl, true).await.expect("apply failed");
        run_verify_ppl(&ppl).await.expect("verify failed");
    }

    /// Cascade migration: payment + FK depth-1 (customer, staff, rental).
    // Config: crates/engine-tests/configs/verify/payment_cascade.ppl
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_cascade_payment_matches() {
        reset_postgres_schema().await;
        let ppl = verify_config!("payment_cascade.ppl");
        run_ppl(&ppl, true).await.expect("cascade apply failed");
        run_verify_ppl(&ppl).await.expect("cascade verify failed");
    }

    /// Verify detects when a row has been modified in the destination after apply.
    // Config: crates/engine-tests/configs/verify/actor.ppl
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_detects_modified_row() {
        use crate::harness::runner::execute;

        reset_postgres_schema().await;
        let ppl = verify_config!("actor.ppl");
        run_ppl(&ppl, true).await.expect("apply failed");

        execute("UPDATE actor SET first_name = 'TAMPERED' WHERE actor_id = 1").await;

        let result = run_verify_ppl(&ppl).await;
        assert!(result.is_err(), "verify should have detected the mismatch");
    }

    /// Verify detects when a row has been deleted from the destination after apply.
    // Config: crates/engine-tests/configs/verify/actor.ppl
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_detects_deleted_row() {
        use crate::harness::runner::execute;

        reset_postgres_schema().await;
        let ppl = verify_config!("actor.ppl");
        run_ppl(&ppl, true).await.expect("apply failed");

        execute("DELETE FROM actor WHERE actor_id = 1").await;

        let result = run_verify_ppl(&ppl).await;
        assert!(result.is_err(), "verify should have detected the mismatch");
    }

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
        let rows = hash_table(&driver, meta, &spec, &sink, None)
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

    /// WHERE filter: only rows passing the filter are migrated.
    ///
    /// The receipt is built from filtered rows only. The destination contains
    /// only those rows. Verify reads the destination as-is (no re-applying of
    /// the filter) and compares against the receipt - hashes should match.
    ///
    /// Key insight: verify checks "destination == receipt", NOT "destination == source".
    /// A filtered migration verifies correctly even though it's a subset of the source.
    // Config: crates/engine-tests/configs/verify/payment_staff_filter.ppl
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_with_where_filter_matches() {
        reset_postgres_schema().await;
        let ppl = verify_config!("payment_staff_filter.ppl");

        run_ppl(&ppl, true).await.expect("apply failed");

        // Destination has only staff_id=1 rows - fewer than the full payment table.
        let dest_count = get_row_count("payment", "sakila", DbType::Postgres).await;
        let total_count = get_row_count("payment", "sakila", DbType::MySql).await;
        assert!(
            dest_count < total_count,
            "filter should have excluded some rows (got {dest_count} of {total_count})"
        );

        run_verify_ppl(&ppl).await.expect("verify failed");
    }

    /// SKIP validation: rows failing the check are excluded from the destination.
    ///
    /// `action = skip` silently drops rows that fail the check - they never reach
    /// the destination and are not included in the receipt. Verify sees a smaller
    /// destination but the receipt matches exactly what was written.
    // Config: crates/engine-tests/configs/verify/payment_skip_validation.ppl
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_with_skip_validation_matches() {
        reset_postgres_schema().await;
        let ppl = verify_config!("payment_skip_validation.ppl");

        run_ppl(&ppl, true).await.expect("apply failed");

        // Destination has only amount <= 5.00 rows.
        let dest_count = get_row_count("payment", "sakila", DbType::Postgres).await;
        let total_count = get_row_count("payment", "sakila", DbType::MySql).await;
        assert!(
            dest_count < total_count,
            "skip validation should have excluded some rows (got {dest_count} of {total_count})"
        );

        run_verify_ppl(&ppl).await.expect("verify failed");
    }

    /// WARN validation: rows failing the check are still migrated (warn != skip).
    ///
    /// All rows reach the destination. The receipt covers the full table.
    /// Verify should pass with the complete row count.
    // Config: crates/engine-tests/configs/verify/actor_warn_validation.ppl
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_with_warn_validation_all_rows_present() {
        reset_postgres_schema().await;
        let ppl = verify_config!("actor_warn_validation.ppl");

        run_ppl(&ppl, true).await.expect("apply failed");

        // Warn does NOT skip rows - destination should have the full actor count.
        let dest_count = get_row_count("actor", "sakila", DbType::Postgres).await;
        let source_count = get_row_count("actor", "sakila", DbType::MySql).await;
        assert_eq!(
            dest_count, source_count,
            "warn validation must not skip rows (dest={dest_count}, src={source_count})"
        );

        run_verify_ppl(&ppl).await.expect("verify failed");
    }

    /// WHERE filter + SKIP validation combined.
    ///
    /// The filter narrows the source rows first (pushes SQL WHERE to source query),
    /// then the skip validation further excludes rows that pass the filter but fail
    /// the check. Verify only sees the doubly-reduced destination and compares it
    /// against the receipt, which was built from the same doubly-reduced set.
    // Config: crates/engine-tests/configs/verify/payment_filter_and_skip.ppl
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_filter_and_skip_combined() {
        reset_postgres_schema().await;
        let ppl = verify_config!("payment_filter_and_skip.ppl");

        run_ppl(&ppl, true).await.expect("apply failed");

        // Both conditions reduce the destination below the full payment count.
        let dest_count = get_row_count("payment", "sakila", DbType::Postgres).await;
        let total_count = get_row_count("payment", "sakila", DbType::MySql).await;
        assert!(
            dest_count < total_count,
            "filter + skip should have excluded rows (got {dest_count} of {total_count})"
        );

        run_verify_ppl(&ppl).await.expect("verify failed");
    }

    /// JOIN + WHERE filter + TIMESTAMP pagination, then verify.
    ///
    /// Migrates rental rows for staff_id=1 only (≈8,000 of 16,044), joined with
    /// customer to produce a computed customer_name column, paginated with a
    /// timestamp cursor on rental_date + tiebreaker rental_id (batch=200, ≈40 pages).
    ///
    /// Verify must handle:
    ///   - computed columns in the receipt hashes
    ///   - a filtered source (receipt covers only the staff_id=1 rows)
    ///   - non-PK cursor: verify replays identical timestamp batch boundaries
    // Config: crates/engine-tests/configs/verify/rental_join_filter_ts.ppl
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_join_filter_timestamp_pagination() {
        reset_postgres_schema().await;
        let ppl = verify_config!("rental_join_filter_ts.ppl");
        run_ppl(&ppl, true).await.expect("apply failed");
        run_verify_ppl(&ppl).await.expect("verify failed");
    }

    /// CASCADE migration of payment with NUMERIC pagination, then verify.
    ///
    /// Root table is payment, cascade depth=1 pulls customer/staff/rental
    /// alongside each payment batch. The root is paginated by the composite
    /// numeric cursor (payment.staff_id + payment.payment_id, batch=500).
    ///
    /// staff_id has only 2 distinct values so every page boundary lands
    /// mid-run, exercising the composite cursor on every page.
    ///
    /// Verify must handle:
    ///   - cascade receipts (rows arrive in FK order, verify reads in PK order)
    ///   - non-PK cursor on the root table
    ///   - variable cascade row counts per batch (customer/staff/rental counts
    ///     differ between the staff_id=1 and staff_id=2 halves)
    // Config: crates/engine-tests/configs/verify/payment_cascade_numeric.ppl
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_cascade_with_numeric_pagination() {
        reset_postgres_schema().await;
        let ppl = verify_config!("payment_cascade_numeric.ppl");
        run_ppl(&ppl, true).await.expect("apply failed");
        run_verify_ppl(&ppl).await.expect("verify failed");
    }

    /// Tamper detection still works on a filtered result set.
    ///
    /// Even though only a subset was migrated, modifying any row in that subset
    /// changes its hash and causes verify to detect the mismatch.
    // Config: crates/engine-tests/configs/verify/payment_staff_filter.ppl
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_filtered_detects_tampering() {
        use crate::harness::runner::execute;

        reset_postgres_schema().await;
        let ppl = verify_config!("payment_staff_filter.ppl");

        run_ppl(&ppl, true).await.expect("apply failed");

        execute(
            "UPDATE payment SET amount = amount + 100.00 WHERE payment_id = \
             (SELECT MIN(payment_id) FROM payment)",
        )
        .await;

        let result = run_verify_ppl(&ppl).await;
        assert!(
            result.is_err(),
            "verify should detect tampering in filtered result set"
        );
    }

    /// Verify detects when a row has been inserted into the destination after apply.
    ///
    /// The receipt records N rows across M batches. If the destination gains a new
    /// row after apply, the hash of at least one batch will differ from the receipt.
    // Config: crates/engine-tests/configs/verify/actor.ppl
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_detects_inserted_row() {
        use crate::harness::runner::execute;

        reset_postgres_schema().await;
        let ppl = verify_config!("actor.ppl");
        run_ppl(&ppl, true).await.expect("apply failed");

        execute(
            "INSERT INTO actor (actor_id, first_name, last_name, last_update) \
             VALUES (9999, 'GHOST', 'ROW', NOW())",
        )
        .await;

        let result = run_verify_ppl(&ppl).await;
        assert!(
            result.is_err(),
            "verify should detect the extra inserted row"
        );
    }

    /// Verify detects when a computed column value is modified in the destination.
    ///
    /// customer_name is a concat() of first_name + last_name from the joined
    /// customer table. If someone updates customer_name directly in the
    /// destination, the row hash changes and verify must catch it.
    // Config: crates/engine-tests/configs/verify/rental_join_filter_ts.ppl
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_detects_tamper_in_computed_column() {
        use crate::harness::runner::execute;

        reset_postgres_schema().await;
        let ppl = verify_config!("rental_join_filter_ts.ppl");
        run_ppl(&ppl, true).await.expect("apply failed");

        execute(
            "UPDATE rental SET customer_name = 'TAMPERED NAME' \
             WHERE rental_id = (SELECT MIN(rental_id) FROM rental)",
        )
        .await;

        let result = run_verify_ppl(&ppl).await;
        assert!(
            result.is_err(),
            "verify should detect tampering in a computed column"
        );
    }

    /// Verify detects tampering in a cascade leaf table.
    ///
    /// The cascade migration writes payment, customer, staff, and rental rows.
    /// Each cascade table has its own keyed receipt.
    /// Modifying a row in a leaf table (customer) must change its receipt hash
    /// and cause verify to report a mismatch.
    // Config: crates/engine-tests/configs/verify/payment_cascade.ppl
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_cascade_detects_tamper_in_leaf_table() {
        use crate::harness::runner::execute;

        reset_postgres_schema().await;
        let ppl = verify_config!("payment_cascade.ppl");
        run_ppl(&ppl, true).await.expect("cascade apply failed");

        // Tamper with a customer row - a cascade leaf table
        execute(
            "UPDATE customer SET first_name = 'TAMPERED' \
             WHERE customer_id = (SELECT MIN(customer_id) FROM customer)",
        )
        .await;

        let result = run_verify_ppl(&ppl).await;
        assert!(
            result.is_err(),
            "verify should detect tampering in a cascade leaf table"
        );
    }

    /// Verify works with PK (keyset) pagination - the most common real-world strategy.
    ///
    /// Receipt batch boundaries are keyed by payment_id cursor positions.
    /// Verify replays identical page boundaries by re-reading payment in
    /// the same order.
    // Config: crates/engine-tests/configs/verify/payment_pk.ppl
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_with_pk_pagination() {
        reset_postgres_schema().await;
        let ppl = verify_config!("payment_pk.ppl");
        run_ppl(&ppl, true).await.expect("apply failed");
        run_verify_ppl(&ppl).await.expect("verify failed");
    }

    /// Verify works with NUMERIC pagination on a plain (non-cascade) table.
    ///
    /// Distinct from the cascade+numeric test: single table, no FK-dependent rows.
    /// Confirms the numeric composite cursor (staff_id + payment_id) replays
    /// correctly in verify when there is no cascade receipt overhead.
    // Config: crates/engine-tests/configs/verify/payment_numeric_plain.ppl
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_with_numeric_pagination_no_cascade() {
        reset_postgres_schema().await;
        let ppl = verify_config!("payment_numeric_plain.ppl");
        run_ppl(&ppl, true).await.expect("apply failed");
        run_verify_ppl(&ppl).await.expect("verify failed");
    }

    /// Verify works with DEFAULT (OFFSET/LIMIT) pagination at scale.
    ///
    /// OFFSET-based batches carry no cursor state; the receipt records row counts
    /// per batch index and verify replays pages sequentially by position.
    // Config: crates/engine-tests/configs/verify/payment_default.ppl
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_with_default_pagination_large_table() {
        reset_postgres_schema().await;
        let ppl = verify_config!("payment_default.ppl");
        run_ppl(&ppl, true).await.expect("apply failed");
        run_verify_ppl(&ppl).await.expect("verify failed");
    }

    /// Verify handles an empty result set (0 rows, 0 batches in receipt).
    ///
    /// A WHERE clause that matches no rows produces an empty migration.
    /// Verify must exit its batch loop immediately and return OK rather
    /// than looping indefinitely or reporting a spurious mismatch.
    // Config: crates/engine-tests/configs/verify/actor_empty.ppl
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_empty_table() {
        reset_postgres_schema().await;
        let ppl = verify_config!("actor_empty.ppl");
        run_ppl(&ppl, true).await.expect("apply failed");
        run_verify_ppl(&ppl)
            .await
            .expect("verify of empty result should pass");
    }

    /// Verify handles a single-row migration where batch_size = 1.
    ///
    /// The first batch is simultaneously the last - exercises the boundary
    /// where start = end = only batch. The receipt has exactly one entry.
    // Config: crates/engine-tests/configs/verify/actor_single_row.ppl
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_single_row() {
        reset_postgres_schema().await;
        let ppl = verify_config!("actor_single_row.ppl");
        run_ppl(&ppl, true).await.expect("apply failed");

        let dest_count = get_row_count("actor", "sakila", DbType::Postgres).await;
        assert_eq!(dest_count, 1, "expected exactly 1 row in destination");

        run_verify_ppl(&ppl).await.expect("verify failed");
    }

    /// Verify hashes NULL values consistently across source and destination.
    ///
    /// rental.return_date is DATETIME NULL; many rows have NULL there (open
    /// rentals not yet returned). Stable NULL encoding is required for hashes
    /// to match between MySQL and PostgreSQL.
    // Config: crates/engine-tests/configs/verify/rental_plain.ppl
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_many_null_columns() {
        reset_postgres_schema().await;
        let ppl = verify_config!("rental_plain.ppl");
        run_ppl(&ppl, true).await.expect("apply failed");
        run_verify_ppl(&ppl).await.expect("verify failed");
    }

    /// Verify is idempotent - running it twice on unchanged data must both pass.
    ///
    /// Verify is a read-only operation. Re-running it against the same receipt
    /// and the same destination must produce identical results without side effects.
    // Config: crates/engine-tests/configs/verify/actor.ppl
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_idempotent() {
        reset_postgres_schema().await;
        let ppl = verify_config!("actor.ppl");
        run_ppl(&ppl, true).await.expect("apply failed");
        run_verify_ppl(&ppl).await.expect("first verify failed");
        // Run a second time - receipt and data are unchanged; must still pass
        run_verify_ppl(&ppl)
            .await
            .expect("second verify should also pass");
    }

    /// Verify handles a config with multiple pipelines linked by an after dependency.
    ///
    /// Both language (6 rows) and film (1,000 rows) pipelines are migrated with
    /// integrity. run_verify_ppl must verify every pipeline in the config and
    /// return OK only if all receipts match.
    // Config: crates/engine-tests/configs/verify/dag_language_film.ppl
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_dag_pipeline() {
        reset_postgres_schema().await;
        let ppl = verify_config!("dag_language_film.ppl");
        run_ppl(&ppl, true).await.expect("dag apply failed");
        run_verify_ppl(&ppl).await.expect("dag verify failed");
    }

    /// Verify names the tampered row by primary key.
    ///
    /// Row-level detail is no longer a mode: every row is keyed, so a mismatch
    /// always reports which row diverged and how, not just a batch range.
    // Config: crates/engine-tests/configs/verify/actor.ppl
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_reports_the_tampered_row_by_key() {
        reset_postgres_schema().await;
        let ppl = verify_config!("actor.ppl");
        run_ppl(&ppl, true).await.expect("apply failed");

        execute("UPDATE actor SET last_name = 'HACKED' WHERE actor_id = 5").await;

        let results = verify_results(&ppl).await;
        let mismatch = results
            .iter()
            .find_map(|r| match r {
                VerificationResult::Mismatch {
                    summary,
                    divergences,
                    ..
                } => Some((summary, divergences)),
                _ => None,
            })
            .expect("verify should report a mismatch");
        let (summary, divergences) = mismatch;

        assert_eq!(summary.changed, 1, "exactly one row changed");
        assert_eq!(summary.missing, 0);
        assert_eq!(summary.extra, 0);
        assert!(
            divergences.iter().any(|d| d.key.contains("actor_id=5")
                && matches!(d.kind, DivergenceKind::Changed { .. })),
            "divergence should name actor_id=5, got {:?}",
            divergences.iter().map(|d| &d.key).collect::<Vec<_>>()
        );
    }

    /// A deleted row is reported as missing *by key*, and an inserted one as
    /// extra - the two are distinguishable, which positional receipts could not
    /// do (both simply shifted every following row).
    // Config: crates/engine-tests/configs/verify/actor.ppl
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_distinguishes_missing_from_extra_rows() {
        reset_postgres_schema().await;
        let ppl = verify_config!("actor.ppl");
        run_ppl(&ppl, true).await.expect("apply failed");

        execute("DELETE FROM actor WHERE actor_id = 3").await;
        execute(
            "INSERT INTO actor (actor_id, first_name, last_name, last_update) \
             VALUES (9999, 'GHOST', 'ROW', NOW())",
        )
        .await;

        let results = verify_results(&ppl).await;
        let summary = results
            .iter()
            .find_map(|r| match r {
                VerificationResult::Mismatch { summary, .. } => Some(summary),
                _ => None,
            })
            .expect("verify should report a mismatch");

        assert_eq!(summary.missing, 1, "actor_id=3 was deleted");
        assert_eq!(summary.extra, 1, "actor_id=9999 was inserted");
        assert_eq!(summary.changed, 0);
    }

    /// Cascade tables now localize divergence too.
    ///
    /// Their rows arrive in FK-join order, which used to force a single sorted
    /// root with no row-level detail. Keyed by primary key, a tampered leaf row
    /// is named exactly like one in the root table.
    // Config: crates/engine-tests/configs/verify/payment_cascade.ppl
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_cascade_names_the_tampered_leaf_row() {
        reset_postgres_schema().await;
        let ppl = verify_config!("payment_cascade.ppl");
        run_ppl(&ppl, true).await.expect("cascade apply failed");

        execute("UPDATE customer SET first_name = 'TAMPERED' WHERE customer_id = 3").await;

        let results = verify_results(&ppl).await;
        let named = results.iter().any(|r| match r {
            VerificationResult::Mismatch {
                receipt,
                divergences,
                ..
            } => {
                receipt.table_name == "customer"
                    && divergences.iter().any(|d| d.key.contains("customer_id=3"))
            }
            _ => false,
        });

        assert!(named, "cascade mismatch should name customer_id=3");
    }

    /// A multi-lane migration verifies, and still names the tampered row.
    ///
    /// Four range lanes write interleaved and out of key order. Because each row
    /// carries its key, lane layout has no bearing on the root or on row-level
    /// localization - which per-batch receipts lost entirely for parallel runs.
    // Config: crates/engine-tests/configs/verify/payment_lanes.ppl
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_multi_lane_migration() {
        reset_postgres_schema().await;
        let ppl = verify_config!("payment_lanes.ppl");
        run_ppl(&ppl, true).await.expect("lane apply failed");
        run_verify_ppl(&ppl).await.expect("lane verify failed");

        execute(
            "UPDATE payment SET amount = amount + 1.00 \
             WHERE payment_id = (SELECT MAX(payment_id) FROM payment)",
        )
        .await;

        let results = verify_results(&ppl).await;
        let changed_one = results.iter().any(|r| {
            matches!(
                r,
                VerificationResult::Mismatch { summary, divergences, .. }
                    if summary.changed == 1 && summary.missing == 0 && summary.extra == 0
                        && divergences.len() == 1
            )
        });
        assert!(
            changed_one,
            "multi-lane verify should pinpoint the one changed row"
        );
    }

    /// A cascade migration verifies when its tables migrate in parallel lanes.
    ///
    /// Graph pipelines used to be forced back to sequential migration whenever
    /// integrity was on, because the receipt needed one ordered accumulator.
    /// Keyed hashes removed that constraint, so this exercises the combination
    /// that was previously unreachable.
    // Config: crates/engine-tests/configs/verify/payment_cascade_lanes.ppl
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_cascade_with_parallel_table_lanes() {
        reset_postgres_schema().await;
        let ppl = verify_config!("payment_cascade_lanes.ppl");
        run_ppl(&ppl, true)
            .await
            .expect("cascade lane apply failed");
        run_verify_ppl(&ppl)
            .await
            .expect("cascade lane verify failed");
    }

    /// A destination that already holds rows the migration under test did not
    /// write: the first half of `actor` is loaded without integrity, then a
    /// second pipeline appends the rest and commits a receipt for those rows
    /// only. Verify reads the whole table, so the pre-existing half comes back
    /// as `extra` even though nothing is corrupt.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_reports_rows_that_were_already_in_the_destination() {
        let ppl = seed_then_append().await;

        let results = verify_results(&ppl).await;
        let summary = results
            .iter()
            .find_map(|r| match r {
                VerificationResult::Mismatch { summary, .. } => Some(summary),
                _ => None,
            })
            .expect("pre-existing rows should fail a strict verify");

        assert_eq!(summary.extra, 100, "the rows loaded before the migration");
        assert_eq!(summary.missing, 0);
        assert_eq!(summary.changed, 0);
    }

    /// The same table with `--allow-extra`: the 100 uncovered rows are accepted
    /// and counted, and the receipt's own 100 rows still have to reproduce
    /// `table_root` for this to be a `Match` at all.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_allow_extra_accepts_rows_outside_the_receipt() {
        let ppl = seed_then_append().await;

        let results = verify_results_with(&ppl, VerifyOptions { allow_extra: true }).await;

        let ignored = results
            .iter()
            .find_map(|r| match r {
                VerificationResult::Match { extra_ignored, .. } => Some(*extra_ignored),
                _ => None,
            })
            .expect("allow_extra should turn the pre-existing rows into a match");

        assert_eq!(ignored, 100, "reported rather than silently dropped");
        assert!(
            !results
                .iter()
                .any(|r| matches!(r, VerificationResult::Mismatch { .. })),
            "no table should still mismatch"
        );
    }

    /// `--allow-extra` forgives uncovered keys and nothing else: tampering with
    /// a row the receipt does cover still fails, and is still named by key.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_allow_extra_still_catches_a_tampered_covered_row() {
        let ppl = seed_then_append().await;

        // actor_id 150 is in the appended half, so the receipt commits it.
        execute("UPDATE actor SET last_name = 'TAMPERED' WHERE actor_id = 150").await;

        let results = verify_results_with(&ppl, VerifyOptions { allow_extra: true }).await;
        let named = results.iter().any(|r| match r {
            VerificationResult::Mismatch {
                summary,
                divergences,
                ..
            } => {
                summary.changed == 1
                    && summary.missing == 0
                    && divergences.iter().any(|d| d.key.contains("actor_id=150"))
            }
            _ => false,
        });

        assert!(named, "a changed covered row must still fail --allow-extra");
    }

    /// `--allow-extra` does not hide data loss either: deleting a row the
    /// receipt covers is still `missing`.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn verify_allow_extra_still_catches_a_deleted_covered_row() {
        let ppl = seed_then_append().await;

        execute("DELETE FROM actor WHERE actor_id = 175").await;

        let results = verify_results_with(&ppl, VerifyOptions { allow_extra: true }).await;
        let missing_one = results.iter().any(|r| {
            matches!(
                r,
                VerificationResult::Mismatch { summary, .. }
                    if summary.missing == 1 && summary.changed == 0
            )
        });

        assert!(missing_one, "a deleted covered row must still fail");
    }

    /// Load `actor` in two halves: the first without integrity (so no receipt
    /// covers it), the second with. Returns the config for the second migration,
    /// which is the one under verification.
    async fn seed_then_append() -> String {
        reset_postgres_schema().await;

        let seed = Direction::MYSQL_TO_POSTGRES.ppl(
            r#"
            pipeline "seed_first_half" {
                from { connection = connection.src  table = "actor" }
                to   { connection = connection.dst  table = "actor" }
                where "first_half" { actor.actor_id <= 100 }
                settings { create_missing_tables = true  batch_size = 50 }
            }
        "#,
        );
        run_ppl(&seed, false).await.expect("seed apply failed");

        let append = Direction::MYSQL_TO_POSTGRES.ppl(
            r#"
            pipeline "append_second_half" {
                from { connection = connection.src  table = "actor" }
                to   { connection = connection.dst  table = "actor" }
                where "second_half" { actor.actor_id > 100 }
                settings { create_missing_tables = true  batch_size = 50 }
            }
        "#,
        );
        run_ppl(&append, true).await.expect("append apply failed");

        assert_eq!(
            get_row_count("actor", "sakila", DbType::Postgres).await,
            200,
            "both halves should be in the destination"
        );

        append
    }

    /// Run verify and hand back the raw results for assertions on detail.
    async fn verify_results(ppl: &str) -> Vec<VerificationResult> {
        let doc = parse(ppl).expect("parse");
        let env = Arc::new(EnvContext::empty());
        let plan = ExecutionPlan::build(&doc, env.clone()).expect("build plan");

        verify(plan, env).await.expect("verify call failed")
    }

    /// Run verify with explicit options and hand back the raw results.
    async fn verify_results_with(ppl: &str, options: VerifyOptions) -> Vec<VerificationResult> {
        let doc = parse(ppl).expect("parse");
        let env = Arc::new(EnvContext::empty());
        let plan = ExecutionPlan::build(&doc, env.clone()).expect("build plan");

        verify_with_progress(plan, env, &mut NoopProgress, options)
            .await
            .expect("verify call failed")
    }
}

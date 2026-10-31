#[cfg(test)]
mod tests {
    use crate::{
        harness::runner::{DbType, execute, get_row_count, run_ppl, run_verify_ppl},
        reset_postgres_schema,
        verify::support::verify_results,
    };
    use model::integrity::result::VerificationResult;
    use tracing_test::traced_test;

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
}

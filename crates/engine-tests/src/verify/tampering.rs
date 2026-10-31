#[cfg(test)]
mod tests {
    use crate::{
        harness::runner::{execute, run_ppl, run_verify_ppl},
        reset_postgres_schema,
        verify::support::verify_results,
    };
    use model::integrity::result::{DivergenceKind, VerificationResult};
    use tracing_test::traced_test;

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
}

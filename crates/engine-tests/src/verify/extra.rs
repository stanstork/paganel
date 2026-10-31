#[cfg(test)]
mod tests {
    use crate::{
        harness::runner::execute,
        verify::support::{seed_then_append, verify_results, verify_results_with},
    };
    use engine_verify::VerifyOptions;
    use model::integrity::result::VerificationResult;
    use tracing_test::traced_test;

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
}

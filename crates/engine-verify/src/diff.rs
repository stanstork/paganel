use std::cmp::Ordering;

use crate::{error::VerifyError, verifier::VerifyOptions};
use engine_state::{PROGRESS_INTERVAL, RowHashIter, Ticker};
use model::integrity::{
    algorithm::HashAlgorithm,
    merkle::MerkleAccumulator,
    receipt::VerificationReceipt,
    result::{Divergence, DivergenceKind, DivergenceSummary, MAX_REPORTED_DIVERGENCES},
    row_key::{KeyedRowHash, describe},
};
use tracing::{debug, info};

/// Rows between clock reads in the per-row diff loop.
const DIFF_CLOCK_SAMPLE: u64 = 1 << 16;

#[derive(Debug, Clone)]
pub struct DiffInput {
    pub table: String,
    pub key_columns: Vec<String>,
    pub algorithm: HashAlgorithm,
    pub expected_rows: u64,
}

impl DiffInput {
    pub fn from_receipt(receipt: &VerificationReceipt) -> Self {
        Self {
            table: receipt.table_name.clone(),
            key_columns: receipt.key_columns.clone(),
            algorithm: receipt.algorithm,
            expected_rows: receipt.total_rows,
        }
    }
}

#[derive(Debug)]
pub struct DiffOutcome {
    pub actual_root: [u8; 32],
    pub summary: DivergenceSummary,
    pub divergences: Vec<Divergence>,
    pub expected_seen: u64,
}

/// Where the merge-join's two heads stand relative to each other.
enum Step<'a> {
    Missing(&'a KeyedRowHash),
    Extra(&'a KeyedRowHash),
    Both(&'a KeyedRowHash, &'a KeyedRowHash),
}

/// Merge-join two key-sorted row-hash streams.
pub fn diff_keyed_sets(
    expected: RowHashIter,
    actual: RowHashIter,
    input: &DiffInput,
    dest_rows_read: u64,
    options: VerifyOptions,
) -> Result<DiffOutcome, VerifyError> {
    let mut compared = 0u64;
    let mut expected_seen = 0u64;
    let mut ticker = Ticker::new(PROGRESS_INTERVAL).sampling(DIFF_CLOCK_SAMPLE);

    let mut acc = MerkleAccumulator::new(input.algorithm);
    let mut summary = DivergenceSummary {
        expected_rows: input.expected_rows,
        ..Default::default()
    };

    let mut divergences: Vec<Divergence> = Vec::new();

    let mut record = |kind: DivergenceKind, key: &[u8]| {
        if divergences.len() < MAX_REPORTED_DIVERGENCES {
            divergences.push(Divergence {
                key: describe(key, &input.key_columns),
                kind,
            });
        }
    };

    let mut expected = PeekPairs::new(expected)?;
    let mut actual = PeekPairs::new(actual)?;

    loop {
        compared += 1;
        if ticker.report(compared) {
            info!(
                table = %input.table,
                rows = summary.actual_rows,
                expected = input.expected_rows,
                "comparing{}",
                percent_of(summary.actual_rows, input.expected_rows)
            );
        }

        let step = match (expected.current(), actual.current()) {
            (None, None) => break,
            (Some(exp), None) => Step::Missing(exp),
            (None, Some(act)) => Step::Extra(act),
            (Some(exp), Some(act)) => match exp.key.cmp(&act.key) {
                Ordering::Less => Step::Missing(exp),
                Ordering::Greater => Step::Extra(act),
                Ordering::Equal => Step::Both(exp, act),
            },
        };

        match step {
            Step::Missing(exp) => {
                // expected is missing from actual
                expected_seen += 1;
                summary.missing += 1;

                record(
                    DivergenceKind::Missing {
                        expected_hash: exp.hash,
                    },
                    &exp.key,
                );
                expected.advance()?;
            }
            Step::Extra(act) => {
                // actual has an extra row not in expected
                summary.extra += 1;

                if !options.allow_extra {
                    summary.actual_rows += 1;
                    acc.push_row(&act.key, &act.hash);
                    record(
                        DivergenceKind::Extra {
                            actual_hash: act.hash,
                        },
                        &act.key,
                    );
                }
                actual.advance()?;
            }
            Step::Both(exp, act) => {
                // rows exist in both, compare hashes
                expected_seen += 1;
                summary.actual_rows += 1;
                acc.push_row(&act.key, &act.hash);

                if exp.hash != act.hash {
                    summary.changed += 1;
                    record(
                        DivergenceKind::Changed {
                            expected_hash: exp.hash,
                            actual_hash: act.hash,
                        },
                        &act.key,
                    );
                }

                expected.advance()?;
                actual.advance()?;
            }
        }
    }

    if dest_rows_read > summary.actual_rows {
        debug!(
            table = %input.table,
            rows_read = dest_rows_read,
            distinct_keys = summary.actual_rows,
            "destination has rows that share a verification key"
        );
    }

    Ok(DiffOutcome {
        actual_root: acc.finish(),
        summary,
        divergences,
        expected_seen,
    })
}

pub(crate) fn percent_of(done: u64, total: u64) -> String {
    if total == 0 {
        return String::new();
    }
    format!(" ({}%)", (done.min(total) * 100) / total)
}

/// One-item lookahead over a fallible row-hash iterator, so the merge-join can
/// compare heads without buffering either side.
struct PeekPairs {
    iter: RowHashIter,
    head: Option<KeyedRowHash>,
}

impl PeekPairs {
    fn new(mut iter: RowHashIter) -> Result<Self, VerifyError> {
        let head = iter.next().transpose()?;
        Ok(Self { iter, head })
    }

    fn current(&self) -> Option<&KeyedRowHash> {
        self.head.as_ref()
    }

    fn advance(&mut self) -> Result<(), VerifyError> {
        self.head = self.iter.next().transpose()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::integrity::{algorithm::HashAlgorithm, merkle::MerkleAccumulator};

    fn receipt(rows: &[(&[u8], u8)]) -> VerificationReceipt {
        let mut acc = MerkleAccumulator::new(HashAlgorithm::Sha256);
        for (key, hash) in rows {
            acc.push_row(key, &[*hash; 32]);
        }
        VerificationReceipt {
            run_id: "run".into(),
            pipeline_name: "p".into(),
            table_name: "t".into(),
            table_root: acc.finish(),
            column_order: vec!["id".into()],
            key_columns: vec!["id".into()],
            total_rows: rows.len() as u64,
            skipped_rows: 0,
            algorithm: HashAlgorithm::Sha256,
            created_at: chrono::Utc::now(),
        }
    }

    /// Build a store-shaped iterator: entries must be in ascending key order,
    /// exactly as sled yields them.
    fn iter(rows: &[(&[u8], u8)]) -> RowHashIter {
        let mut owned: Vec<KeyedRowHash> = rows
            .iter()
            .map(|(key, hash)| KeyedRowHash {
                key: key.to_vec(),
                hash: [*hash; 32],
            })
            .collect();
        owned.sort_by(|a, b| a.key.cmp(&b.key));
        Box::new(owned.into_iter().map(Ok))
    }

    fn diff(
        expected: &[(&[u8], u8)],
        actual: &[(&[u8], u8)],
    ) -> ([u8; 32], DivergenceSummary, Vec<Divergence>) {
        diff_with(expected, actual, VerifyOptions::default())
    }

    fn diff_with(
        expected: &[(&[u8], u8)],
        actual: &[(&[u8], u8)],
        options: VerifyOptions,
    ) -> ([u8; 32], DivergenceSummary, Vec<Divergence>) {
        let r = receipt(expected);
        let rows = actual.len() as u64;
        let out = diff_keyed_sets(
            iter(expected),
            iter(actual),
            &DiffInput::from_receipt(&r),
            rows,
            options,
        )
        .expect("diff");
        // Every committed row is streamed off the (in-memory) apply set here, so
        // the log-consistency invariant verify relies on must hold.
        assert_eq!(out.expected_seen, expected.len() as u64);
        (out.actual_root, out.summary, out.divergences)
    }

    #[test]
    fn identical_sets_match_and_reproduce_the_root() {
        let rows: &[(&[u8], u8)] = &[(b"a", 1), (b"b", 2), (b"c", 3)];
        let (root, summary, divergences) = diff(rows, rows);

        assert!(summary.is_clean());
        assert!(divergences.is_empty());
        assert_eq!(root, receipt(rows).table_root);
        assert_eq!(summary.actual_rows, 3);
    }

    /// The point of the whole change: the destination is read in one order and
    /// the receipt was written in another, and it still matches.
    #[test]
    fn arrival_order_does_not_affect_the_root() {
        let forward: &[(&[u8], u8)] = &[(b"a", 1), (b"b", 2), (b"c", 3)];
        let shuffled: &[(&[u8], u8)] = &[(b"c", 3), (b"a", 1), (b"b", 2)];

        let (root, summary, _) = diff(forward, shuffled);
        assert!(summary.is_clean());
        assert_eq!(root, receipt(forward).table_root);
    }

    #[test]
    fn deleted_row_is_reported_as_missing_by_key() {
        let (root, summary, divergences) = diff(&[(b"a", 1), (b"b", 2)], &[(b"a", 1)]);

        assert_eq!(summary.missing, 1);
        assert_eq!(summary.changed, 0);
        assert_eq!(summary.extra, 0);
        assert_ne!(root, receipt(&[(b"a", 1), (b"b", 2)]).table_root);
        assert!(matches!(
            divergences.as_slice(),
            [Divergence {
                kind: DivergenceKind::Missing { .. },
                ..
            }]
        ));
    }

    #[test]
    fn inserted_row_is_reported_as_extra() {
        let (_, summary, divergences) = diff(&[(b"a", 1)], &[(b"a", 1), (b"z", 9)]);

        assert_eq!(summary.extra, 1);
        assert_eq!(summary.missing, 0);
        assert_eq!(summary.actual_rows, 2);
        assert!(matches!(
            divergences.as_slice(),
            [Divergence {
                kind: DivergenceKind::Extra { .. },
                ..
            }]
        ));
    }

    /// A tampered row keeps its key, so it is one `changed` - not a missing row
    /// plus an unrelated extra one.
    #[test]
    fn rows_already_in_the_destination_are_reported_as_extra() {
        let migrated: &[(&[u8], u8)] = &[(b"b", 2), (b"c", 3)];
        let destination: &[(&[u8], u8)] = &[(b"a", 9), (b"b", 2), (b"c", 3)];

        let (root, summary, divergences) = diff(migrated, destination);

        assert!(!summary.is_clean());
        assert_eq!(summary.extra, 1);
        assert_eq!(summary.missing, 0);
        assert_eq!(summary.changed, 0);
        assert_ne!(root, receipt(migrated).table_root);
        assert!(matches!(
            divergences.as_slice(),
            [Divergence {
                kind: DivergenceKind::Extra { .. },
                ..
            }]
        ));
    }

    /// The same table, verified with `--allow-extra`: the uncovered row is
    /// counted for the report but not folded in, so the receipt's own rows still
    /// have to reproduce the root exactly.
    #[test]
    fn allow_extra_ignores_uncovered_rows_and_still_reproduces_the_root() {
        let migrated: &[(&[u8], u8)] = &[(b"b", 2), (b"c", 3)];
        let destination: &[(&[u8], u8)] = &[(b"a", 9), (b"b", 2), (b"c", 3)];

        let (root, summary, divergences) =
            diff_with(migrated, destination, VerifyOptions { allow_extra: true });

        assert_eq!(summary.extra, 1, "counted, so the report can say so");
        assert_eq!(summary.missing, 0);
        assert_eq!(summary.changed, 0);
        assert_eq!(root, receipt(migrated).table_root);
        assert!(divergences.is_empty());
    }

    /// `--allow-extra` forgives uncovered keys and nothing else: a committed row
    /// that is gone or altered still fails.
    #[test]
    fn allow_extra_still_catches_missing_and_changed_rows() {
        let migrated: &[(&[u8], u8)] = &[(b"b", 2), (b"c", 3)];
        let destination: &[(&[u8], u8)] = &[(b"a", 9), (b"c", 7)];

        let (root, summary, _) =
            diff_with(migrated, destination, VerifyOptions { allow_extra: true });

        assert_eq!(summary.missing, 1, "b");
        assert_eq!(summary.changed, 1, "c");
        assert_ne!(root, receipt(migrated).table_root);
    }

    #[test]
    fn tampered_row_is_reported_as_changed() {
        let (_, summary, divergences) = diff(&[(b"a", 1), (b"b", 2)], &[(b"a", 1), (b"b", 7)]);

        assert_eq!(summary.changed, 1);
        assert_eq!(summary.missing, 0);
        assert_eq!(summary.extra, 0);
        assert!(matches!(
            divergences.as_slice(),
            [Divergence {
                kind: DivergenceKind::Changed { .. },
                ..
            }]
        ));
    }

    #[test]
    fn every_difference_kind_is_caught_in_one_pass() {
        let (_, summary, _) = diff(
            &[(b"a", 1), (b"b", 2), (b"c", 3)],
            &[(b"a", 1), (b"c", 9), (b"d", 4)],
        );

        assert_eq!(summary.missing, 1, "b");
        assert_eq!(summary.changed, 1, "c");
        assert_eq!(summary.extra, 1, "d");
    }

    #[test]
    fn empty_destination_reports_every_row_missing() {
        let (_, summary, _) = diff(&[(b"a", 1), (b"b", 2)], &[]);
        assert_eq!(summary.missing, 2);
        assert_eq!(summary.actual_rows, 0);
    }

    /// An empty committed set with a populated destination flags every row as
    /// `extra`, and `expected_seen` is 0. That zero (against a receipt that
    /// expected rows) is what `verify_table` turns into `LogUnavailable` instead
    /// of a false mismatch when the apply row-hash log is missing.
    #[test]
    fn no_committed_rows_yields_all_extra_and_zero_expected_seen() {
        // `diff` asserts expected_seen == 0 for an empty committed set.
        let (_, summary, _) = diff(&[], &[(b"a", 1), (b"b", 2)]);
        assert_eq!(summary.extra, 2);
        assert_eq!(summary.actual_rows, 2);
    }

    #[test]
    fn divergence_detail_is_capped_but_counts_are_not() {
        let expected: Vec<(&[u8], u8)> = (0..255u8)
            .map(|i| (Box::leak(vec![i].into_boxed_slice()) as &[u8], i))
            .collect();
        let (_, summary, divergences) = diff(&expected, &[]);

        assert_eq!(summary.missing, 255);
        assert_eq!(divergences.len(), MAX_REPORTED_DIVERGENCES);
    }
}

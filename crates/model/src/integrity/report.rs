use crate::integrity::{
    receipt::root_hex,
    result::{Divergence, DivergenceKind, VerificationResult},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReportResult {
    Match,
    Mismatch,
    /// No receipt for this table: it was migrated without `--integrity`.
    NoPriorRun,
    /// A receipt exists, but its row-hash log is missing or truncated.
    LogUnavailable,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableReport {
    pub pipeline: String,
    pub table: String,
    pub result: ReportResult,
    /// Rows the receipt committed.
    pub expected_rows: u64,
    /// Distinct row keys found in the destination; `None` when the table was
    /// not compared.
    pub actual_rows: Option<u64>,
    pub missing: u64,
    pub changed: u64,
    pub extra: u64,
    /// Destination rows outside the receipt, accepted by `--allow-extra`.
    pub extra_ignored: u64,
    pub expected_root: Option<String>,
    pub actual_root: Option<String>,
    /// The first divergences found, at most `MAX_REPORTED_DIVERGENCES`;
    /// `missing + changed + extra` is the full count.
    pub divergences: Vec<DivergenceReport>,
    pub duration_ms: u64,
    /// Why the result is not a comparison, for `log_unavailable`.
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DivergenceReport {
    /// Row key as text, e.g. `actor_id=42`.
    pub key: String,
    pub kind: DivergenceReportKind,
    pub expected_hash: Option<String>,
    pub actual_hash: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DivergenceReportKind {
    Missing,
    Changed,
    Extra,
}

impl TableReport {
    /// A row-less report for a table that was not compared.
    fn uncompared(pipeline: String, table: String, result: ReportResult) -> Self {
        Self {
            pipeline,
            table,
            result,
            expected_rows: 0,
            actual_rows: None,
            missing: 0,
            changed: 0,
            extra: 0,
            extra_ignored: 0,
            expected_root: None,
            actual_root: None,
            divergences: Vec::new(),
            duration_ms: 0,
            message: None,
        }
    }
}

impl From<&Divergence> for DivergenceReport {
    fn from(d: &Divergence) -> Self {
        let (kind, expected, actual) = match &d.kind {
            DivergenceKind::Missing { expected_hash } => {
                (DivergenceReportKind::Missing, Some(expected_hash), None)
            }
            DivergenceKind::Changed {
                expected_hash,
                actual_hash,
            } => (
                DivergenceReportKind::Changed,
                Some(expected_hash),
                Some(actual_hash),
            ),
            DivergenceKind::Extra { actual_hash } => {
                (DivergenceReportKind::Extra, None, Some(actual_hash))
            }
        };

        Self {
            key: d.key.clone(),
            kind,
            expected_hash: expected.map(root_hex),
            actual_hash: actual.map(root_hex),
        }
    }
}

impl From<&VerificationResult> for TableReport {
    fn from(result: &VerificationResult) -> Self {
        match result {
            VerificationResult::Match {
                receipt,
                duration_ms,
                extra_ignored,
            } => {
                let root = receipt.root_hex();
                Self {
                    pipeline: receipt.pipeline_name.clone(),
                    table: receipt.table_name.clone(),
                    result: ReportResult::Match,
                    expected_rows: receipt.total_rows,
                    actual_rows: Some(receipt.total_rows + extra_ignored),
                    missing: 0,
                    changed: 0,
                    extra: 0,
                    extra_ignored: *extra_ignored,
                    expected_root: Some(root.clone()),
                    actual_root: Some(root),
                    divergences: Vec::new(),
                    duration_ms: *duration_ms,
                    message: None,
                }
            }
            VerificationResult::Mismatch {
                receipt,
                actual_root,
                summary,
                divergences,
                duration_ms,
            } => Self {
                pipeline: receipt.pipeline_name.clone(),
                table: receipt.table_name.clone(),
                result: ReportResult::Mismatch,
                expected_rows: summary.expected_rows,
                actual_rows: Some(summary.actual_rows),
                missing: summary.missing,
                changed: summary.changed,
                extra: summary.extra,
                extra_ignored: 0,
                expected_root: Some(receipt.root_hex()),
                actual_root: Some(root_hex(actual_root)),
                divergences: divergences.iter().map(DivergenceReport::from).collect(),
                duration_ms: *duration_ms,
                message: None,
            },
            VerificationResult::NoPriorRun { pipeline, table } => {
                Self::uncompared(pipeline.clone(), table.clone(), ReportResult::NoPriorRun)
            }
            VerificationResult::LogUnavailable {
                pipeline,
                table,
                expected_rows,
                found_rows,
            } => Self {
                expected_rows: *expected_rows,
                message: Some(format!(
                    "row-hash log is missing or truncated ({found_rows} of {expected_rows} \
                     committed rows on disk); re-run `apply --integrity` before verifying"
                )),
                ..Self::uncompared(
                    pipeline.clone(),
                    table.clone(),
                    ReportResult::LogUnavailable,
                )
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::integrity::{
        algorithm::HashAlgorithm,
        receipt::VerificationReceipt,
        result::{Divergence, DivergenceSummary},
    };

    fn receipt() -> VerificationReceipt {
        VerificationReceipt {
            run_id: "run-1".into(),
            pipeline_name: "migrate_actor".into(),
            table_name: "actor".into(),
            table_root: [0xab; 32],
            column_order: vec!["actor_id".into()],
            key_columns: vec!["actor_id".into()],
            total_rows: 200,
            skipped_rows: 0,
            algorithm: HashAlgorithm::Sha256,
            created_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn roots_and_hashes_serialize_as_hex() {
        let report = TableReport::from(&VerificationResult::Mismatch {
            receipt: receipt(),
            actual_root: [0xcd; 32],
            summary: DivergenceSummary {
                missing: 1,
                changed: 1,
                extra: 0,
                expected_rows: 200,
                actual_rows: 199,
            },
            divergences: vec![
                Divergence {
                    key: "actor_id=1".into(),
                    kind: DivergenceKind::Missing {
                        expected_hash: [0x01; 32],
                    },
                },
                Divergence {
                    key: "actor_id=2".into(),
                    kind: DivergenceKind::Changed {
                        expected_hash: [0x02; 32],
                        actual_hash: [0x03; 32],
                    },
                },
            ],
            duration_ms: 12,
        });

        let json = serde_json::to_value(&report).unwrap();
        assert_eq!(json["result"], "mismatch");
        assert_eq!(json["expected_root"], "ab".repeat(32));
        assert_eq!(json["actual_root"], "cd".repeat(32));
        assert_eq!(json["actual_rows"], 199);
        assert_eq!(json["divergences"][0]["kind"], "missing");
        assert_eq!(json["divergences"][0]["expected_hash"], "01".repeat(32));
        assert!(json["divergences"][0]["actual_hash"].is_null());
        assert_eq!(json["divergences"][1]["kind"], "changed");
        assert_eq!(json["divergences"][1]["actual_hash"], "03".repeat(32));
    }

    #[test]
    fn a_match_reports_the_reproduced_root_and_ignored_rows() {
        let report = TableReport::from(&VerificationResult::Match {
            receipt: receipt(),
            duration_ms: 5,
            extra_ignored: 7,
        });

        assert_eq!(report.result, ReportResult::Match);
        assert_eq!(report.actual_root, report.expected_root);
        assert_eq!(report.actual_rows, Some(207));
        assert_eq!(report.extra, 0);
        assert_eq!(report.extra_ignored, 7);
    }

    #[test]
    fn uncompared_tables_name_their_table_and_carry_no_counts() {
        let none = TableReport::from(&VerificationResult::NoPriorRun {
            pipeline: "migrate_film".into(),
            table: "film".into(),
        });
        assert_eq!(none.table, "film");
        assert_eq!(none.actual_rows, None);
        assert_eq!(
            serde_json::to_value(&none).unwrap()["result"],
            "no_prior_run"
        );

        let log = TableReport::from(&VerificationResult::LogUnavailable {
            pipeline: "migrate_actor".into(),
            table: "actor".into(),
            expected_rows: 200,
            found_rows: 50,
        });
        assert_eq!(log.result, ReportResult::LogUnavailable);
        assert_eq!(log.expected_rows, 200);
        assert!(log.message.unwrap().contains("50 of 200"));
    }
}

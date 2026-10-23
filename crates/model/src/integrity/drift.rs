use crate::integrity::report::{ReportResult, TableReport};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Drift runs kept per config; older ones are pruned on save.
pub const KEPT_RUNS: usize = 100;

/// A target drift did not check, and why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Skipped {
    /// Pipeline, `pipeline/table` or import name.
    pub name: String,
    pub reason: String,
}

/// One `pag drift` pass over every target of a config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriftRun {
    pub run_id: String,
    pub config: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub tables: Vec<TableReport>,
    pub skipped: Vec<Skipped>,
}

impl DriftRun {
    pub fn run_id_at(at: DateTime<Utc>) -> String {
        format!("drift-{}", at.format("%Y%m%dT%H%M%S%.3fZ"))
    }

    /// 3 on any mismatch, else 4 when a table could not be confirmed (its log
    /// is missing or it failed), else 0. Skipped targets do not count.
    pub fn exit_code(&self) -> i32 {
        let any = |result: ReportResult| self.tables.iter().any(|t| t.result == result);

        if any(ReportResult::Mismatch) {
            3
        } else if any(ReportResult::LogUnavailable) || any(ReportResult::Error) {
            4
        } else {
            0
        }
    }

    /// Flag each mismatch that was not a mismatch in `last`.
    pub fn mark_new_since(&mut self, last: Option<&DriftRun>) {
        let before: HashSet<(&str, &str)> = last
            .map(|run| {
                run.tables
                    .iter()
                    .filter(|t| t.result == ReportResult::Mismatch)
                    .map(|t| (t.pipeline.as_str(), t.table.as_str()))
                    .collect()
            })
            .unwrap_or_default();

        for t in &mut self.tables {
            t.new_since_last = t.result == ReportResult::Mismatch
                && !before.contains(&(t.pipeline.as_str(), t.table.as_str()));
        }
    }
}

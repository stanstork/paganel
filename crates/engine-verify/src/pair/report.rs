use crate::{DiffOutcome, blocks::BlockStats};
use model::integrity::report::{DivergenceReport, ReportResult, TableReport};
use std::time::Instant;

/// The report when every segment matched: no row was read.
pub(super) fn block_match(
    pipeline: &str,
    table: &str,
    stats: BlockStats,
    start: Instant,
) -> TableReport {
    TableReport {
        pipeline: pipeline.to_string(),
        table: table.to_string(),
        result: ReportResult::Match,
        expected_rows: stats.rows[0],
        actual_rows: Some(stats.rows[1]),
        missing: 0,
        changed: 0,
        extra: 0,
        extra_ignored: 0,
        expected_root: None,
        actual_root: None,
        divergences: Vec::new(),
        duration_ms: start.elapsed().as_millis() as u64,
        message: None,
        new_since_last: false,
        recheck: None,
    }
}

pub(super) fn report_from(pipeline: &str, table: &str, outcome: &DiffOutcome) -> TableReport {
    let summary = &outcome.summary;

    TableReport {
        pipeline: pipeline.to_string(),
        table: table.to_string(),
        result: ReportResult::Match,
        expected_rows: outcome.expected_seen,
        actual_rows: Some(summary.actual_rows),
        missing: summary.missing,
        changed: summary.changed,
        extra: summary.extra,
        extra_ignored: 0,
        expected_root: None,
        actual_root: None,
        divergences: outcome
            .divergences
            .iter()
            .map(DivergenceReport::from)
            .collect(),
        duration_ms: 0,
        message: None,
        new_since_last: false,
        recheck: None,
    }
}

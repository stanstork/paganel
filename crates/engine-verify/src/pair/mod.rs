mod recheck;
mod report;
mod sides;
mod specs;

pub(crate) use specs::SideSpecs;

use crate::{
    VerifyProgress,
    blocks::{self, KeyRange, Narrowed},
    diff::MAX_RECHECK_KEYS,
    error::VerifyError,
    profile,
    table::fetch_table_metadata,
};
use engine_core::{
    drivers::DriverRef,
    plan::pair::{BlockMode, PairSpec},
};
use engine_state::{RowHashLog, RowHashScope};
use model::integrity::{
    pair::pair_pipeline,
    report::{Recheck, ReportResult, TableReport},
};
use report::{block_match, report_from};
use sides::{Sides, both};
use specs::side_specs;
use std::time::Instant;
use tracing::{info, warn};

/// Read a pair's two live tables, diff them by key, and re-read the keys
/// that differ after `grace`.
pub async fn diff_pair(
    spec: &PairSpec,
    source: &DriverRef,
    target: &DriverRef,
    hash_log: &RowHashLog,
    progress: &mut dyn VerifyProgress,
) -> Result<TableReport, VerifyError> {
    let start = Instant::now();
    let pipeline = pair_pipeline(&spec.name);
    let table = spec.target.table.as_str();
    progress.table_started(&pipeline, table);

    let shared = spec.source.connection.name == spec.target.connection.name;

    let (source_meta, target_meta) = both(
        shared,
        fetch_table_metadata(source, &spec.source.table),
        fetch_table_metadata(target, table),
    )
    .await?;

    let specs = side_specs(
        &spec.name,
        &spec.columns,
        &spec.source.ignore,
        &spec.target.ignore,
        spec.key.as_deref(),
        &source_meta,
        &target_meta,
    )?;

    let sides = Sides {
        spec,
        source,
        target,
        source_meta: &source_meta,
        target_meta: &target_meta,
        specs: &specs,
        shared,
    };

    match spec.block.mode {
        BlockMode::Full => diff_rows(sides, hash_log, &pipeline, progress, None, start).await,
        mode => diff_blocks(sides, mode, hash_log, &pipeline, progress, start).await,
    }
}

/// Narrow the pair with block fingerprints, then diff only the key ranges
/// that differ, on block mode's own session per side.
async fn diff_blocks(
    sides: Sides<'_>,
    mode: BlockMode,
    hash_log: &RowHashLog,
    pipeline: &str,
    progress: &mut dyn VerifyProgress,
    start: Instant,
) -> Result<TableReport, VerifyError> {
    let spec = sides.spec;
    progress.table_phase("comparing segment fingerprints");

    let reason = match blocks::run(spec, sides.specs, sides.source_meta, sides.target_meta).await {
        Ok(run) => match run.narrowed {
            Narrowed::Match(stats) => {
                return Ok(block_match(pipeline, &spec.target.table, stats, start));
            }
            Narrowed::Leaves { ranges, stats } => {
                let sessions = Sides {
                    source: &run.source,
                    target: &run.target,
                    // Two sessions, so the sides can run at once.
                    shared: false,
                    ..sides
                };
                let mut report =
                    diff_rows(sessions, hash_log, pipeline, progress, Some(&ranges), start).await?;
                // The leaf read saw only the differing ranges; level 0 counted all.
                report.expected_rows = stats.rows[0];
                report.actual_rows = Some(stats.rows[1]);
                return Ok(report);
            }
            Narrowed::Fallback(reason) => reason,
        },
        Err(e) if mode == BlockMode::Auto => {
            warn!(pair = %spec.name, error = %e, "block mode failed");
            format!("block mode failed: {e}")
        }
        Err(e) => return Err(e),
    };

    if mode == BlockMode::Blocks {
        return Err(VerifyError::InitializationError(format!(
            "pair \"{}\": block mode: {reason}",
            spec.name
        )));
    }
    info!(pair = %spec.name, %reason, "block mode not used; reading both tables in full");
    diff_rows(sides, hash_log, pipeline, progress, None, start).await
}

/// Hash both sides into the row-hash log, diff them by key, and re-read the
/// keys that differ after `grace`: the whole tables, or only `ranges` of the
/// first key.
async fn diff_rows(
    sides: Sides<'_>,
    hash_log: &RowHashLog,
    pipeline: &str,
    progress: &mut dyn VerifyProgress,
    ranges: Option<&[KeyRange]>,
    start: Instant,
) -> Result<TableReport, VerifyError> {
    let spec = sides.spec;
    let table = spec.target.table.as_str();
    let scopes = [RowHashScope::Source, RowHashScope::Verify];

    for scope in scopes {
        hash_log.clear(scope, pipeline, table)?;
    }

    let outcome = sides
        .scan_and_diff(hash_log, pipeline, progress, ranges)
        .await;

    for scope in scopes {
        if let Err(e) = hash_log.clear(scope, pipeline, table) {
            warn!(pair = %spec.name, error = %e, "failed to clear pair scratch row hashes");
        }
    }

    let outcome = outcome?;
    let mut report = report_from(pipeline, table, &outcome);

    if !outcome.summary.is_clean() {
        let differing = report.missing + report.changed + report.extra;

        report.recheck = Some(if spec.grace.is_zero() {
            Recheck::Skipped {
                reason: "grace is 0".into(),
            }
        } else if differing > MAX_RECHECK_KEYS as u64 {
            Recheck::Skipped {
                reason: format!("{differing} keys differ; over {MAX_RECHECK_KEYS} is not lag"),
            }
        } else {
            progress.table_phase("waiting to re-check");
            info!(pair = %spec.name, keys = differing, grace = ?spec.grace, "re-checking differing keys after grace");
            tokio::time::sleep(spec.grace).await;

            let reread = Instant::now();
            let rechecked = sides.recheck(&outcome.divergent_keys).await?;
            profile::log_recheck(
                &spec.name,
                outcome.divergent_keys.len(),
                spec.grace,
                reread.elapsed(),
            );

            match rechecked {
                Some(rechecked) => rechecked.apply(&mut report),
                None => Recheck::Skipped {
                    reason: "only integer, text and UUID keys can be re-read".into(),
                },
            }
        });
    }

    report.result = if report.missing + report.changed + report.extra == 0 {
        ReportResult::Match
    } else {
        ReportResult::Mismatch
    };

    report.duration_ms = start.elapsed().as_millis() as u64;
    Ok(report)
}

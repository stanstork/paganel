use super::{
    BlockStats, KeyRange, Narrowed,
    plan::{column_plan, normal_sql},
    segments::{Grid, Segment, differing, merge, split},
    side::{DEFAULT_QUERY_WAIT, Probe, Side, TIMEOUT_MARGIN},
};
use crate::{error::VerifyError, pair::SideSpecs};
use connectors::sql::{checksum::Segments, metadata::table::TableMetadata};
use engine_core::{drivers::DriverRef, plan::pair::PairSpec};
use std::collections::BTreeSet;
use tracing::info;

pub(super) async fn narrow(
    spec: &PairSpec,
    specs: &SideSpecs,
    source_meta: &TableMetadata,
    target_meta: &TableMetadata,
    source_driver: &DriverRef,
    target_driver: &DriverRef,
) -> Result<Narrowed, VerifyError> {
    let plan = match column_plan(
        specs,
        source_meta,
        target_meta,
        normal_sql(source_driver),
        normal_sql(target_driver),
    ) {
        Ok(plan) => plan,
        Err(reason) => return Ok(Narrowed::Fallback(reason)),
    };

    let wait = spec
        .block
        .statement_timeout
        .map_or(DEFAULT_QUERY_WAIT, |t| t + TIMEOUT_MARGIN);
    let source = Side::new(
        "source",
        source_driver,
        &spec.source,
        source_meta,
        &specs.source,
        plan.source,
        wait,
    )?;
    let target = Side::new(
        "target",
        target_driver,
        &spec.target,
        target_meta,
        &specs.target,
        plan.target,
        wait,
    )?;

    // Pre-flight: the SQL must say what the reader says.
    let (a, b) = tokio::try_join!(
        source.preflight(&plan.families),
        target.preflight(&plan.families)
    )?;
    if let Some(reason) = a.or(b) {
        return Ok(Narrowed::Fallback(format!("pre-flight: {reason}")));
    }

    // Level 0 covers both sides' key ranges.
    let (a, b) = tokio::try_join!(source.key_range(), target.key_range())?;
    let (lo, hi) = match (a, b) {
        (Probe::Unsupported(reason), _) | (_, Probe::Unsupported(reason)) => {
            return Ok(Narrowed::Fallback(reason));
        }
        (Probe::Empty, Probe::Empty) => return Ok(Narrowed::Match(BlockStats::default())),
        (Probe::Range(lo, hi), Probe::Empty) | (Probe::Empty, Probe::Range(lo, hi)) => (lo, hi),
        (Probe::Range(l1, h1), Probe::Range(l2, h2)) => (l1.min(l2), h1.max(h2)),
    };

    let grid = Grid::new(lo, hi);
    let level0 = Segments::Grid {
        lo: grid.lo,
        width: grid.width,
    };
    let (a, b) = tokio::try_join!(source.fingerprints(&level0), target.fingerprints(&level0))?;

    let mut stats = BlockStats {
        rows: [
            a.values().map(|h| h.rows).sum(),
            b.values().map(|h| h.rows).sum(),
        ],
        segments: a.keys().chain(b.keys()).collect::<BTreeSet<_>>().len() as u64,
        ..BlockStats::default()
    };
    let mut current = differing(&a, &b, |seg| grid.range(seg))?;

    // Segment grace: drop segments whose writes were still in flight.
    if !current.is_empty() && !spec.grace.is_zero() {
        info!(pair = %spec.name, segments = current.len(), grace = ?spec.grace, "re-checking differing segments after grace");
        tokio::time::sleep(spec.grace).await;
        let ranges: Vec<KeyRange> = current.iter().map(|s| s.range).collect();
        current = recompare(&source, &target, &ranges, &mut stats).await?;
    }

    // Wide differences: narrowing would only add round trips.
    let total = stats.rows[0].max(stats.rows[1]);
    let differing_rows: u64 = current.iter().map(|s| s.rows).sum();
    if differing_rows.saturating_mul(2) > total {
        info!(pair = %spec.name, differing_rows, total, "differences span over half the rows; reading those segments in full");
        return Ok(finish(
            current.into_iter().map(|s| s.range).collect(),
            stats,
        ));
    }

    let mut leaves = Vec::new();
    while !current.is_empty() {
        let mut children = Vec::new();
        for seg in current {
            if seg.rows > spec.block.leaf_rows && seg.range.0 < seg.range.1 {
                children.extend(split(seg.range));
            } else {
                leaves.push(seg.range);
            }
        }
        if children.is_empty() {
            break;
        }
        current = recompare(&source, &target, &children, &mut stats).await?;
    }

    Ok(finish(leaves, stats))
}

/// Fingerprint `segments` again on both sides; keep those that still differ.
async fn recompare(
    source: &Side<'_>,
    target: &Side<'_>,
    ranges: &[KeyRange],
    stats: &mut BlockStats,
) -> Result<Vec<Segment>, VerifyError> {
    let segments = Segments::Ranges(
        ranges
            .iter()
            .enumerate()
            .map(|(i, &(lo, hi))| (i as i64, lo, hi))
            .collect(),
    );
    let (a, b) = tokio::try_join!(
        source.fingerprints(&segments),
        target.fingerprints(&segments)
    )?;
    stats.rounds += 1;
    stats.segments += ranges.len() as u64;
    differing(&a, &b, |id| {
        usize::try_from(id)
            .ok()
            .and_then(|i| ranges.get(i).copied())
    })
}

fn finish(ranges: Vec<KeyRange>, mut stats: BlockStats) -> Narrowed {
    let ranges = merge(ranges);
    stats.leaf_ranges = ranges.len() as u64;
    if ranges.is_empty() {
        Narrowed::Match(stats)
    } else {
        Narrowed::Leaves { ranges, stats }
    }
}

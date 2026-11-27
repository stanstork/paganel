mod narrow;
mod plan;
mod preflight;
mod segments;
mod side;

use crate::{error::VerifyError, pair::SideSpecs};
use connectors::{session::SessionSettings, sql::metadata::table::TableMetadata};
use engine_core::{drivers::DriverRef, plan::pair::PairSpec};
use tracing::info;

/// Ranges per query, in fingerprints and leaf reads.
pub(crate) const MAX_RANGES_PER_QUERY: usize = 200;

/// A closed range of first-key values.
pub(crate) type KeyRange = (i64, i64);

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BlockStats {
    pub rounds: u32,      // fingerprint rounds after level 0
    pub segments: u64,    // segments compared over all levels
    pub rows: [u64; 2],   // rows per side at level 0
    pub leaf_ranges: u64, // ranges handed to the leaf read
}

#[derive(Debug)]
pub(crate) enum Narrowed {
    Match(BlockStats),
    Leaves {
        ranges: Vec<KeyRange>,
        stats: BlockStats,
    },
    Fallback(String),
}

pub(crate) struct BlockRun {
    pub source: DriverRef,
    pub target: DriverRef,
    pub narrowed: Narrowed,
}

/// Open a session per side and narrow the pair down to the key ranges that differ.
pub(crate) async fn run(
    spec: &PairSpec,
    specs: &SideSpecs,
    source_meta: &TableMetadata,
    target_meta: &TableMetadata,
) -> Result<BlockRun, VerifyError> {
    let settings = SessionSettings {
        statement_timeout: spec.block.statement_timeout,
    };
    let (source, target) = tokio::try_join!(
        DriverRef::open_session(&spec.source.connection, &settings),
        DriverRef::open_session(&spec.target.connection, &settings),
    )?;

    let narrowed = narrow::narrow(spec, specs, source_meta, target_meta, &source, &target).await?;
    if let Narrowed::Match(stats) | Narrowed::Leaves { stats, .. } = &narrowed {
        info!(
            pair = %spec.name,
            rounds = stats.rounds,
            segments = stats.segments,
            leaf_ranges = stats.leaf_ranges,
            source_rows = stats.rows[0],
            target_rows = stats.rows[1],
            "block mode",
        );
    }
    Ok(BlockRun {
        source,
        target,
        narrowed,
    })
}

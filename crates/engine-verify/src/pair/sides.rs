use super::SideSpecs;
use crate::{
    DiffInput, DiffOutcome, HashSink, HashSpec, VerifyOptions, VerifyProgress, blocks::KeyRange,
    diff_keyed_sets, error::VerifyError, hash_table, profile, table::hash_ranges,
};
use connectors::sql::{filter::SqlFilter, metadata::table::TableMetadata};
use engine_core::{drivers::DriverRef, plan::pair::PairSpec};
use engine_state::{RowHashLog, RowHashScope};
use std::time::Instant;
use tracing::{Instrument, info_span};

#[derive(Clone, Copy)]
pub(super) struct Sides<'a> {
    pub(super) spec: &'a PairSpec,
    pub(super) source: &'a DriverRef,
    pub(super) target: &'a DriverRef,
    pub(super) source_meta: &'a TableMetadata,
    pub(super) target_meta: &'a TableMetadata,
    pub(super) specs: &'a SideSpecs,
    pub(super) shared: bool,
}

impl Sides<'_> {
    /// Hash both tables into the row-hash log, sort them, and merge-join.
    pub(super) async fn scan_and_diff(
        &self,
        hash_log: &RowHashLog,
        pipeline: &str,
        progress: &mut dyn VerifyProgress,
        ranges: Option<&[KeyRange]>,
    ) -> Result<DiffOutcome, VerifyError> {
        let table = self.spec.target.table.as_ref();
        let make_sink = |scope| HashSink {
            log: hash_log,
            scope,
            pipeline,
            table,
        };

        let source_sink = make_sink(RowHashScope::Source);
        let target_sink = make_sink(RowHashScope::Verify);

        progress.table_phase(if ranges.is_some() {
            "reading differing segments"
        } else {
            "reading both tables"
        });
        let read = Instant::now();
        let (source_rows, target_rows) = both(
            self.shared,
            read_side(
                self.source,
                self.source_meta,
                &self.specs.source,
                &source_sink,
                self.spec.source.filter.as_ref(),
                ranges,
            )
            .instrument(info_span!("pair", side = %"source")),
            read_side(
                self.target,
                self.target_meta,
                &self.specs.target,
                &target_sink,
                self.spec.target.filter.as_ref(),
                ranges,
            )
            .instrument(info_span!("pair", side = %"target")),
        )
        .await?;
        let read = read.elapsed();

        progress.table_phase("sorting row hashes");
        let sort = Instant::now();
        hash_log.seal(RowHashScope::Source, pipeline, table)?;
        hash_log.seal(RowHashScope::Verify, pipeline, table)?;
        let sort = sort.elapsed();

        let expected = hash_log.stream(RowHashScope::Source, pipeline, table)?;
        let actual = hash_log.stream(RowHashScope::Verify, pipeline, table)?;
        let input = DiffInput {
            table: table.to_string(),
            key_columns: self.specs.source.key_columns.clone(),
            algorithm: self.specs.source.algorithm,
            expected_rows: source_rows,
        };

        progress.table_phase("comparing");
        let diff = Instant::now();
        let outcome = tokio::task::spawn_blocking(move || {
            diff_keyed_sets(
                expected,
                actual,
                &input,
                target_rows,
                VerifyOptions::default(),
            )
        })
        .await
        .map_err(|e| VerifyError::InitializationError(format!("pair task failed: {e}")))?;

        profile::log_pair(&self.spec.name, read, sort, diff.elapsed());
        outcome
    }
}

/// Hash one side into `sink`: the whole table, or only `ranges` of its first key.
async fn read_side(
    driver: &DriverRef,
    meta: &TableMetadata,
    spec: &HashSpec,
    sink: &HashSink<'_>,
    filter: Option<&SqlFilter>,
    ranges: Option<&[KeyRange]>,
) -> Result<u64, VerifyError> {
    match ranges {
        Some(ranges) => {
            hash_ranges(
                driver,
                meta,
                spec,
                sink,
                filter,
                &spec.key_columns[0],
                ranges,
            )
            .await
        }
        None => hash_table(driver, meta.clone(), spec, sink, filter, None).await,
    }
}

/// Run `a` and `b` together, or one after the other when they share a client.
pub(super) async fn both<A, B>(
    sequential: bool,
    a: impl Future<Output = Result<A, VerifyError>>,
    b: impl Future<Output = Result<B, VerifyError>>,
) -> Result<(A, B), VerifyError> {
    if sequential {
        Ok((a.await?, b.await?))
    } else {
        tokio::try_join!(a, b)
    }
}

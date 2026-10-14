use crate::{
    error::VerifyError,
    progress::{NoopProgress, VerifyProgress},
    reader::TableReader,
};
use connectors::{
    sql::metadata::table::TableMetadata, traits::introspector::SchemaIntrospector,
    traits::reader::DataReader,
};
use engine_core::{
    context::{env::EnvContext, exec::ExecutionContext},
    dispatch_driver,
    drivers::DriverRef,
    plan::{cascade::resolve_cascade_tables, execution::ExecutionPlan},
};
use engine_schema::{
    graph_expander::GraphExpander,
    plan::SchemaObjectFlags,
    type_registry::{Dialect, TypeRegistry},
};
use engine_state::{PROGRESS_INTERVAL, RowHashIter, RowHashLog, RowHashScope, StateStore, Ticker};
use model::{
    execution::{
        pipeline::Pipeline,
        references::{DataMode, GraphReferences},
    },
    integrity::{
        hasher::RowHasher,
        merkle::MerkleAccumulator,
        receipt::VerificationReceipt,
        result::{
            Divergence, DivergenceKind, DivergenceSummary, MAX_REPORTED_DIVERGENCES,
            VerificationResult,
        },
        row_key::{KeyedRowHash, describe},
    },
    pagination::cursor::Cursor,
    records::Record,
    transform::mapping::TransformationMetadata,
};
use query_builder::offsets::{OffsetStrategy, OffsetStrategyFactory};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Instant,
};
use tracing::{debug, info, warn};

/// Rows fetched per destination read while re-hashing.
/// Independent of the migration's batch size.
const READ_BATCH_ROWS: usize = 1000;

/// Rows between clock reads in the per-row diff loop.
const DIFF_CLOCK_SAMPLE: u64 = 1 << 16;

/// How strictly the destination must match the receipt.
#[derive(Debug, Clone, Copy, Default)]
pub struct VerifyOptions {
    /// Accept destination rows the receipt does not cover.
    pub allow_extra: bool,
}

/// Outcome of merge-joining the receipt's committed set against the destination.
struct DiffOutcome {
    actual_root: [u8; 32],
    summary: DivergenceSummary,
    divergences: Vec<Divergence>,
    expected_seen: u64,
}

pub async fn verify(
    plan: ExecutionPlan,
    env: Arc<EnvContext>,
) -> Result<Vec<VerificationResult>, VerifyError> {
    verify_with_options(plan, env, VerifyOptions::default()).await
}

pub async fn verify_with_options(
    plan: ExecutionPlan,
    env: Arc<EnvContext>,
    options: VerifyOptions,
) -> Result<Vec<VerificationResult>, VerifyError> {
    verify_with_progress(plan, env, &mut NoopProgress, options).await
}

pub async fn verify_with_progress(
    plan: ExecutionPlan,
    env: Arc<EnvContext>,
    progress: &mut dyn VerifyProgress,
    options: VerifyOptions,
) -> Result<Vec<VerificationResult>, VerifyError> {
    let (state, hash_log) = init_state()?;
    let exec_ctx = ExecutionContext::new(&plan, state.clone(), hash_log.clone(), env);
    let mut results: Vec<VerificationResult> = Vec::new();

    for pipeline in &plan.pipelines {
        verify_pipeline(
            pipeline,
            &exec_ctx,
            &state,
            &hash_log,
            progress,
            &mut results,
            options,
        )
        .await?;
    }

    Ok(results)
}

fn init_state() -> Result<(Arc<StateStore>, Arc<RowHashLog>), VerifyError> {
    let state_dir = dirs::home_dir()
        .ok_or_else(|| {
            VerifyError::InitializationError("Failed to determine home directory".to_string())
        })?
        .join(".paganel/state");

    let state: Arc<StateStore> = StateStore::open(&state_dir)
        .map(Arc::new)
        .map_err(|e| VerifyError::InitializationError(e.to_string()))?;

    Ok((state, Arc::new(RowHashLog::in_state_dir(&state_dir))))
}

#[allow(clippy::too_many_arguments)]
async fn verify_pipeline(
    pipeline: &Pipeline,
    exec_ctx: &ExecutionContext,
    state: &Arc<StateStore>,
    hash_log: &RowHashLog,
    progress: &mut dyn VerifyProgress,
    results: &mut Vec<VerificationResult>,
    options: VerifyOptions,
) -> Result<(), VerifyError> {
    let driver = exec_ctx
        .resolve_driver(&pipeline.destination.connection)
        .await?;
    let mapping = TransformationMetadata::new(pipeline);

    let cascade_meta = get_graph_expansion(pipeline, &driver, &mapping).await?;
    let cascade_tables = resolve_cascade_tables(pipeline, &mapping, &cascade_meta);

    // Filter empty names and deduplicate so each table is verified exactly once.
    let mut seen = HashSet::new();
    let all_tables = std::iter::once(pipeline.destination.table.as_str())
        .chain(cascade_tables.iter().map(|s| s.as_str()))
        .filter(|t| !t.is_empty() && seen.insert(*t))
        .map(String::from)
        .collect::<Vec<_>>();

    for table in all_tables {
        // Load the receipt written by the most recent `apply --integrity`.
        let Some(receipt) = state.load_receipt(&pipeline.name, &table).await? else {
            let result = VerificationResult::NoPriorRun {
                pipeline: pipeline.name.clone(),
            };
            progress.table_finished(&result);
            results.push(result);
            continue;
        };

        progress.table_started(&pipeline.name, &table);
        let result = verify_table(
            &driver,
            &pipeline.name,
            &table,
            &receipt,
            hash_log,
            progress,
            options,
        )
        .await?;
        progress.table_finished(&result);
        results.push(result);
    }

    Ok(())
}

/// Re-read every row of `table` from the destination, key and hash each one,
/// then diff that keyed set against the set the migration committed.
#[allow(clippy::too_many_arguments)]
async fn verify_table(
    driver: &DriverRef,
    pipeline_name: &str,
    table: &str,
    receipt: &VerificationReceipt,
    hash_log: &RowHashLog,
    progress: &mut dyn VerifyProgress,
    options: VerifyOptions,
) -> Result<VerificationResult, VerifyError> {
    let start = Instant::now();

    let meta = fetch_table_metadata(driver, table).await?;
    let col_types: HashMap<String, String> = meta
        .columns
        .values()
        .map(|c| (c.name.clone(), c.data_type.clone()))
        .collect();

    let offset_strategy = OffsetStrategyFactory::keyset_over_pk(
        OffsetStrategyFactory::default_strategy(),
        table,
        &meta.primary_keys,
    );
    let table_reader = create_table_reader(driver, meta, offset_strategy)?;

    // Stage the destination's keyed hashes so both sides can be walked in the
    // same sorted key order without holding either set in memory.
    hash_log.clear(RowHashScope::Verify, pipeline_name, table)?;

    let outcome = stage_and_diff(
        &table_reader,
        receipt,
        &col_types,
        pipeline_name,
        hash_log,
        progress,
        options,
    )
    .await;

    if let Err(e) = hash_log.clear(RowHashScope::Verify, pipeline_name, table) {
        warn!(table, error = %e, "failed to clear verify scratch row hashes");
    }

    let DiffOutcome {
        actual_root,
        summary,
        divergences,
        expected_seen,
    } = outcome?;
    let duration_ms = start.elapsed().as_millis() as u64;

    if expected_seen != receipt.total_rows {
        return Ok(VerificationResult::LogUnavailable {
            pipeline: pipeline_name.to_string(),
            table: table.to_string(),
            expected_rows: receipt.total_rows,
            found_rows: expected_seen,
        });
    }

    // With `allow_extra`, uncovered rows were neither folded into the root nor
    // recorded, so the root still has to reproduce for the receipt's own rows.
    let clean = if options.allow_extra {
        summary.missing == 0 && summary.changed == 0
    } else {
        summary.is_clean()
    };
    let is_match = clean && actual_root == receipt.table_root;

    Ok(if is_match {
        VerificationResult::Match {
            receipt: receipt.clone(),
            duration_ms,
            extra_ignored: summary.extra,
        }
    } else {
        VerificationResult::Mismatch {
            receipt: receipt.clone(),
            actual_root,
            summary,
            divergences,
            duration_ms,
        }
    })
}

/// Stage the destination's rows, then diff them against the receipt's set.
#[allow(clippy::too_many_arguments)]
async fn stage_and_diff(
    reader: &TableReader,
    receipt: &VerificationReceipt,
    col_types: &HashMap<String, String>,
    pipeline_name: &str,
    hash_log: &RowHashLog,
    progress: &mut dyn VerifyProgress,
    options: VerifyOptions,
) -> Result<DiffOutcome, VerifyError> {
    progress.table_phase("reading destination");
    let dest_rows = stage_destination(reader, receipt, col_types, pipeline_name, hash_log).await?;

    // Both sides have to be walked in the same order; sealing is what puts the
    // staged destination into the receipt set's key order.
    let table = &receipt.table_name;
    progress.table_phase("sorting row hashes");
    hash_log.seal(RowHashScope::Verify, pipeline_name, table)?;

    let expected = hash_log.stream(RowHashScope::Apply, pipeline_name, table)?;
    let actual = hash_log.stream(RowHashScope::Verify, pipeline_name, table)?;

    // The merge-join walks the whole table off disk.
    progress.table_phase("comparing");
    let receipt = receipt.clone();

    tokio::task::spawn_blocking(move || {
        diff_keyed_sets(expected, actual, &receipt, dest_rows, options)
    })
    .await
    .map_err(|e| VerifyError::InitializationError(format!("verification task failed: {e}")))?
}

/// Read the destination table start to finish, hashing and keying each row with
/// the receipt's own column order, and write the pairs to the verify scratch space.
async fn stage_destination(
    reader: &TableReader,
    receipt: &VerificationReceipt,
    col_types: &HashMap<String, String>,
    pipeline_name: &str,
    hash_log: &RowHashLog,
) -> Result<u64, VerifyError> {
    let mut hasher = RowHasher::new(receipt.column_order.clone(), receipt.algorithm);
    let mut cursor = Some(Cursor::None);
    let mut rows_read = 0u64;

    let started = Instant::now();
    let mut ticker = Ticker::new(PROGRESS_INTERVAL);

    while let Some(c) = cursor {
        let (rows, next_cursor) = reader.next_batch(c, READ_BATCH_ROWS).await?;
        rows_read += rows.len() as u64;

        if ticker.report(rows_read) {
            info!(
                table = %receipt.table_name,
                rows = rows_read,
                expected = receipt.total_rows,
                "reading destination{}",
                percent_of(rows_read, receipt.total_rows)
            );
        }

        if !rows.is_empty() {
            let refs: Vec<&Record> = rows.iter().collect();
            let entries = hasher.hash_rows(&refs, col_types, &receipt.key_columns);

            hash_log.append(
                RowHashScope::Verify,
                pipeline_name,
                &receipt.table_name,
                &entries,
            )?;
        }

        cursor = next_cursor;
    }

    if started.elapsed() >= PROGRESS_INTERVAL {
        info!(
            table = %receipt.table_name,
            rows = rows_read,
            "destination read; sorting"
        );
    }

    Ok(rows_read)
}

/// Merge-join two key-sorted row-hash streams.
fn diff_keyed_sets(
    expected: RowHashIter,
    actual: RowHashIter,
    receipt: &VerificationReceipt,
    dest_rows_read: u64,
    options: VerifyOptions,
) -> Result<DiffOutcome, VerifyError> {
    let mut compared = 0u64;
    let mut expected_seen = 0u64;
    let mut ticker = Ticker::new(PROGRESS_INTERVAL).sampling(DIFF_CLOCK_SAMPLE);

    let mut acc = MerkleAccumulator::new(receipt.algorithm);
    let mut summary = DivergenceSummary {
        expected_rows: receipt.total_rows,
        ..Default::default()
    };

    let mut divergences: Vec<Divergence> = Vec::new();
    let key_columns = &receipt.key_columns;

    let mut record = |kind: DivergenceKind, key: &[u8]| {
        if divergences.len() < MAX_REPORTED_DIVERGENCES {
            divergences.push(Divergence {
                key: describe(key, key_columns),
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
                table = %receipt.table_name,
                rows = summary.actual_rows,
                expected = receipt.total_rows,
                "comparing{}",
                percent_of(summary.actual_rows, receipt.total_rows)
            );
        }

        match (expected.current(), actual.current()) {
            (None, None) => break,
            (Some(exp), None) => {
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
            (None, Some(act)) => {
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
            (Some(exp), Some(act)) => match exp.key.cmp(&act.key) {
                std::cmp::Ordering::Less => {
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
                std::cmp::Ordering::Greater => {
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
                std::cmp::Ordering::Equal => {
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
            },
        }
    }

    if dest_rows_read > summary.actual_rows {
        debug!(
            table = %receipt.table_name,
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

fn percent_of(done: u64, total: u64) -> String {
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

async fn fetch_table_metadata(
    driver: &DriverRef,
    table: &str,
) -> Result<TableMetadata, VerifyError> {
    dispatch_driver!(driver, |d| {
        let introspector: Arc<dyn SchemaIntrospector> = d.clone() as _;
        introspector.table_metadata(table).await
    })
    .map_err(Into::into)
}

fn create_table_reader(
    driver: &DriverRef,
    meta: TableMetadata,
    offset_strategy: Arc<dyn OffsetStrategy>,
) -> Result<TableReader, VerifyError> {
    dispatch_driver!(driver, |d| {
        let data_reader: Arc<dyn DataReader> = d.clone() as _;
        Ok(TableReader::new(
            data_reader,
            meta.clone(),
            offset_strategy.clone(),
        ))
    })
}

async fn get_graph_expansion(
    pipeline: &Pipeline,
    src_driver: &DriverRef,
    mapping: &TransformationMetadata,
) -> Result<Option<HashMap<String, TableMetadata>>, VerifyError> {
    let Some(refs) = &pipeline.source.graph_references else {
        return Ok(None);
    };

    let dest_driver = &pipeline.destination.connection.driver;
    let dest_dialect = Dialect::parse(dest_driver).ok_or_else(|| {
        VerifyError::InitializationError(format!(
            "graph verification requires a SQL destination dialect, but destination driver '{dest_driver}' is not a SQL dialect"
        ))
    })?;

    expand_graph_references(
        &pipeline.source.table,
        src_driver,
        mapping,
        refs,
        dest_dialect,
    )
    .await
}

async fn expand_graph_references(
    root_table: &str,
    src_driver: &DriverRef,
    mapping: &TransformationMetadata,
    refs: &GraphReferences,
    dest_dialect: Dialect,
) -> Result<Option<HashMap<String, TableMetadata>>, VerifyError> {
    let source_dialect = src_driver.dialect();

    let result = dispatch_driver!(src_driver, |d| {
        let introspector: Arc<dyn SchemaIntrospector> = d.clone() as _;
        let type_registry = Arc::new(TypeRegistry::new(source_dialect, dest_dialect));
        let expander = GraphExpander::new(introspector, type_registry, source_dialect);

        expander
            .expand(
                root_table,
                refs,
                mapping,
                SchemaObjectFlags::default(),
                false,
            )
            .await
            .map_err(|e| VerifyError::InitializationError(e.to_string()))?
    });

    Ok(matches!(refs.data_mode, DataMode::Cascade).then_some(result.discovered_tables))
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
        let out = diff_keyed_sets(iter(expected), iter(actual), &r, rows, options).expect("diff");
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

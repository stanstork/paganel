use crate::{
    DiffInput, DiffOutcome, HashSink, HashSpec, VerifyOptions, VerifyProgress,
    diff::MAX_RECHECK_KEYS,
    diff_keyed_sets,
    error::VerifyError,
    hash_table,
    table::{fetch_table_metadata, hash_rows_where},
};
use connectors::sql::{filter::SqlFilter, metadata::table::TableMetadata};
use engine_core::{
    drivers::DriverRef,
    plan::pair::{PairSpec, target_column},
};
use engine_state::{RowHashLog, RowHashScope};
use model::{
    core::value::Value,
    integrity::{
        algorithm::HashAlgorithm,
        canonical::decode_key,
        pair::pair_pipeline,
        report::{DivergenceReport, Recheck, ReportResult, TableReport},
        result::{Divergence, DivergenceKind, MAX_REPORTED_DIVERGENCES},
        row_key::describe,
    },
};
use std::{
    collections::{BTreeMap, HashMap},
    time::Instant,
};
use tracing::{Instrument, info, info_span, warn};

/// Values per `IN (...)` when re-reading differing keys.
const RECHECK_CHUNK: usize = 500;

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct SideSpecs {
    pub source: HashSpec,
    pub target: HashSpec,
}

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

    let scopes = [RowHashScope::Source, RowHashScope::Verify];

    for scope in scopes {
        hash_log.clear(scope, &pipeline, table)?;
    }

    let sides = Sides {
        spec,
        source,
        target,
        source_meta: &source_meta,
        target_meta: &target_meta,
        specs: &specs,
        shared,
    };

    let outcome = sides.scan_and_diff(hash_log, &pipeline, progress).await;

    for scope in scopes {
        if let Err(e) = hash_log.clear(scope, &pipeline, table) {
            warn!(pair = %spec.name, error = %e, "failed to clear pair scratch row hashes");
        }
    }

    let outcome = outcome?;
    let mut report = report_from(&pipeline, table, &outcome);

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

            match sides.recheck(&outcome.divergent_keys).await? {
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

/// Resolve which columns both sides hash, and in what order.
pub(crate) fn side_specs(
    name: &str,
    map: &BTreeMap<String, String>,
    source_ignore: &[String],
    target_ignore: &[String],
    key: Option<&[String]>,
    source: &TableMetadata,
    target: &TableMetadata,
) -> Result<SideSpecs, VerifyError> {
    let err = |msg: String| VerifyError::InitializationError(format!("pair \"{name}\": {msg}"));

    let find_col = |meta: &TableMetadata, column: &str| {
        meta.columns
            .keys()
            .find(|c| c.eq_ignore_ascii_case(column))
            .cloned()
    };

    let is_ignored =
        |list: &[String], column: &str| list.iter().any(|i| i.eq_ignore_ascii_case(column));

    // Validate table metadata presence
    for (side, meta) in [("source", source), ("target", target)] {
        if meta.columns.is_empty() {
            return Err(err(format!("{side} table '{}' not found", meta.name)));
        }
    }

    // Validate mapped columns exist
    for (s, t) in map {
        if find_col(source, s).is_none() {
            return Err(err(format!("map: '{}' has no column {s}", source.name)));
        }
        if find_col(target, t).is_none() {
            return Err(err(format!("map: '{}' has no column {t}", target.name)));
        }
    }

    // Validate ignored columns exist
    for (meta, list) in [(source, source_ignore), (target, target_ignore)] {
        if let Some(i) = list.iter().find(|i| find_col(meta, i).is_none()) {
            return Err(err(format!("ignore: '{}' has no column {i}", meta.name)));
        }
    }

    let mut pairs: Vec<(String, String)> = Vec::new();
    let mut source_only = Vec::new();
    let mut half_ignored = Vec::new();

    for column in source
        .columns
        .keys()
        .filter(|c| !is_ignored(source_ignore, c))
    {
        match find_col(target, &target_column(map, column)) {
            Some(t) if is_ignored(target_ignore, &t) => half_ignored.push(column.clone()),
            Some(t) => pairs.push((column.clone(), t)),
            None => source_only.push(column.clone()),
        }
    }

    let source_name = |t: &str| {
        map.iter()
            .find(|(_, mapped)| mapped.eq_ignore_ascii_case(t))
            .map_or_else(|| t.to_string(), |(s, _)| s.clone())
    };

    let mut target_only = Vec::new();
    for column in target
        .columns
        .keys()
        .filter(|t| !is_ignored(target_ignore, t))
    {
        if pairs.iter().any(|(_, p)| p.eq_ignore_ascii_case(column)) {
            continue;
        }
        match find_col(source, &source_name(column)) {
            Some(s) if is_ignored(source_ignore, &s) => half_ignored.push(column.clone()),
            _ => target_only.push(column.clone()),
        }
    }

    source_only.sort();
    target_only.sort();
    half_ignored.sort();

    let mut problems = Vec::new();
    if !source_only.is_empty() {
        problems.push(format!(
            "source has {} not in target",
            source_only.join(", ")
        ));
    }
    if !target_only.is_empty() {
        problems.push(format!(
            "target has {} not in source",
            target_only.join(", ")
        ));
    }
    if !problems.is_empty() {
        return Err(err(format!(
            "{}; add them to that side's ignore or to map",
            problems.join("; ")
        )));
    }
    if !half_ignored.is_empty() {
        return Err(err(format!(
            "{} ignored on one side only; ignore on both sides or neither",
            half_ignored.join(", ")
        )));
    }

    pairs.sort_by_key(|(s, _)| s.to_ascii_lowercase());

    let key_source: Vec<String> = match key {
        Some(k_list) => k_list.to_vec(),
        None if !target.primary_keys.is_empty() => target
            .primary_keys
            .iter()
            .map(|t| {
                pairs
                    .iter()
                    .find(|(_, p)| p.eq_ignore_ascii_case(t))
                    .map(|(s, _)| s.clone())
                    .ok_or_else(|| err(format!("target key column {t} is ignored")))
            })
            .collect::<Result<_, _>>()?,
        None if !source.primary_keys.is_empty() => source.primary_keys.clone(),
        None => {
            return Err(err(
                "neither table has a primary key; set key = [\"column\", ...]".into(),
            ));
        }
    };

    let key_pairs: Vec<&(String, String)> = key_source
        .iter()
        .map(|k| {
            pairs
                .iter()
                .find(|(s, _)| s.eq_ignore_ascii_case(k))
                .ok_or_else(|| err(format!("key column {k} is not compared on both sides")))
        })
        .collect::<Result<_, _>>()?;

    let build_side_spec = |pick: fn(&(String, String)) -> &String| HashSpec {
        column_order: pairs.iter().map(|p| pick(p).clone()).collect(),
        key_columns: key_pairs.iter().map(|p| pick(p).clone()).collect(),
        algorithm: HashAlgorithm::default(),
    };

    Ok(SideSpecs {
        source: build_side_spec(|(s, _)| s),
        target: build_side_spec(|(_, t)| t),
    })
}

#[derive(Clone, Copy)]
struct Sides<'a> {
    spec: &'a PairSpec,
    source: &'a DriverRef,
    target: &'a DriverRef,
    source_meta: &'a TableMetadata,
    target_meta: &'a TableMetadata,
    specs: &'a SideSpecs,
    shared: bool,
}

/// The keys that still differ after `grace`.
#[derive(Default)]
struct Rechecked {
    missing: u64,
    changed: u64,
    extra: u64,
    settled: u64,
    divergences: Vec<Divergence>,
}

impl Sides<'_> {
    /// Hash both tables into the row-hash log, sort them, and merge-join.
    async fn scan_and_diff(
        &self,
        hash_log: &RowHashLog,
        pipeline: &str,
        progress: &mut dyn VerifyProgress,
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

        progress.table_phase("reading both tables");
        let (source_rows, target_rows) = both(
            self.shared,
            hash_table(
                self.source,
                self.source_meta.clone(),
                &self.specs.source,
                &source_sink,
                self.spec.source.filter.as_ref(),
                None,
            )
            .instrument(info_span!("pair", side = %"source")),
            hash_table(
                self.target,
                self.target_meta.clone(),
                &self.specs.target,
                &target_sink,
                self.spec.target.filter.as_ref(),
                None,
            )
            .instrument(info_span!("pair", side = %"target")),
        )
        .await?;

        progress.table_phase("sorting row hashes");
        hash_log.seal(RowHashScope::Source, pipeline, table)?;
        hash_log.seal(RowHashScope::Verify, pipeline, table)?;

        let expected = hash_log.stream(RowHashScope::Source, pipeline, table)?;
        let actual = hash_log.stream(RowHashScope::Verify, pipeline, table)?;
        let input = DiffInput {
            table: table.to_string(),
            key_columns: self.specs.source.key_columns.clone(),
            algorithm: self.specs.source.algorithm,
            expected_rows: source_rows,
        };

        progress.table_phase("comparing");
        tokio::task::spawn_blocking(move || {
            diff_keyed_sets(
                expected,
                actual,
                &input,
                target_rows,
                VerifyOptions::default(),
            )
        })
        .await
        .map_err(|e| VerifyError::InitializationError(format!("pair task failed: {e}")))?
    }

    /// Re-read `keys` from both sides and diff them again.
    async fn recheck(&self, keys: &[Vec<u8>]) -> Result<Option<Rechecked>, VerifyError> {
        let firsts = keys
            .iter()
            .filter_map(|key| decode_key(key).and_then(|v| v.into_iter().next()))
            .fold(Vec::new(), |mut acc, v| {
                if !acc.contains(&v) {
                    acc.push(v);
                }
                acc
            });

        if firsts.is_empty() && !keys.is_empty() {
            return Ok(None);
        }

        let (now_source, now_target) = both(
            self.shared,
            read_keys(
                self.source,
                self.source_meta,
                &self.specs.source,
                self.spec.source.filter.as_ref(),
                &firsts,
            )
            .instrument(info_span!("pair", side = %"source")),
            read_keys(
                self.target,
                self.target_meta,
                &self.specs.target,
                self.spec.target.filter.as_ref(),
                &firsts,
            )
            .instrument(info_span!("pair", side = %"target")),
        )
        .await?;

        let mut rechecked = Rechecked::default();
        for key in keys {
            let kind = match (now_source.get(key), now_target.get(key)) {
                (Some(s), Some(t)) if s == t => None,
                (None, None) => None,
                (Some(s), None) => Some(DivergenceKind::Missing { expected_hash: *s }),
                (None, Some(t)) => Some(DivergenceKind::Extra { actual_hash: *t }),
                (Some(s), Some(t)) => Some(DivergenceKind::Changed {
                    expected_hash: *s,
                    actual_hash: *t,
                }),
            };

            let Some(kind) = kind else {
                rechecked.settled += 1;
                continue;
            };

            match &kind {
                DivergenceKind::Missing { .. } => rechecked.missing += 1,
                DivergenceKind::Extra { .. } => rechecked.extra += 1,
                DivergenceKind::Changed { .. } => rechecked.changed += 1,
            }

            if rechecked.divergences.len() < MAX_REPORTED_DIVERGENCES {
                rechecked.divergences.push(Divergence {
                    key: describe(key, &self.specs.source.key_columns),
                    kind,
                });
            }
        }

        Ok(Some(rechecked))
    }
}

impl Rechecked {
    /// Replace the first read's counts with these.
    fn apply(self, report: &mut TableReport) -> Recheck {
        report.missing = self.missing;
        report.changed = self.changed;
        report.extra = self.extra;
        report.divergences = self
            .divergences
            .iter()
            .map(DivergenceReport::from)
            .collect();
        Recheck::Done {
            settled: self.settled,
        }
    }
}

/// Run `a` and `b` together, or one after the other when they share a client.
async fn both<A, B>(
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

async fn read_keys(
    driver: &DriverRef,
    meta: &TableMetadata,
    spec: &HashSpec,
    filter: Option<&SqlFilter>,
    values: &[Value],
) -> Result<HashMap<Vec<u8>, [u8; 32]>, VerifyError> {
    let column = &spec.key_columns[0];
    let mut hashes = HashMap::new();

    for chunk in values.chunks(RECHECK_CHUNK) {
        hashes.extend(hash_rows_where(driver, meta, spec, filter, column, chunk.to_vec()).await?);
    }

    Ok(hashes)
}

fn report_from(pipeline: &str, table: &str, outcome: &DiffOutcome) -> TableReport {
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
